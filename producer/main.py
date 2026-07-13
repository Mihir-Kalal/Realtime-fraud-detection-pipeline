"""
Producer & Ingestion (Phase 1)

Generates synthetic `Transaction` events matching common/schemas.py exactly,
at a configurable rate, with a configurable percentage of injected fraud
patterns, and pushes them to the Redis Stream `transactions:raw`.

Run:
    python producer/main.py
Env vars (all optional, see bottom of this file for defaults):
    REDIS_URL, EVENTS_PER_SECOND, FRAUD_RATE, NUM_SYNTHETIC_USERS, RUN_SECONDS
"""

from __future__ import annotations

import msgpack
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from common.schemas import Channel, Transaction

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

KAFKA_TOPIC = "transactions-raw"

MERCHANT_CATEGORIES = [
    "electronics", "groceries", "travel", "restaurants", "clothing",
    "utilities", "entertainment", "pharmacy", "gaming", "jewelry",
]
COUNTRIES = ["IN", "US", "GB", "DE", "SG"]
CHANNELS = list(Channel)
INDIAN_NAMES = [
    "Aarav", "Kabir", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", 
    "Krishna", "Ishaan", "Shaurya", "Atharva", "Ananya", "Diya", "Pihu", "Aaradhya", 
    "Ira", "Sana", "Fatima", "Priya", "Pooja", "Amit", "Rahul", "Rohit", "Vikram", 
    "Sanjay", "Rajesh", "Anil", "Sunita", "Anita", "Geeta", "Neha", "Rohan", "Siddharth",
    "Deepak", "Sandhya", "Karan", "Kunal", "Meera", "Rani", "Kiran", "Vijay"
]

# -----------------------------------------------------------------------
# Fraud pattern labels — this exact set/spelling is the Phase 1 contract.
# feature_engine / training may key off these labels via the injected
# metadata (see NOTE below) for building labeled offline datasets.
# -----------------------------------------------------------------------
FRAUD_PATTERNS = [
    "velocity_spike",       # many txns from same user in a short burst
    "amount_outlier",       # amount far above the user's normal range
    "impossible_travel",    # two txns, same user, different countries,
                             # too close in time to be physically possible
]


from common.feature_columns import _home_country_for_user

class SyntheticUser:
    """Tracks a synthetic user's baseline behavior so fraud injections can
    deviate from something realistic rather than pure noise."""

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.home_country = _home_country_for_user(user_id)
        self.typical_amount = round(random.uniform(10, 300), 2)
        self.device_id = f"device_{uuid.uuid4().hex[:10]}"
        self.last_txn_time: datetime | None = None
        self.last_country: str | None = None


class TransactionProducer:
    def __init__(
        self,
        kafka_producer: KafkaProducer,
        topic: str,
        events_per_second: float,
        fraud_rate: float,
        num_users: int,
    ):
        self.producer = kafka_producer
        self.topic = topic
        self.events_per_second = events_per_second
        self.fraud_rate = fraud_rate
        # Seed to ensure deterministic user generation across simulator & producer
        random.seed(42)
        self.users = [SyntheticUser(f"{name}_{i:03d}") for i, name in enumerate(random.choices(INDIAN_NAMES, k=num_users))]
        self._velocity_burst_remaining: dict[str, int] = {}

    # ---- event construction -------------------------------------------------

    def _base_transaction(self, user: SyntheticUser) -> Transaction:
        now = datetime.now(timezone.utc)
        txn = Transaction(
            txn_id=f"txn_{uuid.uuid4().hex}",
            user_id=user.user_id,
            amount=round(max(1.0, random.gauss(user.typical_amount, user.typical_amount * 0.25)), 2),
            currency="INR",
            merchant_id=f"merchant_{random.randint(1, 500):05d}",
            merchant_category=random.choice(MERCHANT_CATEGORIES),
            timestamp=now,
            device_id=user.device_id,
            ip_country=user.home_country,
            channel=random.choice(CHANNELS),
        )
        user.last_txn_time = now
        user.last_country = txn.ip_country
        return txn

    def _inject_velocity_spike(self, user: SyntheticUser) -> Transaction:
        # Kick off (or continue) a burst of 5-10 rapid-fire transactions.
        if self._velocity_burst_remaining.get(user.user_id, 0) <= 0:
            self._velocity_burst_remaining[user.user_id] = random.randint(5, 10)
        self._velocity_burst_remaining[user.user_id] -= 1
        txn = self._base_transaction(user)
        # Small, rapid purchases — override amount via model_copy (Pydantic v2 models are immutable).
        return txn.model_copy(update={"amount": round(random.uniform(5, 50), 2)})

    def _inject_amount_outlier(self, user: SyntheticUser) -> Transaction:
        txn = self._base_transaction(user)
        return txn.model_copy(update={"amount": round(user.typical_amount * random.uniform(15, 50), 2)})

    def _inject_impossible_travel(self, user: SyntheticUser) -> Transaction:
        txn = self._base_transaction(user)
        # Pick a country far from the user's last known country and set the
        # timestamp to be implausibly close to their last transaction.
        other_countries = [c for c in COUNTRIES if c != (user.last_country or user.home_country)]
        new_country = random.choice(other_countries)
        new_device = f"device_{uuid.uuid4().hex[:10]}"  # new/unrecognized device
        updates: dict = {"ip_country": new_country, "device_id": new_device}
        if user.last_txn_time is not None:
            updates["timestamp"] = user.last_txn_time + timedelta(seconds=random.randint(30, 300))
        return txn.model_copy(update=updates)

    def generate_event(self) -> tuple[Transaction, str | None]:
        """Returns (transaction, fraud_pattern_label_or_None)."""
        user = random.choice(self.users)
        if random.random() < self.fraud_rate:
            pattern = random.choice(FRAUD_PATTERNS)
            if pattern == "velocity_spike":
                return self._inject_velocity_spike(user), pattern
            if pattern == "amount_outlier":
                return self._inject_amount_outlier(user), pattern
            if pattern == "impossible_travel":
                return self._inject_impossible_travel(user), pattern
        return self._base_transaction(user), None

    # ---- publishing -----------------------------------------------------

    def publish(self, txn: Transaction, fraud_pattern: str | None) -> str:
        # NOTE on wire format (contract for common/kafka_reader.py and any
        # future consumer): We use MessagePack binary serialization for high-throughput.
        # The Transaction is packed as a dict into a single "data" field.
        # `injected_fraud_pattern` is producer-only simulation metadata.
        
        # model_dump(mode='json') ensures dates/enums are converted to basic types (str)
        # which msgpack can serialize easily.
        fields = {"data": txn.model_dump(mode='json')}
        if fraud_pattern:
            fields["injected_fraud_pattern"] = fraud_pattern
        
        serialized = msgpack.packb(fields)
        future = self.producer.send(self.topic, value=serialized)
        metadata = future.get(timeout=5)
        return f"{metadata.partition}:{metadata.offset}"

    def run(self, run_seconds: float | None) -> None:
        interval = 1.0 / self.events_per_second if self.events_per_second > 0 else 0
        start = time.monotonic()
        sent = 0
        fraud_sent = 0
        logger.info(
            "Starting producer: %.2f events/sec, fraud_rate=%.2f, users=%d, topic=%s",
            self.events_per_second, self.fraud_rate, len(self.users), self.topic,
        )
        try:
            while run_seconds is None or (time.monotonic() - start) < run_seconds:
                txn, pattern = self.generate_event()
                msg_id = self.publish(txn, pattern)
                sent += 1
                if pattern:
                    fraud_sent += 1
                    logger.info(
                        "[FRAUD:%s] %s user=%s amount=%.2f country=%s -> %s",
                        pattern, txn.txn_id, txn.user_id, txn.amount, txn.ip_country, msg_id,
                    )
                else:
                    logger.debug("%s user=%s amount=%.2f -> %s", txn.txn_id, txn.user_id, txn.amount, msg_id)
                if interval:
                    time.sleep(interval)
        except KeyboardInterrupt:
            pass
        finally:
            logger.info("Stopped. sent=%d fraud_sent=%d (%.2f%%)", sent, fraud_sent, 100 * fraud_sent / max(sent, 1))


def main() -> None:
    import sys
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    events_per_second = float(os.environ.get("EVENTS_PER_SECOND", "5"))
    fraud_rate = float(os.environ.get("FRAUD_RATE", "0.05"))
    num_users = int(os.environ.get("NUM_SYNTHETIC_USERS", "200"))
    run_seconds_env = os.environ.get("RUN_SECONDS")
    run_seconds = float(run_seconds_env) if run_seconds_env else None

    producer = None
    for attempt in range(15):
        try:
            logger.info("Connecting to Kafka (attempt %d/15)...", attempt + 1)
            producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                acks="all",
                retries=5,
            )
            logger.info("Connected to Kafka successfully.")
            break
        except Exception as e:
            logger.warning("Kafka not ready: %s. Retrying in 3s...", e)
            time.sleep(3)
    else:
        logger.error("Could not connect to Kafka after 15 attempts.")
        sys.exit(1)

    try:
        txn_producer = TransactionProducer(
            kafka_producer=producer,
            topic=KAFKA_TOPIC,
            events_per_second=events_per_second,
            fraud_rate=fraud_rate,
            num_users=num_users,
        )
        txn_producer.run(run_seconds=run_seconds)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
