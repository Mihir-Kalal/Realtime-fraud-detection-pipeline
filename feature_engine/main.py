"""
feature_engine service (Phase 2)

Consumes Transaction events from `transactions:raw` via the shared
`TransactionStreamReader` (common/stream_reader.py), maintains a per-user
rolling event history in Redis, computes the locked online feature vector,
writes it to `features:user:{user_id}` (Redis hash), and persists a snapshot
row to Postgres (`feature_snapshots` table) for offline training.

FINAL LOCKED FEATURE SCHEMA — see PROJECT_STATE.md Phase 2 section for the
authoritative contract. Do not rename/reorder without updating that file and
every downstream consumer (training, serving).
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import redis

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.schemas import Transaction, feature_key  # noqa: E402
from common.kafka_reader import TransactionKafkaReader  # noqa: E402
from feature_engine.validators import validate_feature_vector
import pandera.errors
from graph_features.writer import graph_writer
from graph_features.queries import get_shared_device_count, get_shared_merchant_fraud_count, get_hop_distance_to_known_fraud

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s feature_engine: %(message)s",
)
logger = logging.getLogger("feature_engine")

# ---------------------------------------------------------------------------
# Configuration (env vars, all optional)
# ---------------------------------------------------------------------------

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://frauduser:fraudpass@postgres:5432/frauddb"
)
CONSUMER_INDEX = os.environ.get("CONSUMER_INDEX", "1")
GROUP_NAME = "feature_engine-cg"
CONSUMER_NAME = f"feature_engine-{CONSUMER_INDEX}"

WINDOW_1H_SECONDS = 3600
WINDOW_24H_SECONDS = 86400
IMPOSSIBLE_TRAVEL_MAX_SECONDS = int(
    os.environ.get("IMPOSSIBLE_TRAVEL_MAX_SECONDS", "3600")
)
HISTORY_TTL_SECONDS = WINDOW_24H_SECONDS + 300  # small grace buffer
FEATURE_KEY_TTL_SECONDS = int(os.environ.get("FEATURE_KEY_TTL_SECONDS", "172800"))  # 48h
POSTGRES_FLUSH_BATCH_SIZE = int(os.environ.get("POSTGRES_FLUSH_BATCH_SIZE", "20"))
POSTGRES_FLUSH_INTERVAL_SECONDS = float(
    os.environ.get("POSTGRES_FLUSH_INTERVAL_SECONDS", "2.0")
)


def history_key(user_id: str) -> str:
    return f"feature_engine:history:{user_id}"


def last_seen_key(user_id: str) -> str:
    return f"feature_engine:last_seen:{user_id}"


# ---------------------------------------------------------------------------
# Postgres: snapshot table DDL (idempotent) + batched writer
# ---------------------------------------------------------------------------

FEATURE_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS transactions (
    txn_id              TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency            CHAR(3) NOT NULL,
    merchant_id         TEXT NOT NULL,
    merchant_category   TEXT NOT NULL,
    txn_timestamp       TIMESTAMPTZ NOT NULL,
    device_id           TEXT NOT NULL,
    ip_country          CHAR(2) NOT NULL,
    channel             TEXT NOT NULL CHECK (
                            channel IN ('card_present', 'card_not_present', 'upi', 'login')
                        ),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions (user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_txn_timestamp ON transactions (txn_timestamp);

CREATE TABLE IF NOT EXISTS feature_snapshots (
    id                      BIGSERIAL PRIMARY KEY,
    txn_id                  TEXT NOT NULL UNIQUE REFERENCES transactions (txn_id),
    user_id                 TEXT NOT NULL,
    txn_timestamp           TIMESTAMPTZ NOT NULL,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    txn_velocity_1h         INTEGER NOT NULL,
    txn_velocity_24h        INTEGER NOT NULL,
    amount_mean_24h         DOUBLE PRECISION NOT NULL,
    amount_std_24h          DOUBLE PRECISION NOT NULL,
    amount_zscore           DOUBLE PRECISION NOT NULL,
    distinct_merchants_1h   INTEGER NOT NULL,
    distinct_merchants_24h  INTEGER NOT NULL,
    impossible_travel_flag  SMALLINT NOT NULL,
    seconds_since_last_txn  DOUBLE PRECISION NOT NULL,
    shared_device_count     INTEGER NOT NULL DEFAULT 0,
    shared_merchant_fraud_count INTEGER NOT NULL DEFAULT 0,
    hop_distance_to_fraud   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_feature_snapshots_user_id
    ON feature_snapshots (user_id);
CREATE INDEX IF NOT EXISTS idx_feature_snapshots_txn_timestamp
    ON feature_snapshots (txn_timestamp);
"""

INSERT_SQL = """
INSERT INTO feature_snapshots (
    txn_id, user_id, txn_timestamp,
    txn_velocity_1h, txn_velocity_24h,
    amount_mean_24h, amount_std_24h, amount_zscore,
    distinct_merchants_1h, distinct_merchants_24h,
    impossible_travel_flag, seconds_since_last_txn,
    shared_device_count, shared_merchant_fraud_count, hop_distance_to_fraud
) VALUES %s
ON CONFLICT (txn_id) DO NOTHING
"""


@dataclass
class FeatureVector:
    txn_id: str
    user_id: str
    txn_timestamp: datetime
    txn_velocity_1h: int
    txn_velocity_24h: int
    amount_mean_24h: float
    amount_std_24h: float
    amount_zscore: float
    distinct_merchants_1h: int
    distinct_merchants_24h: int
    impossible_travel_flag: int
    seconds_since_last_txn: float
    shared_device_count: int
    shared_merchant_fraud_count: int
    hop_distance_to_fraud: int

    def as_redis_hash(self) -> dict:
        """String-encoded values for HSET (Redis hashes are string-typed)."""
        return {
            "txn_velocity_1h": str(self.txn_velocity_1h),
            "txn_velocity_24h": str(self.txn_velocity_24h),
            "amount_mean_24h": repr(self.amount_mean_24h),
            "amount_std_24h": repr(self.amount_std_24h),
            "amount_zscore": repr(self.amount_zscore),
            "distinct_merchants_1h": str(self.distinct_merchants_1h),
            "distinct_merchants_24h": str(self.distinct_merchants_24h),
            "impossible_travel_flag": str(self.impossible_travel_flag),
            "seconds_since_last_txn": repr(self.seconds_since_last_txn),
            "shared_device_count": str(self.shared_device_count),
            "shared_merchant_fraud_count": str(self.shared_merchant_fraud_count),
            "hop_distance_to_fraud": str(self.hop_distance_to_fraud),
            "last_txn_id": self.txn_id,
            "last_txn_ts": self.txn_timestamp.isoformat(),
            "feature_computed_at": datetime.now(timezone.utc).isoformat(),
        }

    def as_postgres_row(self) -> tuple:
        return (
            self.txn_id,
            self.user_id,
            self.txn_timestamp,
            self.txn_velocity_1h,
            self.txn_velocity_24h,
            self.amount_mean_24h,
            self.amount_std_24h,
            self.amount_zscore,
            self.distinct_merchants_1h,
            self.distinct_merchants_24h,
            self.impossible_travel_flag,
            self.seconds_since_last_txn,
            self.shared_device_count,
            self.shared_merchant_fraud_count,
            self.hop_distance_to_fraud,
        )


class PostgresSnapshotWriter:
    """Batches feature snapshot rows and flushes them on size/time thresholds.

    A dedicated background thread owns the flush cadence so the main
    consumer loop is never blocked waiting on a DB round-trip per event.
    """

    def __init__(self, dsn: str, batch_size: int, flush_interval: float):
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer: list[tuple] = []
        self._txn_buffer: list[tuple] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._conn = self._connect()
        self._ensure_schema()
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()

    def _connect(self):
        last_err = None
        for attempt in range(10):
            try:
                conn = psycopg2.connect(self.dsn)
                conn.autocommit = True
                return conn
            except psycopg2.OperationalError as e:
                last_err = e
                logger.warning("Postgres not ready (attempt %d/10): %s", attempt + 1, e)
                time.sleep(min(2 ** attempt, 15))
        raise RuntimeError(f"Could not connect to Postgres: {last_err}")

    def _ensure_schema(self):
        with self._conn.cursor() as cur:
            cur.execute(FEATURE_SNAPSHOTS_DDL)

    def enqueue(self, row: tuple, txn: Transaction):
        with self._lock:
            self._buffer.append(row)
            self._txn_buffer.append((
                txn.txn_id,
                txn.user_id,
                txn.amount,
                txn.currency,
                txn.merchant_id,
                txn.merchant_category,
                txn.timestamp,
                txn.device_id,
                txn.ip_country,
                txn.channel.value if hasattr(txn.channel, 'value') else txn.channel,
            ))
            should_flush = len(self._buffer) >= self.batch_size
        if should_flush:
            self.flush()

    def flush(self):
        with self._lock:
            if not self._buffer:
                return
            rows, self._buffer = self._buffer, []
            txn_rows, self._txn_buffer = self._txn_buffer, []
        try:
            with self._conn.cursor() as cur:
                INSERT_TXN_SQL = """
                INSERT INTO transactions (
                    txn_id, user_id, amount, currency, merchant_id,
                    merchant_category, txn_timestamp, device_id, ip_country, channel
                ) VALUES %s
                ON CONFLICT (txn_id) DO NOTHING
                """
                psycopg2.extras.execute_values(cur, INSERT_TXN_SQL, txn_rows)
                psycopg2.extras.execute_values(cur, INSERT_SQL, rows)
            logger.info("Flushed %d transaction(s) and feature snapshot(s) to Postgres", len(rows))
        except Exception:
            logger.error("Postgres flush failed, re-queuing %d row(s)", len(rows), exc_info=True)
            try:
                self._conn = self._connect()
                self._ensure_schema()
            except Exception:
                logger.error("Postgres reconnect failed", exc_info=True)
            with self._lock:
                self._buffer = rows + self._buffer
                self._txn_buffer = txn_rows + self._txn_buffer

    def _flush_loop(self):
        while not self._stop.is_set():
            self._stop.wait(self.flush_interval)
            self.flush()

    def close(self):
        self._stop.set()
        self._thread.join(timeout=self.flush_interval + 2)
        self.flush()
        self._conn.close()


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------

class FeatureComputer:
    """Owns the Redis rolling-history sorted sets and derives the feature
    vector for each incoming transaction.

    History storage: one Redis sorted set per user,
    `feature_engine:history:{user_id}`, score = txn epoch seconds, member =
    `"{txn_id}|{amount}|{merchant_id}"` (txn_id prefix guarantees uniqueness
    even if amount/merchant repeat). Entries older than the 24h window are
    trimmed lazily on every write (documented tradeoff, see PROJECT_STATE.md
    Phase 0 open issue #2 — O(window size) per event, fine at current
    volumes).
    """

    def __init__(self, r: "redis.Redis"):
        self.r = r

    def compute_and_store(self, txn: Transaction) -> FeatureVector:
        now_ts = txn.timestamp.astimezone(timezone.utc).timestamp()
        hkey = history_key(txn.user_id)
        lkey = last_seen_key(txn.user_id)

        pipe = self.r.pipeline()
        pipe.zremrangebyscore(hkey, 0, now_ts - WINDOW_24H_SECONDS)
        pipe.zrangebyscore(hkey, now_ts - WINDOW_24H_SECONDS, now_ts)
        pipe.hgetall(lkey)
        _, history_24h_raw, last_seen_raw = pipe.execute()

        amounts_24h: list[float] = []
        merchants_24h: set[str] = set()
        merchants_1h: set[str] = set()
        count_1h = 0
        count_24h = 0
        window_1h_start = now_ts - WINDOW_1H_SECONDS

        for member in history_24h_raw:
            member_str = member.decode() if isinstance(member, bytes) else member
            try:
                ts_part, amount_part, merchant_part = member_str.split("|", 2)
            except ValueError:
                continue
            entry_ts = float(ts_part)
            amount = float(amount_part)
            amounts_24h.append(amount)
            merchants_24h.add(merchant_part)
            count_24h += 1
            if entry_ts >= window_1h_start:
                count_1h += 1
                merchants_1h.add(merchant_part)

        amount_mean_24h = statistics.fmean(amounts_24h) if amounts_24h else 0.0
        amount_std_24h = statistics.pstdev(amounts_24h) if len(amounts_24h) >= 2 else 0.0
        if amount_std_24h > 0:
            amount_zscore = (txn.amount - amount_mean_24h) / amount_std_24h
        else:
            amount_zscore = 0.0

        last_seen = {
            k.decode() if isinstance(k, bytes) else k: (v.decode() if isinstance(v, bytes) else v)
            for k, v in (last_seen_raw or {}).items()
        }

        impossible_travel_flag = 0
        seconds_since_last_txn = -1.0
        if last_seen:
            try:
                last_ts = float(last_seen["last_ts"])
                seconds_since_last_txn = max(now_ts - last_ts, 0.0)
                last_country = last_seen.get("last_ip_country", "")
                last_device = last_seen.get("last_device_id", "")
                if (
                    last_country
                    and last_country != txn.ip_country
                    and last_device
                    and last_device != txn.device_id
                    and seconds_since_last_txn <= IMPOSSIBLE_TRAVEL_MAX_SECONDS
                ):
                    impossible_travel_flag = 1
            except (KeyError, ValueError):
                pass

        # Fetch graph features and update graph
        graph_writer.write(txn)
        shared_device_count = get_shared_device_count(txn.user_id)
        shared_merchant_fraud_count = get_shared_merchant_fraud_count(txn.merchant_id)
        hop_distance_to_fraud = get_hop_distance_to_known_fraud(txn.user_id)

        fv = FeatureVector(
            txn_id=txn.txn_id,
            user_id=txn.user_id,
            txn_timestamp=txn.timestamp,
            txn_velocity_1h=count_1h + 1,  # include current txn
            txn_velocity_24h=count_24h + 1,
            amount_mean_24h=round(amount_mean_24h, 6),
            amount_std_24h=round(amount_std_24h, 6),
            amount_zscore=round(amount_zscore, 6) if math.isfinite(amount_zscore) else 0.0,
            distinct_merchants_1h=len(merchants_1h | {txn.merchant_id}),
            distinct_merchants_24h=len(merchants_24h | {txn.merchant_id}),
            impossible_travel_flag=impossible_travel_flag,
            seconds_since_last_txn=round(seconds_since_last_txn, 3),
            shared_device_count=shared_device_count,
            shared_merchant_fraud_count=shared_merchant_fraud_count,
            hop_distance_to_fraud=hop_distance_to_fraud,
        )

        # Validate schema contract before storing anything
        validate_feature_vector(fv)

        member = f"{now_ts}|{txn.amount}|{txn.merchant_id}"
        write_pipe = self.r.pipeline()
        write_pipe.zadd(hkey, {member: now_ts})
        write_pipe.expire(hkey, HISTORY_TTL_SECONDS)
        write_pipe.hset(lkey, mapping={
            "last_ts": repr(now_ts),
            "last_ip_country": txn.ip_country,
            "last_device_id": txn.device_id,
        })
        write_pipe.expire(lkey, HISTORY_TTL_SECONDS)
        fkey = feature_key(txn.user_id)
        write_pipe.hset(fkey, mapping=fv.as_redis_hash())
        write_pipe.expire(fkey, FEATURE_KEY_TTL_SECONDS)
        write_pipe.execute()

        return fv


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting feature_engine consumer=%s group=%s", CONSUMER_NAME, GROUP_NAME)

    r = redis.Redis.from_url(REDIS_URL, decode_responses=False)
    for attempt in range(10):
        try:
            r.ping()
            break
        except redis.exceptions.ConnectionError as e:
            logger.warning("Redis not ready (attempt %d/10): %s", attempt + 1, e)
            time.sleep(min(2 ** attempt, 15))
    else:
        raise RuntimeError("Could not connect to Redis")

    pg_writer = PostgresSnapshotWriter(
        POSTGRES_DSN, POSTGRES_FLUSH_BATCH_SIZE, POSTGRES_FLUSH_INTERVAL_SECONDS
    )
    
    # Initialize Kafka Reader with retry logic
    reader = None
    for attempt in range(15):
        try:
            logger.info("Connecting Kafka Consumer (attempt %d/15)...", attempt + 1)
            reader = TransactionKafkaReader(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_NAME,
                client_id=CONSUMER_NAME,
            )
            break
        except Exception as e:
            logger.warning("Kafka not ready: %s. Retrying in 3s...", e)
            time.sleep(3)
    else:
        logger.error("Could not initialize Kafka Consumer.")
        sys.exit(1)

    computer = FeatureComputer(r)

    stop_flag = threading.Event()

    def handle_signal(signum, frame):
        logger.info("Received signal %s, shutting down gracefully", signum)
        stop_flag.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    processed = 0

    try:
        while not stop_flag.is_set():
            batch = reader.read_batch()
            if not batch:
                continue
            for msg in batch:
                try:
                    fv = computer.compute_and_store(msg.transaction)
                    pg_writer.enqueue(fv.as_postgres_row(), msg.transaction)
                    reader.ack(msg.message_id)  # no-op in Kafka
                    processed += 1
                    if processed % 500 == 0:
                        logger.info("Processed %d transactions", processed)
                except pandera.errors.SchemaError as e:
                    logger.error("Schema validation failed for txn_id=%s", msg.transaction.txn_id)
                    # Push to Dead Letter Queue (DLQ)
                    r.lpush("dlq:feature_engine", msg.transaction.model_dump_json())
                    reader.ack(msg.message_id)
                except Exception:
                    logger.error(
                        "Failed to process message %s (txn_id=%s)",
                        msg.message_id,
                        getattr(msg.transaction, "txn_id", "?"),
                        exc_info=True,
                    )
            # Commit Kafka offsets after processing the batch
            reader.commit()
    finally:
        logger.info("Shutting down; processed=%d total", processed)
        pg_writer.close()
        reader.close()


if __name__ == "__main__":
    main()
