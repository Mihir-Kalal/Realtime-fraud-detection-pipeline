import json
import logging
import time
import os
from kafka import KafkaProducer
import msgpack
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dlq_stress_test")

KAFKA_TOPIC = "transactions-raw"

def main():
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        acks="all"
    )
    
    # 14 distinct schema violations
    poison_pills = [
        {"txn_id": "bad_1", "amount": "NOT_A_FLOAT", "currency": "USD", "merchant_id": "m1", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d1", "ip_country": "US", "channel": "login"}, # 1. Missing user_id
        {"txn_id": "bad_2", "user_id": "u2", "amount": "STILL_NOT_FLOAT", "currency": "USD", "merchant_id": "m2", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d2", "ip_country": "US", "channel": "login"}, # 2. Wrong type (amount)
        {"txn_id": "bad_3", "user_id": "u3", "amount": -100.5, "currency": "USD", "merchant_id": "m3", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d3", "ip_country": "US", "channel": "login"}, # 3. Boundary violation (amount < 0)
        {"txn_id": "bad_4", "user_id": "u4", "currency": "USD", "merchant_id": "m4", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d4", "ip_country": "US", "channel": "login"}, # 4. Missing amount
        {"txn_id": "bad_5", "user_id": "u5", "amount": 10.0, "currency": 12345, "merchant_id": "m5", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d5", "ip_country": "US", "channel": "login"}, # 5. Wrong type (currency is int)
        {"txn_id": "bad_6", "user_id": "u6", "amount": 10.0, "currency": "USD", "merchant_id": {"id": "m6"}, "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d6", "ip_country": "US", "channel": "login"}, # 6. Wrong type (merchant_id is dict)
        {"txn_id": "bad_7", "user_id": "u7", "amount": 10.0, "currency": "USD", "merchant_id": "m7", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d7", "ip_country": "US", "channel": "login"}, # 7. Missing merchant_category
        {"txn_id": "bad_8", "user_id": "u8", "amount": 10.0, "currency": "USD", "merchant_id": "m8", "merchant_category": "retail", "timestamp": "yesterday", "device_id": "d8", "ip_country": "US", "channel": "login"}, # 8. Malformed timestamp string
        {"txn_id": "bad_9", "user_id": "u9", "amount": 10.0, "currency": "USD", "merchant_id": "m9", "merchant_category": "retail", "device_id": "d9", "ip_country": "US", "channel": "login"}, # 9. Missing timestamp
        {"txn_id": "bad_10", "user_id": "u10", "amount": 10.0, "currency": "USD", "merchant_id": "m10", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": ["d10"], "ip_country": "US", "channel": "login"}, # 10. Wrong type (device_id is list)
        {"txn_id": "bad_11", "user_id": "u11", "amount": 10.0, "currency": "USD", "merchant_id": "m11", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d11", "ip_country": 404, "channel": "login"}, # 11. Wrong type (ip_country is int)
        {"txn_id": "bad_12", "user_id": "u12", "amount": 10.0, "currency": "USD", "merchant_id": "m12", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d12", "ip_country": "US", "channel": "telepathy"}, # 12. Invalid Enum value (channel)
        {"txn_id": "bad_13", "user_id": "u13", "amount": 10.0, "currency": "USD", "merchant_id": "m13", "merchant_category": "retail", "timestamp": "2023-01-01T00:00:00Z", "device_id": "d13", "ip_country": "US"}, # 13. Missing channel
        {} # 14. Completely empty payload
    ]
    
    logger.info(f"Injecting {len(poison_pills)} distinct malformed schema payloads to stress test the DLQ...")
    
    for i, bad_transaction in enumerate(poison_pills):
        fields = {"data": bad_transaction}
        serialized = msgpack.packb(fields)
        
        future = producer.send(KAFKA_TOPIC, value=serialized)
        metadata = future.get(timeout=5)
        logger.info(f"Injected Poison Pill {i+1}/{len(poison_pills)} -> Partition: {metadata.partition}, Offset: {metadata.offset}")
        time.sleep(0.1)

    producer.close()
    logger.info(f"Successfully injected {len(poison_pills)} distinct schema failures. Verify that Redis list 'dlq:feature_engine' caught all 14.")

if __name__ == "__main__":
    main()
