import json
import logging
import time
from kafka import KafkaProducer
import msgpack

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dlq_stress_test")

KAFKA_TOPIC = "transactions-raw"
NUM_POISON_PILLS = 14

import os

def main():
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        acks="all"
    )
    
    logger.info(f"Injecting {NUM_POISON_PILLS} malformed schema payloads to stress test the DLQ...")
    
    for i in range(NUM_POISON_PILLS):
        # Malformed payload (missing required fields like 'user_id', amount is a string)
        bad_transaction = {
            "txn_id": f"bad_txn_{i}",
            "amount": "NOT_A_FLOAT",  # Should cause Pydantic validation error
            "currency": "INR",
            # Missing user_id entirely
        }
        
        # msgpack the payload just like the real producer
        fields = {"data": bad_transaction}
        serialized = msgpack.packb(fields)
        
        future = producer.send(KAFKA_TOPIC, value=serialized)
        metadata = future.get(timeout=5)
        logger.info(f"Injected Poison Pill {i+1}/{NUM_POISON_PILLS} -> Partition: {metadata.partition}, Offset: {metadata.offset}")
        time.sleep(0.1)

    producer.close()
    logger.info(f"Successfully injected {NUM_POISON_PILLS} schema failures. Verify that Redis list 'dlq:feature_engine' caught all 14.")

if __name__ == "__main__":
    main()
