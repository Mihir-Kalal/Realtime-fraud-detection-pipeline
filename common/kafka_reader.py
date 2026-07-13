"""
Shared Kafka reader for the `transactions-raw` topic.

Replaces Redis Streams with Apache Kafka for scalable, distributed transaction ingestion.
"""

from __future__ import annotations

import json
import msgpack
import logging
from dataclasses import dataclass
from typing import Iterator

from kafka import KafkaConsumer
from common.schemas import Transaction

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "transactions-raw"


@dataclass
class StreamMessage:
    """A single delivered Kafka message, mapped to the StreamMessage contract."""
    message_id: str
    transaction: Transaction


def deserialize_message(x: bytes) -> dict:
    try:
        return msgpack.unpackb(x)
    except Exception:
        # Fallback to json for backward compatibility during rolling restart
        return json.loads(x.decode("utf-8"))


class TransactionKafkaReader:
    """
    Kafka consumer group reader over `transactions-raw` topic.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        client_id: str,
        topic: str = KAFKA_TOPIC,
        poll_timeout_ms: int = 1000,
        batch_size: int = 50,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.client_id = client_id
        self.poll_timeout_ms = poll_timeout_ms
        self.batch_size = batch_size

        logger.info(
            "Initializing Kafka Consumer for topic=%s, group=%s, client=%s on %s",
            self.topic,
            self.group_id,
            self.client_id,
            self.bootstrap_servers,
        )

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            client_id=self.client_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=deserialize_message,
        )

    def read_batch(self) -> list[StreamMessage]:
        """Read up to `batch_size` new messages from Kafka partitions."""
        messages: list[StreamMessage] = []
        try:
            # poll returns a dict of TopicPartition to list of ConsumerRecords
            records = self.consumer.poll(
                timeout_ms=self.poll_timeout_ms,
                max_records=self.batch_size
            )
            for _tp, recs in records.items():
                for rec in recs:
                    try:
                        val = rec.value
                        # Extract the data field which contains JSON transaction payload
                        data_payload = val.get("data")
                        if isinstance(data_payload, str):
                            txn = Transaction.model_validate_json(data_payload)
                        else:
                            txn = Transaction.model_validate(data_payload)
                        
                        messages.append(
                            StreamMessage(
                                message_id=f"{rec.partition}:{rec.offset}",
                                transaction=txn
                            )
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to deserialize Kafka message at partition=%d, offset=%d: %s. "
                            "Skipping to prevent poison pill from blocking partition.",
                            rec.partition,
                            rec.offset,
                            exc,
                        )
        except Exception as exc:
            logger.error("Error polling messages from Kafka: %s", exc)

        return messages

    def ack(self, message_id: str) -> None:
        """Ack is a no-op for individual Kafka messages. Call commit() instead."""
        pass

    def commit(self) -> None:
        """Commit the current offsets for all assigned partitions synchronously."""
        try:
            self.consumer.commit()
        except Exception as exc:
            logger.error("Failed to commit Kafka offsets: %s", exc)

    def close(self) -> None:
        """Close the Kafka consumer connection."""
        try:
            self.consumer.close()
        except Exception as exc:
            logger.error("Error closing Kafka consumer: %s", exc)
