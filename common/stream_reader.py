"""
Shared consumer-group-aware reader for the `transactions:raw` Redis Stream.

Any service that needs to consume raw transaction events (feature_engine in
Phase 2, and potentially others later) should import `TransactionStreamReader`
from here instead of writing its own XREADGROUP loop. This keeps consumer
group naming, dead-letter handling, and Transaction deserialization in one
place.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Callable, Iterator

import redis

from common.schemas import Transaction

logger = logging.getLogger(__name__)

# Canonical stream name — matches producer/main.py and PROJECT_STATE.md.
TRANSACTIONS_STREAM = "transactions:raw"

# Naming convention for consumer groups: "<service_name>-cg"
# e.g. feature_engine's group is "feature_engine-cg". Services should pass
# their own group name in; this module does not hardcode a single group so
# multiple independent consumers (feature_engine, monitoring, etc.) can each
# read the full stream independently via their own group.


@dataclass
class StreamMessage:
    """A single delivered message, ready to ack."""

    message_id: str
    transaction: Transaction


class TransactionStreamReader:
    """
    Consumer-group-aware reader over `transactions:raw`.

    Usage:
        reader = TransactionStreamReader(
            redis_client=r,
            group_name="feature_engine-cg",
            consumer_name="feature_engine-1",
        )
        for msg in reader.read_forever():
            handle(msg.transaction)
            reader.ack(msg.message_id)
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        group_name: str,
        consumer_name: str,
        stream_name: str = TRANSACTIONS_STREAM,
        block_ms: int = 5000,
        batch_size: int = 50,
        start_id: str = "0",
    ) -> None:
        self.redis = redis_client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.block_ms = block_ms
        self.batch_size = batch_size
        self._ensure_group(start_id)

    def _ensure_group(self, start_id: str) -> None:
        try:
            self.redis.xgroup_create(
                name=self.stream_name,
                groupname=self.group_name,
                id=start_id,
                mkstream=True,
            )
            logger.info(
                "Created consumer group %s on stream %s",
                self.group_name,
                self.stream_name,
            )
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(
                    "Consumer group %s already exists on %s",
                    self.group_name,
                    self.stream_name,
                )
            else:
                raise

    @staticmethod
    def _deserialize(fields: dict) -> Transaction:
        # Producer writes a single field "data" containing the JSON-encoded
        # Transaction (see producer/main.py). Keep this the single point of
        # truth for wire format so it only needs to change in one place.
        raw = fields.get("data") or fields.get(b"data")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return Transaction.model_validate_json(raw)

    def read_batch(self) -> list[StreamMessage]:
        """Read up to `batch_size` new messages, blocking up to `block_ms`."""
        response = self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_name: ">"},
            count=self.batch_size,
            block=self.block_ms,
        )
        messages: list[StreamMessage] = []
        if not response:
            return messages
        for _stream, entries in response:
            for message_id, fields in entries:
                try:
                    txn = self._deserialize(fields)
                except Exception:
                    logger.exception(
                        "Failed to deserialize message %s on stream %s; "
                        "acking to avoid poison-pill blocking (dead-letter "
                        "handling is a Phase 1 open issue, see PROJECT_STATE.md)",
                        message_id,
                        self.stream_name,
                    )
                    self.ack(message_id)
                    continue
                messages.append(StreamMessage(message_id=message_id, transaction=txn))
        return messages

    def read_forever(
        self, stop_flag: Callable[[], bool] | None = None
    ) -> Iterator[StreamMessage]:
        """Yield messages indefinitely until stop_flag() returns True."""
        while stop_flag is None or not stop_flag():
            for msg in self.read_batch():
                yield msg

    def ack(self, message_id: str) -> None:
        self.redis.xack(self.stream_name, self.group_name, message_id)

    def claim_stale(self, min_idle_ms: int = 60_000, count: int = 50) -> list[StreamMessage]:
        """
        Claim messages that were delivered to another consumer in this group
        but never acked (consumer crash, etc.). Call periodically from a
        service's main loop for basic self-healing.
        """
        result = self.redis.xautoclaim(
            name=self.stream_name,
            groupname=self.group_name,
            consumername=self.consumer_name,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
        # xautoclaim returns (next_cursor, claimed_entries, deleted_ids)
        _next_cursor, entries, _deleted = result
        messages: list[StreamMessage] = []
        for message_id, fields in entries:
            try:
                txn = self._deserialize(fields)
            except Exception:
                logger.exception("Failed to deserialize claimed message %s", message_id)
                self.ack(message_id)
                continue
            messages.append(StreamMessage(message_id=message_id, transaction=txn))
        return messages
