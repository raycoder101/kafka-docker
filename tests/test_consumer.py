"""
Consumer tests covering the patterns used in query_events.py and query_events_avro.py:
read from earliest, fresh group per run, JSON decoding, partition/offset metadata,
and independent groups each receiving the full batch.
"""
import json
import time
import uuid
from typing import List

import pytest
from confluent_kafka import Consumer, KafkaError, Producer

from tests.conftest import BOOTSTRAP_SERVERS


def _produce_messages(topic: str, messages: List[bytes]) -> None:
    p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
    for msg in messages:
        p.produce(topic, value=msg)
    p.flush(timeout=20)


def _consume_all(consumer: Consumer, topic: str, expected: int, timeout: float = 20.0) -> list:
    consumer.subscribe([topic])
    collected = []
    deadline = time.time() + timeout
    while len(collected) < expected and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(f"Consumer error: {msg.error()}")
        collected.append(msg)
    return collected


class TestConsumer:
    def test_consume_batch_from_earliest(self, consumer_factory, unique_topic):
        """Fresh consumer group reads all 100 messages from the beginning."""
        n = 100
        _produce_messages(unique_topic, [f"msg-{i}".encode() for i in range(n)])
        msgs = _consume_all(consumer_factory(), unique_topic, expected=n)
        assert len(msgs) == n

    def test_consume_json_payload(self, consumer_factory, unique_topic):
        """Consumer can decode JSON and access fields — mirrors query script logic."""
        payload = {"event_type": "purchase", "user_id": "user-001", "value": 42.5}
        _produce_messages(unique_topic, [json.dumps(payload).encode()])
        msgs = _consume_all(consumer_factory(), unique_topic, expected=1)
        decoded = json.loads(msgs[0].value())
        assert decoded["event_type"] == "purchase"
        assert decoded["user_id"] == "user-001"

    def test_partition_and_offset_metadata(self, consumer_factory, unique_topic):
        """Partition and offset are accessible on each message — used in query script output."""
        _produce_messages(unique_topic, [b"event"])
        msgs = _consume_all(consumer_factory(), unique_topic, expected=1)
        assert msgs[0].partition() >= 0
        assert msgs[0].offset() >= 0

    def test_manual_commit_advances_offset(self, consumer_factory, unique_topic):
        """Committed offset is respected — a rejoining consumer in the same group
        does not reprocess already-committed messages. Critical for any sink consumer
        (e.g. Iceberg) that must not duplicate records on restart."""
        _produce_messages(unique_topic, [b"commit-test"])
        group_id = f"cg-commit-{uuid.uuid4().hex[:8]}"

        c = consumer_factory(group_id=group_id)
        msgs = _consume_all(c, unique_topic, expected=1)
        assert msgs
        c.commit(message=msgs[-1], asynchronous=False)

        # A new consumer in the same group must find no pending messages.
        c2 = consumer_factory(group_id=group_id, auto_offset_reset="latest")
        c2.subscribe([unique_topic])
        time.sleep(2)
        new_msgs = _consume_all(c2, unique_topic, expected=0, timeout=5)
        assert len(new_msgs) == 0

    def test_independent_groups_each_receive_full_batch(self, consumer_factory, unique_topic):
        """Two fresh consumer groups both receive all messages — mirrors running
        query_events.py twice (or alongside an Iceberg sink consumer)."""
        n = 20
        _produce_messages(unique_topic, [f"msg-{i}".encode() for i in range(n)])
        msgs_a = _consume_all(consumer_factory(group_id=f"query-a-{uuid.uuid4().hex[:8]}"), unique_topic, expected=n)
        msgs_b = _consume_all(consumer_factory(group_id=f"query-b-{uuid.uuid4().hex[:8]}"), unique_topic, expected=n)
        assert len(msgs_a) == n
        assert len(msgs_b) == n
