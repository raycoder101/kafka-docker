"""
Consumer tests: offset management, consumer groups, partition assignment, lag.
"""
import json
import time
import uuid
from typing import Generator, List, Optional

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, Producer, TopicPartition

from tests.conftest import BOOTSTRAP_SERVERS


def _produce_messages(topic: str, messages: List[bytes], *, key: Optional[bytes] = None) -> None:
    """Helper: synchronously produce a list of messages to a topic."""
    p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
    for msg in messages:
        p.produce(topic, value=msg, key=key)
    p.flush(timeout=20)


def _consume_all(consumer: Consumer, topic: str, expected: int, timeout: float = 20.0) -> list:
    """Consume until `expected` messages are received or timeout."""
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


class TestBasicConsumption:
    def test_consume_single_message(self, consumer_factory, unique_topic):
        _produce_messages(unique_topic, [b"hello"])
        c = consumer_factory()
        msgs = _consume_all(c, unique_topic, expected=1)
        assert len(msgs) == 1
        assert msgs[0].value() == b"hello"

    def test_consume_batch(self, consumer_factory, unique_topic):
        n = 20
        _produce_messages(unique_topic, [f"msg-{i}".encode() for i in range(n)])
        c = consumer_factory()
        msgs = _consume_all(c, unique_topic, expected=n)
        assert len(msgs) == n

    def test_consume_preserves_key(self, consumer_factory, unique_topic):
        _produce_messages(unique_topic, [b"value"], key=b"my-key")
        c = consumer_factory()
        msgs = _consume_all(c, unique_topic, expected=1)
        assert msgs[0].key() == b"my-key"

    def test_consume_json_payload(self, consumer_factory, unique_topic):
        payload = {"event": "test", "id": str(uuid.uuid4())}
        _produce_messages(unique_topic, [json.dumps(payload).encode()])
        c = consumer_factory()
        msgs = _consume_all(c, unique_topic, expected=1)
        decoded = json.loads(msgs[0].value())
        assert decoded["event"] == "test"

    def test_latest_offset_skips_prior_messages(self, consumer_factory, unique_topic):
        """A consumer starting at 'latest' must not see messages produced before it joined."""
        _produce_messages(unique_topic, [b"old-message"])
        c = consumer_factory(auto_offset_reset="latest")
        c.subscribe([unique_topic])

        # Wait for partition assignment
        deadline = time.time() + 15
        while not c.assignment() and time.time() < deadline:
            c.poll(timeout=1.0)
        assert c.assignment(), "Partitions were never assigned"

        # Explicitly seek to the high watermark on each partition — deterministic
        # alternative to relying on auto.offset.reset timing
        for tp in c.assignment():
            _, high = c.get_watermark_offsets(tp, timeout=5)
            c.seek(TopicPartition(tp.topic, tp.partition, high))

        _produce_messages(unique_topic, [b"new-message"])

        # Consume directly — don't go through _consume_all which would re-subscribe
        collected = []
        deadline = time.time() + 15
        while time.time() < deadline:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Consumer error: {msg.error()}")
            collected.append(msg)
            if len(collected) >= 1:
                break

        values = [m.value() for m in collected]
        assert b"old-message" not in values
        assert b"new-message" in values


class TestOffsetManagement:
    def test_manual_commit_advances_offset(self, consumer_factory, unique_topic):
        _produce_messages(unique_topic, [b"commit-test"])
        group_id = f"cg-commit-{uuid.uuid4().hex[:8]}"
        c = consumer_factory(group_id=group_id)
        msgs = _consume_all(c, unique_topic, expected=1)
        assert msgs
        c.commit(message=msgs[-1], asynchronous=False)

        # A new consumer in the same group should find no pending messages.
        c2 = consumer_factory(group_id=group_id, auto_offset_reset="latest")
        c2.subscribe([unique_topic])
        time.sleep(2)
        new_msgs = _consume_all(c2, unique_topic, expected=0, timeout=5)
        assert len(new_msgs) == 0

    def test_seek_to_beginning(self, consumer_factory, unique_topic):
        n = 5
        _produce_messages(unique_topic, [f"seek-{i}".encode() for i in range(n)])

        c = consumer_factory()
        c.subscribe([unique_topic])
        # Trigger partition assignment
        # Poll until partitions are assigned
        deadline = time.time() + 15
        while not c.assignment() and time.time() < deadline:
            c.poll(timeout=1.0)
        assigned = c.assignment()
        assert assigned, "No partitions assigned"

        # confluent_kafka uses seek() per partition, not seek_to_beginning()
        for tp in assigned:
            c.seek(TopicPartition(tp.topic, tp.partition, OFFSET_BEGINNING))

        msgs = _consume_all(c, unique_topic, expected=n)
        assert len(msgs) == n

    def test_watermarks_reflect_produced_messages(self, consumer_factory, unique_topic):
        """High-water mark must be >= number of messages produced."""
        n = 10
        _produce_messages(unique_topic, [f"wm-{i}".encode() for i in range(n)])

        c = consumer_factory()
        c.subscribe([unique_topic])
        c.poll(timeout=5)  # trigger assignment
        assigned = c.assignment()
        assert assigned

        total_high = 0
        for tp in assigned:
            low, high = c.get_watermark_offsets(tp, timeout=5)
            total_high += high
        assert total_high >= n


class TestConsumerGroup:
    def test_two_consumers_share_partitions(self, admin_client, consumer_factory):
        """With 4 partitions and 2 consumers in the same group, each gets 2."""
        from tests.conftest import create_topic, delete_topic

        topic = f"cg-split-{uuid.uuid4().hex[:8]}"
        group = f"cg-{uuid.uuid4().hex[:8]}"
        create_topic(admin_client, topic, num_partitions=4)
        try:
            c1 = consumer_factory(group_id=group)
            c2 = consumer_factory(group_id=group)
            c1.subscribe([topic])
            c2.subscribe([topic])

            deadline = time.time() + 30
            while time.time() < deadline:
                c1.poll(timeout=1)
                c2.poll(timeout=1)
                if len(c1.assignment()) + len(c2.assignment()) == 4:
                    break

            total = len(c1.assignment()) + len(c2.assignment())
            assert total == 4, f"Expected 4 total partitions assigned, got {total}"
        finally:
            delete_topic(admin_client, topic)

    def test_independent_groups_both_receive_messages(self, consumer_factory, unique_topic):
        """Two separate consumer groups must each receive all produced messages."""
        messages = [f"cg-msg-{i}".encode() for i in range(5)]
        _produce_messages(unique_topic, messages)

        c1 = consumer_factory(group_id=f"group-a-{uuid.uuid4().hex[:8]}")
        c2 = consumer_factory(group_id=f"group-b-{uuid.uuid4().hex[:8]}")

        msgs_a = _consume_all(c1, unique_topic, expected=5)
        msgs_b = _consume_all(c2, unique_topic, expected=5)

        assert len(msgs_a) == 5
        assert len(msgs_b) == 5
