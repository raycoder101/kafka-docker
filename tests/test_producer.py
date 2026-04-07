"""
Producer tests: delivery guarantees, partitioning, headers, serialisation.
"""
import json
import time
import uuid

import pytest
from confluent_kafka import KafkaError, Producer

from tests.conftest import BOOTSTRAP_SERVERS


def _delivery_report(err, msg, *, results: list):
    if err:
        results.append(("error", err))
    else:
        results.append(("ok", msg))


class TestProducerDelivery:
    def test_produce_and_flush_single_message(self, producer, unique_topic):
        results = []
        producer.produce(
            unique_topic,
            key=b"k1",
            value=b"hello-kafka",
            callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)),
        )
        producer.flush(timeout=15)
        assert len(results) == 1
        status, msg = results[0]
        assert status == "ok", f"Delivery failed: {msg}"

    def test_produce_json_payload(self, producer, unique_topic):
        payload = json.dumps({"event": "test", "ts": time.time()}).encode()
        results = []
        producer.produce(
            unique_topic,
            key=b"json-key",
            value=payload,
            callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)),
        )
        producer.flush(timeout=15)
        assert results[0][0] == "ok"

    def test_produce_batch_messages(self, producer, unique_topic):
        n = 50
        results = []
        callback = lambda err, msg: results.append(("error", err) if err else ("ok", msg))
        for i in range(n):
            producer.produce(unique_topic, value=f"msg-{i}".encode(), callback=callback)
        producer.flush(timeout=30)
        assert len(results) == n
        errors = [r for r in results if r[0] == "error"]
        assert not errors, f"Delivery errors: {errors}"

    def test_produce_with_message_headers(self, producer, unique_topic):
        results = []
        producer.produce(
            unique_topic,
            value=b"with-headers",
            headers={"source": "pytest", "version": "1"},
            callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)),
        )
        producer.flush(timeout=15)
        assert results[0][0] == "ok"

    def test_produce_to_specific_partition(self, producer, unique_topic):
        results = []
        producer.produce(
            unique_topic,
            value=b"partition-pinned",
            partition=0,
            callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)),
        )
        producer.flush(timeout=15)
        status, msg_or_err = results[0]
        assert status == "ok"
        assert msg_or_err.partition() == 0

    def test_produce_null_value_tombstone(self, producer, unique_topic):
        """A None value is a valid Kafka tombstone (used for log compaction deletes)."""
        results = []
        producer.produce(
            unique_topic,
            key=b"delete-me",
            value=None,
            callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)),
        )
        producer.flush(timeout=15)
        assert results[0][0] == "ok"


class TestProducerConfiguration:
    def test_acks_all_producer_delivers(self, unique_topic):
        """acks=all should still deliver successfully on a single-node cluster."""
        p = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 5,
        })
        results = []
        p.produce(
            unique_topic,
            value=b"acks-all",
            callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)),
        )
        p.flush(timeout=20)
        assert results[0][0] == "ok"

    def test_idempotent_producer(self, unique_topic):
        """enable.idempotence=true (exactly-once semantics for the producer)."""
        p = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
        })
        results = []
        p.produce(
            unique_topic,
            value=b"idempotent",
            callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)),
        )
        p.flush(timeout=20)
        assert results[0][0] == "ok"
