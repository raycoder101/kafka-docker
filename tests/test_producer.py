"""
Producer tests covering the patterns used in load_events.py and load_events_avro.py:
acks=all, keyed messages, JSON payloads, batch production with periodic poll and flush.
"""
import json
import time
import uuid

from confluent_kafka import Producer

from tests.conftest import BOOTSTRAP_SERVERS



class TestProducer:
    def _make_producer(self):
        return Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "linger.ms": 10,
            "compression.type": "lz4",
        })

    def test_produce_keyed_message(self, unique_topic):
        results = []
        p = self._make_producer()
        p.produce(unique_topic, key=b"user-001", value=b"hello",
                  callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)))
        p.flush(timeout=15)
        assert results[0][0] == "ok"

    def test_produce_json_payload(self, unique_topic):
        results = []
        p = self._make_producer()
        payload = json.dumps({"event_type": "purchase", "user_id": "user-001", "value": 42.5}).encode()
        p.produce(unique_topic, key=b"user-001", value=payload,
                  callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)))
        p.flush(timeout=15)
        assert results[0][0] == "ok"

    def test_produce_batch_with_periodic_poll(self, unique_topic):
        """Mirrors the load script pattern: produce N messages, poll every 10, then flush."""
        n = 100
        results = []
        p = self._make_producer()
        callback = lambda err, msg: results.append(("error", err) if err else ("ok", msg))
        for i in range(n):
            p.produce(unique_topic, key=f"user-{i % 20:03d}".encode(),
                      value=json.dumps({"sequence": i}).encode(), callback=callback)
            if i % 10 == 0:
                p.poll(0)
        p.flush(timeout=30)
        assert len(results) == n
        assert not [r for r in results if r[0] == "error"], "Delivery errors in batch"

    def test_idempotent_producer(self, unique_topic):
        """Broker supports idempotent producers — required for exactly-once delivery
        guarantees needed by the Iceberg sink connector."""
        results = []
        p = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
        })
        p.produce(unique_topic, value=b"idempotent",
                  callback=lambda err, msg: results.append(("error", err) if err else ("ok", msg)))
        p.flush(timeout=20)
        assert results[0][0] == "ok"
