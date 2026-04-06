"""
Schema Registry tests: subject registration, schema evolution, compatibility.
These tests also validate the Avro producer/consumer path that Iceberg typically uses.
"""
import json
import time
import uuid

import pytest
import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

from tests.conftest import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL

SR_CONF = {"url": SCHEMA_REGISTRY_URL}

# ── Avro schemas ───────────────────────────────────────────────────────────────

SCHEMA_V1 = json.dumps({
    "type": "record",
    "name": "TestEvent",
    "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "timestamp_ms", "type": "long"},
    ],
})

# V2 adds an optional field — backward-compatible evolution (Iceberg-friendly)
SCHEMA_V2 = json.dumps({
    "type": "record",
    "name": "TestEvent",
    "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "timestamp_ms", "type": "long"},
        {"name": "source", "type": ["null", "string"], "default": None},
    ],
})

# Incompatible: adds a required field with no default → BACKWARD-incompatible
# (new schema consumers can't deserialise old data that's missing this field)
SCHEMA_INCOMPATIBLE = json.dumps({
    "type": "record",
    "name": "TestEvent",
    "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "timestamp_ms", "type": "long"},
        {"name": "required_new_field", "type": "string"},  # no default → breaks old readers
    ],
})


class TestSubjectRegistration:
    def test_register_schema(self, schema_registry_url):
        subject = f"pytest-{uuid.uuid4().hex[:8]}-value"
        resp = requests.post(
            f"{schema_registry_url}/subjects/{subject}/versions",
            json={"schema": SCHEMA_V1},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        assert resp.status_code == 200, resp.text
        assert "id" in resp.json()

    def test_retrieve_registered_schema(self, schema_registry_url):
        subject = f"pytest-{uuid.uuid4().hex[:8]}-value"
        resp = requests.post(
            f"{schema_registry_url}/subjects/{subject}/versions",
            json={"schema": SCHEMA_V1},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        schema_id = resp.json()["id"]

        get_resp = requests.get(f"{schema_registry_url}/schemas/ids/{schema_id}")
        assert get_resp.status_code == 200
        retrieved = json.loads(get_resp.json()["schema"])
        assert retrieved["name"] == "TestEvent"

    def test_list_subjects(self, schema_registry_url):
        subject = f"pytest-{uuid.uuid4().hex[:8]}-value"
        requests.post(
            f"{schema_registry_url}/subjects/{subject}/versions",
            json={"schema": SCHEMA_V1},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        resp = requests.get(f"{schema_registry_url}/subjects")
        assert resp.status_code == 200
        assert subject in resp.json()


class TestSchemaCompatibility:
    def _register(self, url: str, subject: str, schema: str) -> int:
        resp = requests.post(
            f"{url}/subjects/{subject}/versions",
            json={"schema": schema},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        assert resp.status_code == 200, resp.text
        return resp.json()["id"]

    def test_backward_compatible_evolution(self, schema_registry_url):
        """Adding an optional field (V2) must be accepted under BACKWARD compatibility."""
        subject = f"pytest-compat-{uuid.uuid4().hex[:8]}-value"
        self._register(schema_registry_url, subject, SCHEMA_V1)

        check = requests.post(
            f"{schema_registry_url}/compatibility/subjects/{subject}/versions/latest",
            json={"schema": SCHEMA_V2},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        assert check.status_code == 200
        assert check.json().get("is_compatible") is True

    def test_incompatible_schema_rejected(self, schema_registry_url):
        """Adding a required field with no default must NOT be backward-compatible."""
        subject = f"pytest-incompat-{uuid.uuid4().hex[:8]}-value"
        self._register(schema_registry_url, subject, SCHEMA_V1)

        check = requests.post(
            f"{schema_registry_url}/compatibility/subjects/{subject}/versions/latest",
            json={"schema": SCHEMA_INCOMPATIBLE},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        assert check.status_code == 200
        assert check.json().get("is_compatible") is False


class TestAvroProducerConsumer:
    """End-to-end Avro serialisation test — mirrors the Iceberg ingestion path."""

    def test_produce_and_consume_avro_message(self, unique_topic):
        sr_client = SchemaRegistryClient(SR_CONF)
        from confluent_kafka.schema_registry import Schema

        serializer = AvroSerializer(sr_client, SCHEMA_V1)
        deserializer = AvroDeserializer(sr_client, SCHEMA_V1)

        record = {
            "id": str(uuid.uuid4()),
            "event_type": "page_view",
            "timestamp_ms": int(time.time() * 1000),
        }

        # Produce
        p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
        p.produce(
            unique_topic,
            key=StringSerializer("utf_8")(record["id"], SerializationContext(unique_topic, MessageField.KEY)),
            value=serializer(record, SerializationContext(unique_topic, MessageField.VALUE)),
        )
        p.flush(timeout=15)

        # Consume
        c = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"avro-cg-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c.subscribe([unique_topic])

        decoded = None
        deadline = time.time() + 20
        while time.time() < deadline:
            msg = c.poll(timeout=1.0)
            if msg and not msg.error():
                decoded = deserializer(msg.value(), SerializationContext(unique_topic, MessageField.VALUE))
                break
        c.close()

        assert decoded is not None, "No Avro message received"
        assert decoded["id"] == record["id"]
        assert decoded["event_type"] == "page_view"
