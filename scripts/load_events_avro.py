"""
Produce 100 Avro-serialised events into the test-events-avro topic.
Schema is registered in Schema Registry automatically on first run.

Usage: python3 scripts/load_events_avro.py
"""
import json
import random
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "test-events-avro"
NUM_EVENTS = 100

EVENT_TYPES = ["page_view", "click", "purchase", "signup", "logout"]
SOURCES = ["web", "mobile-ios", "mobile-android", "api"]

SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "TestEvent",
    "namespace": "com.example",
    "fields": [
        {"name": "id",            "type": "string"},
        {"name": "sequence",      "type": "int"},
        {"name": "event_type",    "type": "string"},
        {"name": "source",        "type": "string"},
        {"name": "user_id",       "type": "string"},
        {"name": "timestamp_ms",  "type": "long"},
        {"name": "session_id",    "type": "string"},
        {"name": "value",         "type": "double"},
    ],
})


def delivery_report(err, msg):
    if err:
        print(f"  ERROR: {err}")
    else:
        print(f"  offset={msg.offset():>4}  partition={msg.partition()}  key={msg.key().decode()}")


def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(sr_client, SCHEMA_STR)
    key_serializer = StringSerializer("utf_8")

    p = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "linger.ms": 10,
        "compression.type": "lz4",
    })

    print(f"Producing {NUM_EVENTS} Avro events to '{TOPIC}'...\n")
    for i in range(NUM_EVENTS):
        record = {
            "id":           str(uuid.uuid4()),
            "sequence":     i,
            "event_type":   random.choice(EVENT_TYPES),
            "source":       random.choice(SOURCES),
            "user_id":      f"user-{random.randint(1, 20):03d}",
            "timestamp_ms": int(time.time() * 1000),
            "session_id":   str(uuid.uuid4())[:8],
            "value":        round(random.uniform(1.0, 500.0), 2),
        }
        p.produce(
            TOPIC,
            key=key_serializer(record["user_id"], SerializationContext(TOPIC, MessageField.KEY)),
            value=serializer(record, SerializationContext(TOPIC, MessageField.VALUE)),
            callback=delivery_report,
        )
        if i % 10 == 0:
            p.poll(0)

    remaining = p.flush(timeout=30)
    if remaining:
        print(f"\nWARNING: {remaining} messages were not delivered.")
    else:
        print(f"\nDone. {NUM_EVENTS} Avro events delivered successfully.")
        print(f"Schema registered under subject: {TOPIC}-value")


if __name__ == "__main__":
    main()
