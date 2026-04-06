"""
Produce 100 sample events into the test-events topic.
Usage: python3 scripts/load_events.py
"""
import json
import random
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "test-events"
NUM_EVENTS = 100

EVENT_TYPES = ["page_view", "click", "purchase", "signup", "logout"]
SOURCES = ["web", "mobile-ios", "mobile-android", "api"]


def delivery_report(err, msg):
    if err:
        print(f"  ERROR: {err}")
    else:
        print(f"  offset={msg.offset():>4}  partition={msg.partition()}  key={msg.key().decode()}")


def main():
    p = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "linger.ms": 10,
        "compression.type": "lz4",
    })

    print(f"Producing {NUM_EVENTS} events to '{TOPIC}'...\n")
    for i in range(NUM_EVENTS):
        event = {
            "id": str(uuid.uuid4()),
            "sequence": i,
            "event_type": random.choice(EVENT_TYPES),
            "source": random.choice(SOURCES),
            "user_id": f"user-{random.randint(1, 20):03d}",
            "timestamp_ms": int(time.time() * 1000),
            "properties": {
                "session_id": str(uuid.uuid4())[:8],
                "value": round(random.uniform(1.0, 500.0), 2),
            },
        }
        p.produce(
            TOPIC,
            key=event["user_id"].encode(),
            value=json.dumps(event).encode(),
            callback=delivery_report,
        )
        # Poll periodically to trigger delivery callbacks
        if i % 10 == 0:
            p.poll(0)

    remaining = p.flush(timeout=30)
    if remaining:
        print(f"\nWARNING: {remaining} messages were not delivered.")
    else:
        print(f"\nDone. {NUM_EVENTS} events delivered successfully.")


if __name__ == "__main__":
    main()
