"""
Query Avro events from the test-events-avro topic.
Deserialises using Schema Registry — no need to specify the schema manually.

Usage:
  python3 scripts/query_events_avro.py                        # all events
  python3 scripts/query_events_avro.py --event-type purchase  # filter by event_type
  python3 scripts/query_events_avro.py --source mobile-ios    # filter by source
  python3 scripts/query_events_avro.py --user-id user-007     # filter by user_id
  python3 scripts/query_events_avro.py --limit 10             # first N matches
"""
import argparse
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "test-events-avro"


def parse_args():
    p = argparse.ArgumentParser(description="Query test-events-avro topic")
    p.add_argument("--event-type", help="Filter by event_type")
    p.add_argument("--source",     help="Filter by source")
    p.add_argument("--user-id",    help="Filter by user_id")
    p.add_argument("--limit", type=int, default=0, help="Max records to print (0 = all)")
    return p.parse_args()


def matches(event: dict, args) -> bool:
    if args.event_type and event.get("event_type") != args.event_type:
        return False
    if args.source and event.get("source") != args.source:
        return False
    if args.user_id and event.get("user_id") != args.user_id:
        return False
    return True


def main():
    args = parse_args()

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(sr_client)

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"avro-query-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([TOPIC])

    print(f"Reading Avro events from '{TOPIC}' (beginning)...\n")

    all_events = []
    idle_polls = 0

    while idle_polls < 5:
        msg = c.poll(timeout=2.0)
        if msg is None:
            idle_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                idle_polls += 1
            else:
                print(f"ERROR: {msg.error()}")
            continue
        idle_polls = 0
        event = deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
        event["_partition"] = msg.partition()
        event["_offset"] = msg.offset()
        all_events.append(event)

    c.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"{'─' * 60}")
    print(f"Total events read : {len(all_events)}")

    event_type_counts = Counter(e.get("event_type") for e in all_events)
    source_counts     = Counter(e.get("source")     for e in all_events)
    user_counts       = Counter(e.get("user_id")    for e in all_events)

    print(f"\nEvent types:")
    for k, v in sorted(event_type_counts.items()):
        print(f"  {k:<20} {v:>4}")

    print(f"\nSources:")
    for k, v in sorted(source_counts.items()):
        print(f"  {k:<20} {v:>4}")

    print(f"\nTop 5 users by event count:")
    for user, count in user_counts.most_common(5):
        print(f"  {user:<20} {count:>4}")

    total_value = sum(e.get("value", 0) for e in all_events)
    print(f"\nTotal value (sum) : {total_value:,.2f}")
    print(f"Avg value         : {total_value / len(all_events):,.2f}" if all_events else "")

    # ── Filtered results ───────────────────────────────────────────────────────
    filtered = [e for e in all_events if matches(e, args)]
    active_filters = {k: v for k, v in vars(args).items() if v and k != "limit"}

    print(f"\n{'─' * 60}")
    if active_filters:
        print(f"Filters : {active_filters}")
        print(f"Matched : {len(filtered)} / {len(all_events)} events\n")
    else:
        print(f"(No filters — showing all events)\n")

    to_print = filtered[:args.limit] if args.limit else filtered
    for e in to_print:
        print(
            f"  [{e['_partition']}:{e['_offset']:>4}]"
            f"  seq={e.get('sequence'):>3}"
            f"  {e.get('event_type'):<12}"
            f"  {e.get('source'):<18}"
            f"  {e.get('user_id'):<12}"
            f"  val={e.get('value'):>7.2f}"
        )

    if args.limit and len(filtered) > args.limit:
        print(f"\n  ... {len(filtered) - args.limit} more (increase --limit to see all)")


if __name__ == "__main__":
    main()
