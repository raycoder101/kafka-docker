"""
Query events from the test-events topic with optional filters.
Reads from the beginning and prints a summary + matching records.

Usage:
  python3 scripts/query_events.py                        # all events
  python3 scripts/query_events.py --event-type purchase  # filter by event_type
  python3 scripts/query_events.py --source mobile-ios    # filter by source
  python3 scripts/query_events.py --user-id user-007     # filter by user_id
  python3 scripts/query_events.py --limit 10             # first N matches
"""
import argparse
import json
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = "localhost:9092"
TOPIC = "test-events"


def parse_args():
    p = argparse.ArgumentParser(description="Query test-events topic")
    p.add_argument("--event-type", help="Filter by event_type")
    p.add_argument("--source", help="Filter by source")
    p.add_argument("--user-id", help="Filter by user_id")
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

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"query-cli-{int(time.time())}",  # fresh group each run
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([TOPIC])

    print(f"Reading from '{TOPIC}' (beginning)...\n")

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
        try:
            event = json.loads(msg.value())
            event["_partition"] = msg.partition()
            event["_offset"] = msg.offset()
            all_events.append(event)
        except json.JSONDecodeError:
            print(f"  Skipping non-JSON message at offset {msg.offset()}")

    c.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"{'─' * 60}")
    print(f"Total events read : {len(all_events)}")

    event_type_counts = Counter(e.get("event_type") for e in all_events)
    source_counts = Counter(e.get("source") for e in all_events)
    user_counts = Counter(e.get("user_id") for e in all_events)

    print(f"\nEvent types:")
    for k, v in sorted(event_type_counts.items()):
        print(f"  {k:<20} {v:>4}")

    print(f"\nSources:")
    for k, v in sorted(source_counts.items()):
        print(f"  {k:<20} {v:>4}")

    print(f"\nTop 5 users by event count:")
    for user, count in user_counts.most_common(5):
        print(f"  {user:<20} {count:>4}")

    # ── Filtered results ───────────────────────────────────────────────────────
    filtered = [e for e in all_events if matches(e, args)]
    active_filters = {k: v for k, v in vars(args).items() if v and k != "limit"}

    if active_filters:
        print(f"\n{'─' * 60}")
        print(f"Filters: {active_filters}")
        print(f"Matched : {len(filtered)} / {len(all_events)} events\n")
    else:
        print(f"\n{'─' * 60}")
        print(f"(No filters applied — showing all events)\n")

    to_print = filtered[:args.limit] if args.limit else filtered
    for e in to_print:
        print(
            f"  [{e['_partition']}:{e['_offset']:>4}]"
            f"  seq={e.get('sequence'):>3}"
            f"  {e.get('event_type'):<12}"
            f"  {e.get('source'):<18}"
            f"  {e.get('user_id')}"
        )

    if args.limit and len(filtered) > args.limit:
        print(f"\n  ... {len(filtered) - args.limit} more (increase --limit to see all)")


if __name__ == "__main__":
    main()
