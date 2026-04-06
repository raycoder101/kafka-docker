#!/usr/bin/env bash
# Creates the standard set of topics used in this environment.
# Run after the kafka container is healthy:
#   docker compose up -d && ./scripts/create_topics.sh
set -euo pipefail

BOOTSTRAP="localhost:9092"
CONTAINER="kafka"

create_topic() {
  local name=$1
  local partitions=$2
  local replication=$3
  local retention_ms=$4

  echo "Creating topic: $name (partitions=$partitions, replication=$replication)"

  docker exec "$CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$replication" \
    --config retention.ms="$retention_ms" \
    --config cleanup.policy=delete
}

# ── Test / dev topics ──────────────────────────────────────────────────────────
create_topic "test-events"          4  1  604800000   # 7 days
create_topic "test-events-avro"     4  1  604800000   # 7 days — Schema Registry demo
create_topic "test-dlq"             2  1  2592000000  # 30 days — dead-letter queue

# ── Future datalake ingestion topics (placeholders) ───────────────────────────
# Uncomment when the Iceberg / S3 connector is wired up.
# create_topic "datalake-raw"         8  1  604800000
# create_topic "datalake-processed"   8  1  604800000

echo ""
echo "Listing all topics:"
docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list
