"""
Shared fixtures for all Kafka integration tests.

The tests assume a running docker-compose stack (kafka + schema-registry).
Start it with:
    docker compose up -d
    # wait for healthy, then:
    pytest tests/ -v
"""
import time
import uuid
from typing import Optional

import pytest
import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# ── Admin client ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def admin_client():
    client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    yield client


# ── Topic helpers ──────────────────────────────────────────────────────────────

def create_topic(admin: AdminClient, name: str, num_partitions: int = 1) -> None:
    """Create a topic, waiting for the operation to finish."""
    topic = NewTopic(name, num_partitions=num_partitions, replication_factor=1)
    futures = admin.create_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result(timeout=15)
        except Exception as exc:
            # Already exists is fine
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"Failed to create topic {topic_name}: {exc}") from exc


def delete_topic(admin: AdminClient, name: str) -> None:
    futures = admin.delete_topics([name])
    for topic_name, future in futures.items():
        try:
            future.result(timeout=15)
        except Exception as exc:
            if "UNKNOWN_TOPIC_OR_PARTITION" not in str(exc):
                raise RuntimeError(f"Failed to delete topic {topic_name}: {exc}") from exc


@pytest.fixture
def unique_topic(admin_client):
    """Yields a unique topic name, creates it before the test, deletes after."""
    name = f"pytest-{uuid.uuid4().hex[:8]}"
    create_topic(admin_client, name, num_partitions=2)
    yield name
    delete_topic(admin_client, name)


# ── Producer ───────────────────────────────────────────────────────────────────

@pytest.fixture
def producer():
    p = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Production-like durability: wait for leader + all ISR acks
        "acks": "all",
        "retries": 3,
        "linger.ms": 5,
        "compression.type": "lz4",
    })
    yield p
    p.flush(timeout=10)


# ── Consumer ───────────────────────────────────────────────────────────────────

@pytest.fixture
def consumer_factory():
    """Returns a factory function so each test can request its own consumer group."""
    consumers = []

    def _make(group_id: Optional[str] = None, auto_offset_reset: str = "earliest"):
        c = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id or f"pytest-cg-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        })
        consumers.append(c)
        return c

    yield _make

    for c in consumers:
        c.close()


# ── Schema Registry ────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def schema_registry_url():
    return SCHEMA_REGISTRY_URL


def wait_for_service(url: str, timeout: int = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"Service at {url} not ready after {timeout}s")


@pytest.fixture(scope="session", autouse=True)
def wait_for_kafka(admin_client):
    """Block until the broker is reachable before any test runs."""
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            meta = admin_client.list_topics(timeout=5)
            if meta:
                return
        except Exception:
            pass
        time.sleep(3)
    raise TimeoutError("Kafka broker not reachable after 60s")


@pytest.fixture(scope="session", autouse=True)
def wait_for_schema_registry(schema_registry_url):
    wait_for_service(f"{schema_registry_url}/subjects")
