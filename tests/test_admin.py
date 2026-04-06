"""
Admin API tests: topic lifecycle, partition inspection, config retrieval.
"""
import uuid

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from tests.conftest import BOOTSTRAP_SERVERS, create_topic, delete_topic


class TestTopicLifecycle:
    def test_create_and_list_topic(self, admin_client):
        name = f"test-admin-{uuid.uuid4().hex[:8]}"
        try:
            create_topic(admin_client, name, num_partitions=2)
            topics = admin_client.list_topics(timeout=10).topics
            assert name in topics, f"Topic {name!r} not found in {list(topics)}"
        finally:
            delete_topic(admin_client, name)

    def test_topic_has_correct_partition_count(self, admin_client):
        name = f"test-partitions-{uuid.uuid4().hex[:8]}"
        partitions = 3
        try:
            create_topic(admin_client, name, num_partitions=partitions)
            meta = admin_client.list_topics(timeout=10).topics[name]
            assert len(meta.partitions) == partitions
        finally:
            delete_topic(admin_client, name)

    def test_delete_topic(self, admin_client):
        name = f"test-delete-{uuid.uuid4().hex[:8]}"
        create_topic(admin_client, name)
        delete_topic(admin_client, name)

        import time
        deadline = __import__("time").time() + 15
        while time.time() < deadline:
            topics = admin_client.list_topics(timeout=5).topics
            if name not in topics:
                return
            time.sleep(1)
        pytest.fail(f"Topic {name!r} still present after deletion")

    def test_idempotent_topic_creation(self, admin_client):
        """Creating an already-existing topic must not raise."""
        name = f"test-idem-{uuid.uuid4().hex[:8]}"
        try:
            create_topic(admin_client, name)
            create_topic(admin_client, name)  # second call — must not raise
        finally:
            delete_topic(admin_client, name)

    def test_describe_topic_config(self, admin_client, unique_topic):
        resource = ConfigResource("topic", unique_topic)
        futures = admin_client.describe_configs([resource])
        config = futures[resource].result(timeout=10)
        assert "retention.ms" in config
        assert "cleanup.policy" in config


class TestBrokerInspection:
    def test_broker_is_reachable(self, admin_client):
        meta = admin_client.list_topics(timeout=10)
        assert meta.brokers, "No brokers found"

    def test_cluster_has_one_broker(self, admin_client):
        meta = admin_client.list_topics(timeout=10)
        assert len(meta.brokers) >= 1

    def test_cluster_id_is_set(self, admin_client):
        meta = admin_client.list_topics(timeout=10)
        # Cluster ID present in KRaft mode
        assert meta.cluster_id is not None and meta.cluster_id != ""
