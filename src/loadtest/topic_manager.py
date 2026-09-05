"""Kafka topic lifecycle management for isolated load testing.

Ensures the dedicated topic 'fleet.telemetry.loadtest' is configured with
multiple partitions (default: 6) and can be cleanly recreated between benchmark runs.
"""

import logging
import time
from typing import Optional

from confluent_kafka.admin import AdminClient, NewTopic, KafkaException

logger = logging.getLogger(__name__)

DEFAULT_LOADTEST_TOPIC = "fleet.telemetry.loadtest"
DEFAULT_PARTITIONS = 6
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


class TopicManager:
    """Manages creation and clean reset of benchmark Kafka topics."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        topic_name: str = DEFAULT_LOADTEST_TOPIC,
        num_partitions: int = DEFAULT_PARTITIONS,
        replication_factor: int = 1,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})

    def topic_exists(self) -> bool:
        """Check if load-test topic currently exists on the broker."""
        metadata = self.admin.list_topics(timeout=5.0)
        return self.topic_name in metadata.topics

    def delete_topic(self, timeout: float = 10.0) -> bool:
        """Delete topic if it exists."""
        if not self.topic_exists():
            return True

        logger.info("Deleting topic '%s'...", self.topic_name)
        futures = self.admin.delete_topics([self.topic_name])
        for topic, future in futures.items():
            try:
                future.result(timeout=timeout)
                logger.info("Topic '%s' deleted successfully.", topic)
            except Exception as e:
                logger.warning("Error deleting topic '%s': %s", topic, e)
                return False

        # Wait briefly for broker metadata propagation
        time.sleep(1.0)
        return True

    def create_topic(self, timeout: float = 10.0) -> bool:
        """Create load-test topic with configured partition count."""
        logger.info(
            "Creating topic '%s' with %d partitions (replication factor: %d)...",
            self.topic_name,
            self.num_partitions,
            self.replication_factor,
        )
        new_topic = NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
        )
        futures = self.admin.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result(timeout=timeout)
                logger.info("Topic '%s' created successfully.", topic)
            except Exception as e:
                logger.error("Failed to create topic '%s': %s", topic, e)
                return False

        # Verify topic metadata
        time.sleep(1.0)
        metadata = self.admin.list_topics(timeout=5.0)
        if self.topic_name in metadata.topics:
            actual_partitions = len(metadata.topics[self.topic_name].partitions)
            logger.info(
                "Topic '%s' verified with %d partitions.",
                self.topic_name,
                actual_partitions,
            )
            return True
        return False

    def reset_topic(self) -> bool:
        """Clean and recreate topic to ensure pristine conditions for benchmark runs."""
        logger.info("Resetting topic '%s' for clean benchmark execution...", self.topic_name)
        if self.topic_exists():
            self.delete_topic()
            time.sleep(1.0)
        return self.create_topic()
