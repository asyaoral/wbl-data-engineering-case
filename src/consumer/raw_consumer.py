"""Raw telemetry consumer and local file storage handler.

Milestone 1 requirement:
- Read events from Kafka topic 'fleet.telemetry.raw'
- Do NOT deduplicate, clean, or reject anything yet
- Preserve incoming records for the Raw layer
- Store raw records locally in data/raw/
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "raw"


class RawStorageWriter:
    """Manages appending raw telemetry records verbatim to local storage."""

    def __init__(self, raw_dir: Path = DEFAULT_RAW_DIR) -> None:
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self._current_file = None
        self._current_date_str = ""

    def _get_target_filepath(self) -> Path:
        """Partition raw files by ingestion date."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self.raw_dir / f"telemetry_raw_{date_str}.jsonl"

    def write_record(self, raw_payload: str) -> None:
        """Write a raw line verbatim to the raw storage file.

        No transformations, deduplications, or schema enforcement are applied.
        """
        filepath = self._get_target_filepath()
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(raw_payload.strip() + "\n")

    def write_event_dict(self, event: Dict[str, Any]) -> None:
        """Serialize and write event dictionary."""
        self.write_record(json.dumps(event))


class RawTelemetryConsumer:
    """Consumes raw events from Kafka and writes them directly to raw storage."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "fleet.telemetry.raw",
        group_id: str = "fleet-raw-consumer-group",
        raw_dir: Path = DEFAULT_RAW_DIR,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.writer = RawStorageWriter(raw_dir=raw_dir)
        self.auto_offset_reset = auto_offset_reset
        self._consumer: Optional[Consumer] = None
        self.is_running = False

    def _init_consumer(self) -> Consumer:
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        return consumer

    def run(self, max_messages: int = 0, poll_timeout: float = 1.0) -> int:
        """Start consumption loop.

        Args:
            max_messages: Stop after consuming this many messages (0 = run indefinitely).
            poll_timeout: Seconds to wait on Kafka poll.

        Returns:
            Number of raw messages successfully persisted.
        """
        self._consumer = self._init_consumer()
        self.is_running = True
        logger.info(
            "Consumer started for topic '%s' (group: '%s', target: %s)",
            self.topic,
            self.group_id,
            self.writer.raw_dir,
        )

        consumed_count = 0
        try:
            while self.is_running:
                msg = self._consumer.poll(timeout=poll_timeout)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition %s [%d]", msg.topic(), msg.partition())
                    else:
                        logger.error("Kafka error: %s", msg.error())
                    continue

                # Preserve raw string payload verbatim
                raw_payload = msg.value().decode("utf-8")
                self.writer.write_record(raw_payload)

                consumed_count += 1
                if consumed_count % 10 == 0:
                    logger.info("Persisted %d raw events to %s", consumed_count, self.writer.raw_dir)

                if max_messages > 0 and consumed_count >= max_messages:
                    logger.info("Reached target of %d messages. Stopping consumer.", max_messages)
                    break

        except KeyboardInterrupt:
            logger.info("Shutdown signal received.")
        finally:
            self.close()

        return consumed_count

    def close(self) -> None:
        """Close Kafka consumer cleanly."""
        self.is_running = False
        if self._consumer is not None:
            logger.info("Closing Kafka consumer...")
            self._consumer.close()
            self._consumer = None
