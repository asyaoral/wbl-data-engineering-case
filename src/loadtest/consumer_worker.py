"""Consumer worker thread for live load testing.

Features:
- Subscribes to 'fleet.telemetry.loadtest' within a shared consumer group
- Exposes live consumer current position (partition -> last offset + 1) for accurate backlog tracking
- Measures live Kafka publish-to-consume latency within local machine clock domain
- Commits offsets periodically (async) to maintain commit progress
- Appends to worker-specific isolated JSONL file to prevent unsafe concurrent writes
"""

from dataclasses import dataclass, field
import json
import logging
from pathlib import Path
import threading
import time
from typing import Dict, List, Optional, Set

from confluent_kafka import Consumer, KafkaError, Message

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "fleet.telemetry.loadtest"


@dataclass
class WorkerMetrics:
    worker_id: int
    consumed_count: int = 0
    latencies_ms: List[float] = field(default_factory=list)
    seq_numbers_seen: Set[int] = field(default_factory=set)
    first_received_time: Optional[float] = None
    last_received_time: Optional[float] = None
    error_count: int = 0


class ConsumerWorker(threading.Thread):
    """Independent consumer thread running in a consumer group."""

    def __init__(
        self,
        worker_id: int,
        group_id: str,
        output_dir: Path,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        topic: str = DEFAULT_TOPIC,
        poll_timeout: float = 0.2,
    ) -> None:
        super().__init__(daemon=True, name=f"ConsumerWorker-{worker_id}")
        self.worker_id = worker_id
        self.group_id = group_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.poll_timeout = poll_timeout

        self.output_file = self.output_dir / f"worker_{self.worker_id}.jsonl"
        self._is_running = False
        self._stop_requested = False
        self.metrics = WorkerMetrics(worker_id=worker_id)
        self._consumer: Optional[Consumer] = None

        # Live partition positions: partition -> next offset (last offset + 1)
        self._current_positions: Dict[int, int] = {}
        self._pos_lock = threading.Lock()
        self._uncommitted_count = 0

    def get_positions(self) -> Dict[int, int]:
        """Return a snapshot of current consumed positions per partition."""
        with self._pos_lock:
            return dict(self._current_positions)

    def _init_consumer(self) -> Consumer:
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": f"loadtest-consumer-{self.worker_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 250,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 50,
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        return consumer

    def run(self) -> None:
        self._consumer = self._init_consumer()
        self._is_running = True
        logger.info(
            "Worker %d started (group: %s, target file: %s)",
            self.worker_id,
            self.group_id,
            self.output_file.name,
        )

        with open(self.output_file, "w", encoding="utf-8") as out_fp:
            while not self._stop_requested:
                msg = self._consumer.poll(self.poll_timeout)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Worker %d Kafka error: %s", self.worker_id, msg.error())
                        self.metrics.error_count += 1
                    continue

                now_ms = time.time() * 1000.0
                now_sec = now_ms / 1000.0

                if self.metrics.first_received_time is None:
                    self.metrics.first_received_time = now_sec
                self.metrics.last_received_time = now_sec

                # Update live consumer current position (partition -> offset + 1)
                part = msg.partition()
                offset_next = msg.offset() + 1
                with self._pos_lock:
                    self._current_positions[part] = offset_next

                # Calculate live publish-to-consume latency
                ts_type, pub_time_ms = msg.timestamp()
                if pub_time_ms > 0:
                    latency_ms = max(0.0, now_ms - pub_time_ms)
                    self.metrics.latencies_ms.append(latency_ms)

                # Parse seq_num for integrity check
                payload_str = msg.value().decode("utf-8")
                try:
                    event_data = json.loads(payload_str)
                    if "seq_num" in event_data:
                        self.metrics.seq_numbers_seen.add(event_data["seq_num"])
                except Exception:
                    pass

                # Write safely to worker-isolated file
                out_fp.write(payload_str + "\n")
                self.metrics.consumed_count += 1
                self._uncommitted_count += 1

                # Periodically trigger async commit to prevent commit lag drift
                if self._uncommitted_count >= 500:
                    try:
                        self._consumer.commit(asynchronous=True)
                    except Exception:
                        pass
                    self._uncommitted_count = 0

                if self.metrics.consumed_count % 5000 == 0:
                    logger.info(
                        "Worker %d has consumed %d events...",
                        self.worker_id,
                        self.metrics.consumed_count,
                    )
                    out_fp.flush()

            # Final synchronous commit on worker shutdown
            try:
                self._consumer.commit(asynchronous=False)
            except Exception:
                pass

        # Clean close
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        self._is_running = False
        logger.info(
            "Worker %d finished. Total consumed: %d",
            self.worker_id,
            self.metrics.consumed_count,
        )

    def stop(self) -> None:
        """Signal consumer thread to stop."""
        self._stop_requested = True
