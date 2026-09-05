"""Real-time Kafka consumer lag and backlog monitoring module.

Distinguishes:
1. Broker Log End Offset: The highest offset appended to the broker log.
2. Consumer Current Position: The offset of the record currently consumed by workers.
   - Processing Backlog = Broker Log End Offset - Consumer Current Position
   - Measures true unconsumed message queue depth (live backpressure).
3. Committed Offset: The offset saved in Kafka's offset coordinator.
   - Commit Lag = Broker Log End Offset - Committed Offset
   - Measures offset persistence delay due to asynchronous commit batching.
"""

from dataclasses import dataclass, field
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

from confluent_kafka import Consumer, TopicPartition

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "fleet.telemetry.loadtest"
DEFAULT_PARTITIONS = 6


@dataclass
class LagSnapshot:
    timestamp: float
    broker_log_end: int
    consumer_position: int
    committed_offset: int
    processing_backlog: int
    commit_lag: int
    partition_backlog: Dict[int, int] = field(default_factory=dict)


@dataclass
class LagReport:
    pre_burst_backlog: int = 0
    max_processing_backlog: int = 0
    post_burst_backlog: int = 0
    max_commit_lag: int = 0
    recovery_time_sec: float = 0.0
    samples: List[LagSnapshot] = field(default_factory=list)


class LagMonitor(threading.Thread):
    """Samples broker log end offsets, consumer current positions, and committed offsets."""

    def __init__(
        self,
        group_id: str,
        workers: list,
        topic: str = DEFAULT_TOPIC,
        num_partitions: int = DEFAULT_PARTITIONS,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        sample_interval_sec: float = 0.1,  # 100ms high-resolution sampling
    ) -> None:
        super().__init__(daemon=True, name="LagMonitor")
        self.group_id = group_id
        self.workers = workers
        self.topic = topic
        self.num_partitions = num_partitions
        self.bootstrap_servers = bootstrap_servers
        self.sample_interval_sec = sample_interval_sec

        self._stop_requested = False
        self.samples: List[LagSnapshot] = []
        self._admin_consumer: Optional[Consumer] = None
        self._lock = threading.Lock()

        self.burst_start_time: Optional[float] = None
        self.burst_end_time: Optional[float] = None
        self.drain_complete_time: Optional[float] = None

    def _init_consumer(self) -> Consumer:
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"lag-query-{self.group_id}-{time.time()}",
            "enable.auto.commit": False,
        }
        return Consumer(conf)

    def sample_current_lag(self) -> LagSnapshot:
        """Query broker log end offsets, consumer positions, and committed offsets."""
        if self._admin_consumer is None:
            self._admin_consumer = self._init_consumer()

        tps = [TopicPartition(self.topic, p) for p in range(self.num_partitions)]

        # 1. Broker log end offsets (high watermarks)
        broker_log_ends: Dict[int, int] = {}
        for tp in tps:
            try:
                low, high = self._admin_consumer.get_watermark_offsets(tp, timeout=1.0)
                broker_log_ends[tp.partition] = high
            except Exception:
                broker_log_ends[tp.partition] = 0

        # 2. Consumer current positions across active workers
        consumer_positions: Dict[int, int] = {}
        for w in self.workers:
            for p, pos in w.get_positions().items():
                consumer_positions[p] = max(consumer_positions.get(p, 0), pos)

        # 3. Committed offsets
        committed_offsets: Dict[int, int] = {}
        try:
            comm_list = self._admin_consumer.committed(tps, timeout=1.0)
            for tp in comm_list:
                committed_offsets[tp.partition] = tp.offset if tp.offset >= 0 else 0
        except Exception:
            pass

        # Aggregate metrics
        total_broker_end = sum(broker_log_ends.values())
        total_consumer_pos = sum(consumer_positions.get(p, 0) for p in range(self.num_partitions))
        total_committed = sum(committed_offsets.get(p, 0) for p in range(self.num_partitions))

        part_backlog: Dict[int, int] = {}
        total_backlog = 0
        for p in range(self.num_partitions):
            b_end = broker_log_ends.get(p, 0)
            c_pos = consumer_positions.get(p, 0)
            lag = max(0, b_end - c_pos)
            part_backlog[p] = lag
            total_backlog += lag

        total_commit_lag = max(0, total_broker_end - total_committed)

        return LagSnapshot(
            timestamp=time.time(),
            broker_log_end=total_broker_end,
            consumer_position=total_consumer_pos,
            committed_offset=total_committed,
            processing_backlog=total_backlog,
            commit_lag=total_commit_lag,
            partition_backlog=part_backlog,
        )

    def run(self) -> None:
        self._admin_consumer = self._init_consumer()
        while not self._stop_requested:
            snapshot = self.sample_current_lag()
            with self._lock:
                self.samples.append(snapshot)

            # Check if processing backlog drained after burst
            if (
                self.burst_end_time is not None
                and self.drain_complete_time is None
                and snapshot.processing_backlog == 0
                and snapshot.broker_log_end > 0
            ):
                self.drain_complete_time = snapshot.timestamp
                logger.info(
                    "LagMonitor: Processing backlog fully drained (backlog=0, broker_end=%d).",
                    snapshot.broker_log_end,
                )

            time.sleep(self.sample_interval_sec)

        if self._admin_consumer is not None:
            self._admin_consumer.close()
            self._admin_consumer = None

    def stop(self) -> None:
        self._stop_requested = True

    def mark_burst_start(self, timestamp: Optional[float] = None) -> None:
        self.burst_start_time = timestamp or time.time()

    def mark_burst_end(self, timestamp: Optional[float] = None) -> None:
        self.burst_end_time = timestamp or time.time()

    def get_report(self) -> LagReport:
        """Analyze sampled snapshots and compile lag report."""
        with self._lock:
            snapshots = list(self.samples)

        report = LagReport(samples=snapshots)
        if not snapshots:
            return report

        # Pre-burst backlog
        if self.burst_start_time is not None:
            pre_samples = [s for s in snapshots if s.timestamp <= self.burst_start_time]
            if pre_samples:
                report.pre_burst_backlog = pre_samples[-1].processing_backlog

        # Max processing backlog & max commit lag
        report.max_processing_backlog = max(s.processing_backlog for s in snapshots)
        report.max_commit_lag = max(s.commit_lag for s in snapshots)

        # Post-burst backlog (snapshot immediately following burst_end)
        if self.burst_end_time is not None:
            post_samples = [s for s in snapshots if s.timestamp >= self.burst_end_time]
            if post_samples:
                report.post_burst_backlog = post_samples[0].processing_backlog

        # Recovery time: duration from burst_end until processing_backlog == 0
        if self.burst_end_time is not None and self.drain_complete_time is not None:
            report.recovery_time_sec = max(
                0.0, round(self.drain_complete_time - self.burst_end_time, 2)
            )
        elif self.burst_end_time is not None:
            post_samples = [s for s in snapshots if s.timestamp >= self.burst_end_time]
            drained = [s for s in post_samples if s.processing_backlog == 0]
            if drained:
                report.recovery_time_sec = max(
                    0.0, round(drained[0].timestamp - self.burst_end_time, 2)
                )
            else:
                report.recovery_time_sec = 0.0

        return report
