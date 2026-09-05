"""Benchmark runner orchestrating isolated 1-consumer and multi-consumer load test runs.

Coordinates:
- Topic reset between runs to guarantee identical clean baseline state
- Traffic generation (baseline + 40x burst)
- Consumer worker lifecycle (1 vs 4 consumers)
- Latency percentile calculation (p50, p95, p99)
- Distinguishes processing backlog (true backpressure) from commit lag
- Accurately reconciles produced vs consumed vs missing
"""

from dataclasses import dataclass, field
import logging
from pathlib import Path
import time
from typing import Dict, List, Optional, Set

from src.loadtest.consumer_worker import ConsumerWorker, WorkerMetrics
from src.loadtest.lag_monitor import LagMonitor, LagReport
from src.loadtest.topic_manager import TopicManager
from src.loadtest.traffic_generator import ProducerResult, TrafficGenerator

logger = logging.getLogger(__name__)

DEFAULT_LOADTEST_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "loadtest"


def calculate_percentile(data: List[float], percentile: float) -> float:
    """Calculate the p-th percentile of a list of floats using linear interpolation."""
    if not data:
        return 0.0
    s = sorted(data)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * (percentile / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    d0 = s[f]
    d1 = s[c]
    return round(d0 + (k - f) * (d1 - d0), 2)


@dataclass
class LatencySummary:
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    avg_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0


@dataclass
class BenchmarkRunResult:
    run_name: str
    num_consumers: int
    num_partitions: int
    group_id: str
    producer_result: ProducerResult
    total_consumed: int
    missing_count: int
    consumer_throughput_eps: float
    max_processing_backlog: int
    max_commit_lag: int
    backlog_recovery_time_sec: float
    latency: LatencySummary = field(default_factory=LatencySummary)
    lag_report: LagReport = field(default_factory=LagReport)
    worker_counts: Dict[int, int] = field(default_factory=dict)


class BenchmarkRunner:
    """Orchestrates an isolated end-to-end benchmark execution."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "fleet.telemetry.loadtest",
        num_partitions: int = 6,
        base_output_dir: Path = DEFAULT_LOADTEST_DIR,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.num_partitions = num_partitions
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        self.topic_manager = TopicManager(
            bootstrap_servers=self.bootstrap_servers,
            topic_name=self.topic,
            num_partitions=self.num_partitions,
        )

    def run_benchmark(
        self,
        run_name: str,
        num_consumers: int,
        baseline_eps: float = 50.0,
        burst_eps: float = 2000.0,
        baseline_duration_sec: float = 15.0,
        burst_duration_sec: float = 15.0,
        max_drain_wait_sec: float = 30.0,
    ) -> BenchmarkRunResult:
        """Execute a clean benchmark run for the given consumer concurrency."""
        logger.info(
            "\n==================================================\n"
            "STARTING BENCHMARK: %s (%d Consumer%s, %d Partitions)\n"
            "==================================================",
            run_name,
            num_consumers,
            "s" if num_consumers > 1 else "",
            self.num_partitions,
        )

        run_output_dir = self.base_output_dir / run_name.lower().replace(" ", "_")
        run_output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Clean topic recreation so every run starts with zero offsets and equivalent state
        self.topic_manager.reset_topic()
        time.sleep(1.5)

        group_id = f"loadtest-group-{run_name.lower().replace(' ', '-')}-{int(time.time())}"

        # 2. Start Consumer Workers
        workers: List[ConsumerWorker] = []
        for wid in range(num_consumers):
            w = ConsumerWorker(
                worker_id=wid,
                group_id=group_id,
                output_dir=run_output_dir,
                bootstrap_servers=self.bootstrap_servers,
                topic=self.topic,
            )
            workers.append(w)
            w.start()

        # Allow consumer group coordinator to assign partitions
        logger.info("Waiting for consumer group rebalance and partition assignment...")
        time.sleep(2.5)

        # 3. Start Lag Monitor with worker position references
        lag_monitor = LagMonitor(
            group_id=group_id,
            workers=workers,
            topic=self.topic,
            num_partitions=self.num_partitions,
            bootstrap_servers=self.bootstrap_servers,
        )
        lag_monitor.start()

        # 4. Generate Traffic (Baseline -> 40x Burst)
        generator = TrafficGenerator(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic,
            baseline_eps=baseline_eps,
            burst_eps=burst_eps,
            baseline_duration_sec=baseline_duration_sec,
            burst_duration_sec=burst_duration_sec,
        )

        producer_result = generator.execute()
        lag_monitor.mark_burst_start(producer_result.burst_start_time)
        lag_monitor.mark_burst_end(producer_result.burst_end_time)

        # 5. Drain Phase: wait until consumers drain the processing backlog
        logger.info(
            "Traffic production completed (%d events). Awaiting consumer backlog drain...",
            producer_result.total_produced,
        )

        drain_start = time.time()
        while time.time() - drain_start < max_drain_wait_sec:
            current_consumed = sum(w.metrics.consumed_count for w in workers)
            snapshot = lag_monitor.sample_current_lag()

            # Drain is complete when processing backlog is 0 AND consumed equals produced
            if snapshot.processing_backlog == 0 and current_consumed >= producer_result.total_produced:
                logger.info(
                    "Backlog successfully drained! Consumed: %d / Produced: %d (Processing Backlog: 0, Commit Lag: %d)",
                    current_consumed,
                    producer_result.total_produced,
                    snapshot.commit_lag,
                )
                break

            time.sleep(0.2)

        # Allow brief commit completion
        time.sleep(0.5)

        # 6. Stop workers and lag monitor
        lag_monitor.stop()
        for w in workers:
            w.stop()
        for w in workers:
            w.join(timeout=3.0)

        lag_report = lag_monitor.get_report()

        # 7. Aggregate Latencies and Throughput
        all_latencies: List[float] = []
        all_seqs: Set[int] = set()
        worker_counts: Dict[int, int] = {}
        first_recv_times = []
        last_recv_times = []

        for w in workers:
            all_latencies.extend(w.metrics.latencies_ms)
            all_seqs.update(w.metrics.seq_numbers_seen)
            worker_counts[w.worker_id] = w.metrics.consumed_count
            if w.metrics.first_received_time:
                first_recv_times.append(w.metrics.first_received_time)
            if w.metrics.last_received_time:
                last_recv_times.append(w.metrics.last_received_time)

        total_consumed = sum(worker_counts.values())
        missing_count = max(0, producer_result.total_produced - total_consumed)

        # Calculate throughput over the exact active consumption window
        if first_recv_times and last_recv_times and max(last_recv_times) > min(first_recv_times):
            consumer_active_duration = max(last_recv_times) - min(first_recv_times)
        else:
            consumer_active_duration = 1.0

        consumer_throughput = round(total_consumed / consumer_active_duration, 1)

        # Latency calculations
        latency_summary = LatencySummary(
            p50_ms=calculate_percentile(all_latencies, 50),
            p95_ms=calculate_percentile(all_latencies, 95),
            p99_ms=calculate_percentile(all_latencies, 99),
            avg_ms=round(sum(all_latencies) / len(all_latencies), 2) if all_latencies else 0.0,
            min_ms=round(min(all_latencies), 2) if all_latencies else 0.0,
            max_ms=round(max(all_latencies), 2) if all_latencies else 0.0,
        )

        return BenchmarkRunResult(
            run_name=run_name,
            num_consumers=num_consumers,
            num_partitions=self.num_partitions,
            group_id=group_id,
            producer_result=producer_result,
            total_consumed=total_consumed,
            missing_count=missing_count,
            consumer_throughput_eps=consumer_throughput,
            max_processing_backlog=lag_report.max_processing_backlog,
            max_commit_lag=lag_report.max_commit_lag,
            backlog_recovery_time_sec=lag_report.recovery_time_sec,
            latency=latency_summary,
            lag_report=lag_report,
            worker_counts=worker_counts,
        )
