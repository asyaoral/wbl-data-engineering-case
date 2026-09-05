"""High-throughput asynchronous traffic generator for load and burst testing.

Features:
- Configurable baseline (default: 50 msg/s for 15s)
- Instantaneous 40x burst (default: 2,000 msg/s for 15s)
- Non-blocking asynchronous production with high-resolution rate slicing
- Embeds monotonic sequence numbers and valid telemetry payloads
- Reports requested vs achieved rates transparently
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import random
import time
from typing import Callable, Optional
import uuid

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "fleet.telemetry.loadtest"


@dataclass
class ProducerResult:
    """Execution statistics for a traffic generation run."""
    baseline_requested_eps: float
    baseline_achieved_eps: float
    burst_requested_eps: float
    burst_achieved_eps: float
    baseline_events: int
    burst_events: int
    total_produced: int
    baseline_duration_sec: float
    burst_duration_sec: float
    burst_start_time: float
    burst_end_time: float


class TrafficGenerator:
    """Generates rate-controlled telemetry events to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        topic: str = DEFAULT_TOPIC,
        baseline_eps: float = 50.0,
        burst_eps: float = 2000.0,
        baseline_duration_sec: float = 15.0,
        burst_duration_sec: float = 15.0,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.baseline_eps = baseline_eps
        self.burst_eps = burst_eps
        self.baseline_duration_sec = baseline_duration_sec
        self.burst_duration_sec = burst_duration_sec

        self.producer = Producer({
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "fleet-loadtest-generator",
            "acks": 1,  # Fast acknowledgement for load testing
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 200000,
            "queue.buffering.max.kbytes": 2097152,
        })
        self._vehicles = [f"veh-{i:03d}" for i in range(1, 11)]
        self._seq = 0

    def _generate_event(self, seq_num: int) -> dict:
        """Create a valid telemetry payload with embedded sequence number."""
        now_iso = datetime.now(timezone.utc).isoformat()
        vehicle_id = random.choice(self._vehicles)
        return {
            "event_id": str(uuid.uuid4()),
            "vehicle_id": vehicle_id,
            "event_time": now_iso,
            "ingest_time": now_iso,
            "speed_kmh": round(random.uniform(20.0, 110.0), 1),
            "engine_temp": round(random.uniform(85.0, 95.0), 1),
            "seq_num": seq_num,
        }

    def _run_phase(
        self,
        target_eps: float,
        duration_sec: float,
        phase_name: str,
        on_event_sent: Optional[Callable[[int], None]] = None,
    ) -> tuple[int, float]:
        """Execute a rate-limited generation phase using high-resolution slicing."""
        slice_sec = 0.05  # 50ms intervals
        t_start = time.time()
        t_end = t_start + duration_sec
        sent = 0

        logger.info(
            "Starting %s phase: target %.1f eps for %.1fs...",
            phase_name,
            target_eps,
            duration_sec,
        )

        while time.time() < t_end:
            s_start = time.time()
            elapsed_overall = s_start - t_start
            target_cumulative = int((elapsed_overall + slice_sec) * target_eps)
            to_send = max(0, target_cumulative - sent)

            for _ in range(to_send):
                self._seq += 1
                event = self._generate_event(self._seq)
                payload = json.dumps(event).encode("utf-8")
                key = event["vehicle_id"].encode("utf-8")

                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=key,
                        value=payload,
                    )
                except BufferError:
                    self.producer.poll(0.01)
                    self.producer.produce(
                        topic=self.topic,
                        key=key,
                        value=payload,
                    )

                sent += 1
                if on_event_sent:
                    on_event_sent(self._seq)

            self.producer.poll(0)
            s_elapsed = time.time() - s_start
            rem = slice_sec - s_elapsed
            if rem > 0:
                time.sleep(rem)

        actual_duration = time.time() - t_start
        achieved_rate = sent / actual_duration if actual_duration > 0 else 0.0
        logger.info(
            "Completed %s phase: produced %d events in %.2fs (achieved %.1f eps).",
            phase_name,
            sent,
            actual_duration,
            achieved_rate,
        )
        return sent, actual_duration

    def execute(
        self,
        on_event_sent: Optional[Callable[[int], None]] = None,
    ) -> ProducerResult:
        """Run the full traffic profile: baseline -> burst -> flush."""
        logger.info("Starting TrafficGenerator profile on topic '%s'...", self.topic)

        # 1. Baseline Phase
        base_count, base_dur = self._run_phase(
            target_eps=self.baseline_eps,
            duration_sec=self.baseline_duration_sec,
            phase_name="BASELINE",
            on_event_sent=on_event_sent,
        )

        # 2. Instantaneous 40x Burst Phase
        burst_start = time.time()
        burst_count, burst_dur = self._run_phase(
            target_eps=self.burst_eps,
            duration_sec=self.burst_duration_sec,
            phase_name="40x BURST",
            on_event_sent=on_event_sent,
        )
        burst_end = time.time()

        # 3. Flush broker queue
        logger.info("Flushing producer buffer...")
        self.producer.flush(30.0)
        logger.info("Producer flush complete.")

        total_count = base_count + burst_count
        base_rate = base_count / base_dur if base_dur > 0 else 0.0
        burst_rate = burst_count / burst_dur if burst_dur > 0 else 0.0

        return ProducerResult(
            baseline_requested_eps=self.baseline_eps,
            baseline_achieved_eps=round(base_rate, 1),
            burst_requested_eps=self.burst_eps,
            burst_achieved_eps=round(burst_rate, 1),
            baseline_events=base_count,
            burst_events=burst_count,
            total_produced=total_count,
            baseline_duration_sec=round(base_dur, 2),
            burst_duration_sec=round(burst_dur, 2),
            burst_start_time=burst_start,
            burst_end_time=burst_end,
        )
