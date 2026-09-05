"""Fleet Telemetry Simulator.

Generates realistic telemetry events for a fleet of vehicles, intentionally
introducing:
- ~2% duplicate events (simulating network retransmissions / missing ACKs)
- Up to 11 minutes of device clock drift (simulating RTC drift)
"""

from collections import deque
from datetime import datetime, timezone, timedelta
import json
import logging
import random
import uuid
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

TOPIC_RAW = "fleet.telemetry.raw"


class TelemetrySimulator:
    """Simulates fleet sensor telemetry with realistic variations and intentional anomalies."""

    def __init__(
        self,
        vehicle_ids: Optional[List[str]] = None,
        duplicate_rate: float = 0.02,
        drift_rate: float = 0.10,
        max_drift_minutes: float = 11.0,
        buffer_size: int = 100,
    ) -> None:
        """Initialize simulator parameters.

        Args:
            vehicle_ids: List of vehicle identifiers.
            duplicate_rate: Probability of emitting a duplicate event (~0.02 = 2%).
            drift_rate: Probability of a device timestamp experiencing clock drift.
            max_drift_minutes: Maximum magnitude of clock drift in minutes (default 11.0).
            buffer_size: Number of recent events retained to draw duplicates from.
        """
        self.vehicle_ids = vehicle_ids or [f"veh-{i:03d}" for i in range(1, 11)]
        self.duplicate_rate = duplicate_rate
        self.drift_rate = drift_rate
        self.max_drift_seconds = max_drift_minutes * 60.0
        self.recent_events: deque = deque(maxlen=buffer_size)

        # Stateful vehicle kinematics for smooth realistic sensor values
        self.vehicle_states: Dict[str, Dict[str, float]] = {
            vid: {
                "speed_kmh": round(random.uniform(20.0, 80.0), 1),
                "engine_temp": round(random.uniform(85.0, 95.0), 1),
            }
            for vid in self.vehicle_ids
        }

    def _update_vehicle_state(self, vehicle_id: str) -> Dict[str, float]:
        """Perform a realistic random walk on speed and temperature."""
        state = self.vehicle_states[vehicle_id]

        # Speed fluctuation: accelerate/decelerate between 0 and 130 km/h
        delta_speed = random.gauss(0, 3.5)
        new_speed = max(0.0, min(130.0, state["speed_kmh"] + delta_speed))

        # Engine temperature correlates slightly with speed, centered around 90C
        target_temp = 85.0 + (new_speed / 130.0) * 15.0
        temp_adjustment = (target_temp - state["engine_temp"]) * 0.05 + random.gauss(0, 0.2)
        new_temp = max(65.0, min(115.0, state["engine_temp"] + temp_adjustment))

        state["speed_kmh"] = round(new_speed, 1)
        state["engine_temp"] = round(new_temp, 1)
        return state

    def generate_event(self) -> Dict[str, Any]:
        """Generate a single telemetry event.

        With probability `duplicate_rate`, re-emits an exact previous event.
        Otherwise generates a fresh event with potential device clock drift.
        """
        now = datetime.now(timezone.utc)

        # 1. Intentional duplicate generation (~2% probability)
        if self.recent_events and random.random() < self.duplicate_rate:
            duplicated_event = random.choice(list(self.recent_events)).copy()
            # Retain original event_id, vehicle_id, event_time, speed_kmh, engine_temp
            # Update ingest_time to reflect when this transmission hit the pipeline
            duplicated_event["ingest_time"] = now.isoformat()
            logger.debug("Emitting duplicate event: %s", duplicated_event["event_id"])
            return duplicated_event

        # 2. Pick vehicle and update state
        vehicle_id = random.choice(self.vehicle_ids)
        state = self._update_vehicle_state(vehicle_id)

        # 3. Device event timestamp with potential clock drift (up to 11 minutes)
        if random.random() < self.drift_rate:
            drift_seconds = random.uniform(-self.max_drift_seconds, self.max_drift_seconds)
            event_time = now + timedelta(seconds=drift_seconds)
        else:
            event_time = now

        event = {
            "event_id": str(uuid.uuid4()),
            "vehicle_id": vehicle_id,
            "event_time": event_time.isoformat(),
            "ingest_time": now.isoformat(),
            "speed_kmh": state["speed_kmh"],
            "engine_temp": state["engine_temp"],
        }

        self.recent_events.append(event)
        return event

    def generate_batch(self, count: int) -> List[Dict[str, Any]]:
        """Generate a batch of telemetry events."""
        return [self.generate_event() for _ in range(count)]
