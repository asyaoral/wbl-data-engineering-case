"""Tests for TelemetrySimulator."""

from datetime import datetime, timezone
import uuid
import pytest

from src.producer.simulator import TelemetrySimulator


def test_event_structure():
    """Verify generated event has all required fields with expected types."""
    simulator = TelemetrySimulator()
    event = simulator.generate_event()

    assert "event_id" in event
    assert "vehicle_id" in event
    assert "event_time" in event
    assert "ingest_time" in event
    assert "speed_kmh" in event
    assert "engine_temp" in event

    # event_id is a valid UUID
    parsed_uuid = uuid.UUID(event["event_id"])
    assert str(parsed_uuid) == event["event_id"]

    # vehicle_id format
    assert event["vehicle_id"].startswith("veh-")

    # Timestamps are valid ISO 8601
    dt_event = datetime.fromisoformat(event["event_time"])
    dt_ingest = datetime.fromisoformat(event["ingest_time"])
    assert dt_event.tzinfo is not None
    assert dt_ingest.tzinfo is not None

    # Sensor measurements are numerical and within sane physical bounds
    assert 0.0 <= event["speed_kmh"] <= 200.0
    assert 40.0 <= event["engine_temp"] <= 150.0


def test_duplicate_event_generation():
    """Verify that duplicate events are generated with the specified probability."""
    # Force duplicate rate to 20% for test statistical reliability
    simulator = TelemetrySimulator(duplicate_rate=0.20, buffer_size=50)

    events = simulator.generate_batch(500)
    event_ids = [e["event_id"] for e in events]
    unique_ids = set(event_ids)

    # With 20% rate over 500 events, duplicates must exist
    duplicate_count = len(event_ids) - len(unique_ids)
    assert duplicate_count > 0, "Expected duplicate event_ids to be produced"

    # Default 2% simulator also produces duplicates over large sample
    sim_default = TelemetrySimulator(duplicate_rate=0.02, buffer_size=100)
    batch = sim_default.generate_batch(1000)
    default_ids = [e["event_id"] for e in batch]
    default_dupes = len(default_ids) - len(set(default_ids))
    # Roughly ~20 duplicates expected (binom test range: 5 to 45)
    assert 2 <= default_dupes <= 60


def test_clock_drift():
    """Verify that device timestamps show clock drift up to 11 minutes."""
    # Force drift rate to 100% with max 11 minutes
    max_drift_minutes = 11.0
    simulator = TelemetrySimulator(
        duplicate_rate=0.0,
        drift_rate=1.0,
        max_drift_minutes=max_drift_minutes,
    )

    batch = simulator.generate_batch(200)
    max_drift_observed = 0.0

    for event in batch:
        t_event = datetime.fromisoformat(event["event_time"])
        t_ingest = datetime.fromisoformat(event["ingest_time"])
        drift_seconds = abs((t_event - t_ingest).total_seconds())

        assert drift_seconds <= (max_drift_minutes * 60.0) + 1.0, (
            f"Clock drift {drift_seconds}s exceeds {max_drift_minutes} minutes"
        )
        if drift_seconds > max_drift_observed:
            max_drift_observed = drift_seconds

    # Over 200 samples with uniform [-11, 11] minutes, max drift should comfortably exceed 5 minutes
    assert max_drift_observed > 300.0, "Observed maximum clock drift was unexpectedly small"
