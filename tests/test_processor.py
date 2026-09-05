"""Deterministic tests for Milestone 2 processor:
- Valid event -> Conformed
- Duplicate event_id -> only one Conformed record
- Invalid event -> Quarantine without stopping the pipeline
- Breaking schema change simulation -> Quarantined cleanly
- Late event -> correctly flagged with is_late=True and preserved (never dropped)
- Future-skewed event cannot advance the watermark
- Raw data is never modified (immutability)
- Re-running processor does not double-count Conformed data (idempotency)
"""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pytest

from src.processor.conformed_writer import ConformedStorageWriter
from src.processor.pipeline import ConformedPipeline
from src.processor.validator import EventValidator
from src.processor.watermark import WatermarkTracker


def _create_sample_event(
    event_id: str = "11111111-1111-1111-1111-111111111111",
    vehicle_id: str = "veh-001",
    event_time: str = "2026-09-04T19:43:00.000000+00:00",
    ingest_time: str = "2026-09-04T19:43:00.000000+00:00",
    speed_kmh: float = 65.5,
    engine_temp: float = 88.0,
) -> dict:
    return {
        "event_id": event_id,
        "vehicle_id": vehicle_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "speed_kmh": speed_kmh,
        "engine_temp": engine_temp,
    }


def test_valid_event_to_conformed(tmp_path: Path):
    """Valid event must be processed and persisted into Conformed Parquet."""
    raw_dir = tmp_path / "raw"
    conformed_dir = tmp_path / "conformed"
    quarantine_dir = tmp_path / "quarantine"
    raw_dir.mkdir()

    event = _create_sample_event()
    raw_file = raw_dir / "raw.jsonl"
    raw_file.write_text(json.dumps(event) + "\n", encoding="utf-8")

    pipeline = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
    )
    metrics = pipeline.process_raw_files()

    assert metrics.raw_count == 1
    assert metrics.valid_count == 1
    assert metrics.invalid_count == 0
    assert metrics.duplicate_count == 0
    assert metrics.unique_conformed_count == 1

    # Verify DuckDB reading
    writer = ConformedStorageWriter(conformed_dir=conformed_dir)
    try:
        rows = writer.query("SELECT event_id, vehicle_id, is_late FROM conformed_events")
        assert len(rows) == 1
        assert rows[0][0] == event["event_id"]
        assert rows[0][1] == "veh-001"
        assert rows[0][2] is False  # is_late is false
    finally:
        writer.close()

    # Quarantine must remain empty
    quarantine_files = list(quarantine_dir.glob("*.jsonl"))
    assert len(quarantine_files) == 0


def test_duplicate_event_deduplication(tmp_path: Path):
    """Raw preserves duplicates, but Conformed contains only one copy of each event_id."""
    raw_dir = tmp_path / "raw"
    conformed_dir = tmp_path / "conformed"
    quarantine_dir = tmp_path / "quarantine"
    raw_dir.mkdir()

    event1 = _create_sample_event(event_id="22222222-2222-2222-2222-222222222222")
    event2 = _create_sample_event(event_id="33333333-3333-3333-3333-333333333333")
    duplicate_event1 = event1.copy()

    raw_file = raw_dir / "raw.jsonl"
    with open(raw_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(event1) + "\n")
        f.write(json.dumps(event2) + "\n")
        f.write(json.dumps(duplicate_event1) + "\n")

    pipeline = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
    )
    metrics = pipeline.process_raw_files()

    assert metrics.raw_count == 3
    assert metrics.valid_count == 3
    assert metrics.duplicate_count == 1
    assert metrics.unique_conformed_count == 2

    # Verify only 2 distinct rows exist in conformed Parquet
    writer = ConformedStorageWriter(conformed_dir=conformed_dir)
    try:
        rows = writer.query("SELECT event_id FROM conformed_events ORDER BY event_id")
        assert len(rows) == 2
        assert rows[0][0] == "22222222-2222-2222-2222-222222222222"
        assert rows[1][0] == "33333333-3333-3333-3333-333333333333"
    finally:
        writer.close()


def test_invalid_event_quarantine_resilience(tmp_path: Path):
    """Invalid events must be routed to quarantine with rejection reason without halting pipeline."""
    raw_dir = tmp_path / "raw"
    conformed_dir = tmp_path / "conformed"
    quarantine_dir = tmp_path / "quarantine"
    raw_dir.mkdir()

    valid_event = _create_sample_event(event_id="44444444-4444-4444-4444-444444444444")
    invalid_type_event = {
        "event_id": "55555555-5555-5555-5555-555555555555",
        "vehicle_id": "veh-002",
        "event_time": "2026-09-04T19:43:00.000000+00:00",
        "ingest_time": "2026-09-04T19:43:00.000000+00:00",
        "speed_kmh": "not-a-number",  # wrong type
        "engine_temp": 90.0,
    }
    missing_field_event = {
        "event_id": "66666666-6666-6666-6666-666666666666",
        # missing vehicle_id
        "event_time": "2026-09-04T19:43:00.000000+00:00",
        "ingest_time": "2026-09-04T19:43:00.000000+00:00",
        "speed_kmh": 50.0,
        "engine_temp": 85.0,
    }
    invalid_date_event = {
        "event_id": "77777777-7777-7777-7777-777777777777",
        "vehicle_id": "veh-003",
        "event_time": "unparseable-date-stamp",
        "ingest_time": "2026-09-04T19:43:00.000000+00:00",
        "speed_kmh": 50.0,
        "engine_temp": 85.0,
    }

    raw_file = raw_dir / "raw.jsonl"
    with open(raw_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(valid_event) + "\n")
        f.write(json.dumps(invalid_type_event) + "\n")
        f.write(json.dumps(missing_field_event) + "\n")
        f.write(json.dumps(invalid_date_event) + "\n")

    pipeline = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
    )
    metrics = pipeline.process_raw_files()

    assert metrics.raw_count == 4
    assert metrics.valid_count == 1
    assert metrics.invalid_count == 3
    assert metrics.unique_conformed_count == 1

    # Check quarantine files
    quarantine_files = list(quarantine_dir.glob("*.jsonl"))
    assert len(quarantine_files) == 1
    with open(quarantine_files[0], "r", encoding="utf-8") as f:
        quarantined = [json.loads(line) for line in f if line.strip()]

    assert len(quarantined) == 3
    reasons = [q["rejection_reason"] for q in quarantined]
    assert any("speed_kmh" in r for r in reasons)
    assert any("vehicle_id" in r for r in reasons)
    assert any("event_time" in r for r in reasons)


def test_breaking_schema_change_simulation(tmp_path: Path):
    """Simulate a breaking schema change: malformed event must be quarantined without crashing pipeline."""
    raw_dir = tmp_path / "raw"
    conformed_dir = tmp_path / "conformed"
    quarantine_dir = tmp_path / "quarantine"
    raw_dir.mkdir()

    # Normal v1 event
    event_v1 = _create_sample_event(event_id="88888888-8888-8888-8888-888888888888")
    # Breaking v2 event with incompatible structure and unexpected schema version
    malformed_v2 = {
        "schema_version": "2.0-beta",
        "device_signature": "veh-999-telematics",
        "telemetry_readings": {"velocity": 110.2, "temperature_f": 195.0},
    }

    raw_file = raw_dir / "raw.jsonl"
    with open(raw_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(event_v1) + "\n")
        f.write(json.dumps(malformed_v2) + "\n")

    pipeline = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
    )
    metrics = pipeline.process_raw_files()

    assert metrics.raw_count == 2
    assert metrics.valid_count == 1
    assert metrics.invalid_count == 1
    assert metrics.unique_conformed_count == 1

    quarantine_files = list(quarantine_dir.glob("*.jsonl"))
    assert len(quarantine_files) == 1
    with open(quarantine_files[0], "r", encoding="utf-8") as f:
        entry = json.loads(f.readline())
    assert "event_id" in entry["rejection_reason"] or "required" in entry["rejection_reason"]


def test_late_event_flagging_and_preservation(tmp_path: Path):
    """Late events behind watermark must be flagged (is_late=True) and preserved in Conformed (not dropped)."""
    raw_dir = tmp_path / "raw"
    conformed_dir = tmp_path / "conformed"
    quarantine_dir = tmp_path / "quarantine"
    raw_dir.mkdir()

    # Event 1 establishes watermark:
    # event_time: 2026-09-04T19:40:00Z -> watermark = 19:40 - 15m = 19:25:00Z
    event1 = _create_sample_event(
        event_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        event_time="2026-09-04T19:40:00+00:00",
        ingest_time="2026-09-04T19:40:00+00:00",
    )
    # Event 2 is on time (19:30:00Z >= 19:25:00Z)
    event2 = _create_sample_event(
        event_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        event_time="2026-09-04T19:30:00+00:00",
        ingest_time="2026-09-04T19:40:05+00:00",
    )
    # Event 3 is late (19:20:00Z < 19:25:00Z)
    event3 = _create_sample_event(
        event_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
        event_time="2026-09-04T19:20:00+00:00",
        ingest_time="2026-09-04T19:40:10+00:00",
    )

    raw_file = raw_dir / "raw.jsonl"
    with open(raw_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(event1) + "\n")
        f.write(json.dumps(event2) + "\n")
        f.write(json.dumps(event3) + "\n")

    pipeline = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
        allowed_lateness_seconds=15 * 60,
    )
    metrics = pipeline.process_raw_files()

    assert metrics.raw_count == 3
    assert metrics.valid_count == 3
    assert metrics.late_event_count == 1
    # Critical requirement: Late event is NOT silently dropped; all 3 events are stored!
    assert metrics.unique_conformed_count == 3

    writer = ConformedStorageWriter(conformed_dir=conformed_dir)
    try:
        rows = dict(writer.query("SELECT event_id, is_late FROM conformed_events"))
        assert rows["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"] is False
        assert rows["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"] is False
        assert rows["cccccccc-cccc-cccc-cccc-cccccccccccc"] is True  # Preserved and flagged!
    finally:
        writer.close()


def test_future_skew_protection():
    """Future-skewed device timestamps cannot aggressively move the watermark forward."""
    tracker = WatermarkTracker(
        allowed_lateness_seconds=15 * 60,
        max_future_skew_seconds=12 * 60,
    )

    # Normal baseline event
    event1 = {
        "event_id": "e1",
        "event_time": "2026-09-04T19:40:00+00:00",
        "ingest_time": "2026-09-04T19:40:00+00:00",
    }
    is_late1, is_skew1, wm1 = tracker.process_event(event1)
    assert not is_late1
    assert not is_skew1
    assert wm1 == datetime.fromisoformat("2026-09-04T19:25:00+00:00")

    # Rogue device reporting timestamp 2 hours in the future
    event_rogue = {
        "event_id": "e_rogue",
        "event_time": "2026-09-04T21:40:00+00:00",  # +2 hours future skew
        "ingest_time": "2026-09-04T19:40:05+00:00",
    }
    is_late_rogue, is_skew_rogue, wm_rogue = tracker.process_event(event_rogue)
    assert is_skew_rogue is True
    # Watermark must NOT advance to 21:40 - 15m = 21:25!
    assert wm_rogue == datetime.fromisoformat("2026-09-04T19:25:00+00:00")

    # Subsequent normal event (19:41:00) is NOT marked as late because watermark wasn't corrupted
    event2 = {
        "event_id": "e2",
        "event_time": "2026-09-04T19:41:00+00:00",
        "ingest_time": "2026-09-04T19:41:00+00:00",
    }
    is_late2, is_skew2, wm2 = tracker.process_event(event2)
    assert not is_late2
    assert not is_skew2
    assert wm2 == datetime.fromisoformat("2026-09-04T19:26:00+00:00")


def test_raw_data_never_modified(tmp_path: Path):
    """Execution of Milestone 2 pipeline must never alter raw source files."""
    raw_dir = tmp_path / "raw"
    conformed_dir = tmp_path / "conformed"
    quarantine_dir = tmp_path / "quarantine"
    raw_dir.mkdir()

    event = _create_sample_event()
    raw_file = raw_dir / "telemetry.jsonl"
    content = (json.dumps(event) + "\n").encode("utf-8")
    raw_file.write_bytes(content)

    before_hash = hashlib.sha256(raw_file.read_bytes()).hexdigest()

    pipeline = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
    )
    pipeline.process_raw_files()

    after_hash = hashlib.sha256(raw_file.read_bytes()).hexdigest()
    assert before_hash == after_hash, "Raw file was modified during processing!"


def test_idempotent_re_run(tmp_path: Path):
    """Re-running the processor on identical raw data must not duplicate conformed records."""
    raw_dir = tmp_path / "raw"
    conformed_dir = tmp_path / "conformed"
    quarantine_dir = tmp_path / "quarantine"
    raw_dir.mkdir()

    event1 = _create_sample_event(event_id="dddddddd-dddd-dddd-dddd-dddddddddddd")
    event2 = _create_sample_event(event_id="eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")

    raw_file = raw_dir / "raw.jsonl"
    with open(raw_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(event1) + "\n")
        f.write(json.dumps(event2) + "\n")

    # Run 1
    pipeline1 = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
    )
    metrics1 = pipeline1.process_raw_files()
    assert metrics1.unique_conformed_count == 2

    # Run 2 (simulate repeated pipeline trigger)
    pipeline2 = ConformedPipeline(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
    )
    metrics2 = pipeline2.process_raw_files()
    assert metrics2.unique_conformed_count == 2

    # Verify DuckDB Parquet storage count directly
    writer = ConformedStorageWriter(conformed_dir=conformed_dir)
    try:
        assert writer.get_count() == 2, "Conformed count must remain exactly 2 without duplication"
    finally:
        writer.close()
