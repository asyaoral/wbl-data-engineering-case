"""Tests for raw consumer and storage handler."""

import json
from pathlib import Path
import pytest

from src.consumer.raw_consumer import RawStorageWriter
from src.producer.simulator import TelemetrySimulator


def test_raw_storage_writer_preserves_records(tmp_path: Path):
    """Verify that records are appended verbatim without filtering, deduplication, or schema checks."""
    writer = RawStorageWriter(raw_dir=tmp_path)
    simulator = TelemetrySimulator()

    # Generate events, including an intentional duplicate
    event1 = simulator.generate_event()
    event2 = simulator.generate_event()
    event3 = event1.copy()  # duplicate

    writer.write_event_dict(event1)
    writer.write_event_dict(event2)
    writer.write_event_dict(event3)

    raw_files = list(tmp_path.glob("*.jsonl"))
    assert len(raw_files) == 1, "Expected single partitioned raw jsonl file"

    with open(raw_files[0], "r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]

    assert len(lines) == 3, "All 3 records must be written to raw storage without deduplication"
    assert lines[0]["event_id"] == event1["event_id"]
    assert lines[1]["event_id"] == event2["event_id"]
    assert lines[2]["event_id"] == event1["event_id"], "Duplicate event_id must be preserved in raw"


def test_raw_storage_writer_handles_arbitrary_raw_text(tmp_path: Path):
    """Raw storage must preserve incoming raw text exactly as received."""
    writer = RawStorageWriter(raw_dir=tmp_path)
    raw_strings = [
        '{"event_id": "test-1", "corrupt_flag": true}',
        '{"invalid_json": true',
        'plain-text-payload-unparseable',
    ]

    for raw in raw_strings:
        writer.write_record(raw)

    raw_files = list(tmp_path.glob("*.jsonl"))
    assert len(raw_files) == 1

    with open(raw_files[0], "r", encoding="utf-8") as f:
        stored_lines = [line.strip() for line in f if line.strip()]

    assert stored_lines == raw_strings, "Raw records must be saved completely untouched"
