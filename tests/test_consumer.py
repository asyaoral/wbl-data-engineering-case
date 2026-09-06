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


def test_raw_consumer_config_disables_auto_commit(monkeypatch):
    """Verify RawTelemetryConsumer disables Kafka auto-commit."""
    from unittest.mock import MagicMock
    import src.consumer.raw_consumer as rc

    captured_conf = {}

    def mock_consumer_init(conf):
        captured_conf.update(conf)
        mock = MagicMock()
        return mock

    monkeypatch.setattr(rc, "Consumer", mock_consumer_init)

    consumer = rc.RawTelemetryConsumer()
    _ = consumer._init_consumer()

    assert captured_conf.get("enable.auto.commit") is False, "Auto-commit must be disabled"


def test_raw_consumer_persists_before_commit(tmp_path: Path):
    """Verify persist-before-commit ordering: payload is written to storage before offset commit."""
    from unittest.mock import MagicMock
    from src.consumer.raw_consumer import RawTelemetryConsumer

    events_order = []

    mock_consumer = MagicMock()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'{"event_id": "order-test-1", "vehicle_id": "v1"}'
    mock_msg.offset.return_value = 101
    mock_msg.topic.return_value = "fleet.telemetry.raw"
    mock_msg.partition.return_value = 0

    # Return message on first poll, then None
    mock_consumer.poll.side_effect = [mock_msg, None]

    raw_consumer = RawTelemetryConsumer(raw_dir=tmp_path, consumer=mock_consumer)

    original_write = raw_consumer.writer.write_record

    def spy_write_record(payload):
        events_order.append("write_record")
        original_write(payload)

    raw_consumer.writer.write_record = spy_write_record

    def spy_commit(message=None, asynchronous=False):
        events_order.append("commit")

    mock_consumer.commit = spy_commit

    count = raw_consumer.run(max_messages=1, poll_timeout=0.1)

    assert count == 1
    assert events_order == ["write_record", "commit"], "Persistence must strictly precede offset commit"

    # Verify message persisted in raw file
    raw_files = list(tmp_path.glob("*.jsonl"))
    assert len(raw_files) == 1
    assert "order-test-1" in raw_files[0].read_text(encoding="utf-8")


def test_raw_consumer_failed_write_does_not_commit(tmp_path: Path):
    """Verify that if raw persistence fails, the Kafka offset is NOT committed."""
    from unittest.mock import MagicMock
    from src.consumer.raw_consumer import RawTelemetryConsumer

    mock_consumer = MagicMock()
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'{"event_id": "fail-test-1"}'
    mock_msg.offset.return_value = 202
    mock_msg.topic.return_value = "fleet.telemetry.raw"
    mock_msg.partition.return_value = 0

    # Return message on first poll, then None
    mock_consumer.poll.side_effect = [mock_msg, None]

    raw_consumer = RawTelemetryConsumer(raw_dir=tmp_path, consumer=mock_consumer)

    def failing_write_record(payload):
        raise OSError("Disk I/O error writing raw file")

    raw_consumer.writer.write_record = failing_write_record

    # Stop after 2 polls
    def stop_after_polls(timeout):
        if mock_consumer.poll.call_count >= 2:
            raw_consumer.is_running = False
        return mock_msg if mock_consumer.poll.call_count == 1 else None

    mock_consumer.poll.side_effect = stop_after_polls

    count = raw_consumer.run(max_messages=1, poll_timeout=0.1)

    assert count == 0, "Failed write must not increment consumed count"
    mock_consumer.commit.assert_not_called(), "Offset must NOT be committed when persistence fails"
