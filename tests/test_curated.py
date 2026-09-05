"""Deterministic tests for Milestone 3 Curated layer, Metrics, and Analytics:
- Curated is created ONLY from Conformed (isolation from raw/quarantine)
- Duplicate records do not reappear in Curated
- Curated build is strictly idempotent
- event_date partitioning is correctly applied
- MetricsReporter returns accurate, expected metrics on test fixtures
- CuratedAnalytics executes BI queries accurately
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import pytest
import duckdb

from src.curated.analytics import CuratedAnalytics
from src.curated.curated_builder import CuratedStorageBuilder
from src.curated.metrics import MetricsReporter
from src.processor.conformed_writer import ConformedStorageWriter


def _stage_sample_conformed(conformed_dir: Path, events: list) -> None:
    """Helper to stage test records in Conformed Parquet."""
    writer = ConformedStorageWriter(conformed_dir=conformed_dir)
    for event, is_late, proc_time in events:
        writer.write_event(event, is_late=is_late, processed_at=proc_time)
    writer.flush()
    writer.close()


def test_curated_created_only_from_conformed(tmp_path: Path):
    """Curated must read strictly from Conformed and ignore raw and quarantine directories."""
    raw_dir = tmp_path / "raw"
    quarantine_dir = tmp_path / "quarantine"
    conformed_dir = tmp_path / "conformed"
    curated_dir = tmp_path / "curated"

    raw_dir.mkdir()
    quarantine_dir.mkdir()

    # Raw has an extra record
    (raw_dir / "raw.jsonl").write_text('{"event_id": "raw-only", "extra": true}\n')
    # Quarantine has an invalid record
    (quarantine_dir / "quarantine.jsonl").write_text('{"raw_payload": "bad"}\n')

    # Conformed has 2 valid records
    proc_time = datetime(2026, 9, 5, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        (
            {
                "event_id": "c1111111-1111-1111-1111-111111111111",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T19:40:00.000000+00:00",
                "ingest_time": "2026-09-04T19:40:00.000000+00:00",
                "speed_kmh": 60.0,
                "engine_temp": 85.0,
            },
            False,
            proc_time,
        ),
        (
            {
                "event_id": "c2222222-2222-2222-2222-222222222222",
                "vehicle_id": "veh-002",
                "event_time": "2026-09-04T19:41:00.000000+00:00",
                "ingest_time": "2026-09-04T19:41:00.000000+00:00",
                "speed_kmh": 70.0,
                "engine_temp": 90.0,
            },
            True,
            proc_time,
        ),
    ]
    _stage_sample_conformed(conformed_dir, events)

    builder = CuratedStorageBuilder(conformed_dir=conformed_dir, curated_dir=curated_dir)
    count = builder.build()
    builder.close()

    assert count == 2
    # Check DuckDB content in curated
    con = duckdb.connect()
    rows = con.execute(f"SELECT event_id, vehicle_id, is_late FROM read_parquet('{curated_dir}/**/*.parquet')").fetchall()
    con.close()

    assert len(rows) == 2
    event_ids = {r[0] for r in rows}
    assert event_ids == {"c1111111-1111-1111-1111-111111111111", "c2222222-2222-2222-2222-222222222222"}
    assert "raw-only" not in event_ids


def test_curated_no_duplicates_reappear(tmp_path: Path):
    """Ensure Curated preserves uniqueness of event_id and contains no duplicates."""
    conformed_dir = tmp_path / "conformed"
    curated_dir = tmp_path / "curated"

    proc_time = datetime(2026, 9, 5, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        (
            {
                "event_id": "dup-check-1",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T19:40:00.000000+00:00",
                "ingest_time": "2026-09-04T19:40:00.000000+00:00",
                "speed_kmh": 60.0,
                "engine_temp": 85.0,
            },
            False,
            proc_time,
        ),
    ]
    _stage_sample_conformed(conformed_dir, events)

    builder = CuratedStorageBuilder(conformed_dir=conformed_dir, curated_dir=curated_dir)
    builder.build()
    builder.close()

    con = duckdb.connect()
    total = con.execute(f"SELECT count(*), count(distinct event_id) FROM read_parquet('{curated_dir}/**/*.parquet')").fetchone()
    con.close()

    assert total[0] == 1
    assert total[1] == 1


def test_curated_build_is_idempotent(tmp_path: Path):
    """Re-running CuratedStorageBuilder on the same conformed records must not increase record count."""
    conformed_dir = tmp_path / "conformed"
    curated_dir = tmp_path / "curated"

    proc_time = datetime(2026, 9, 5, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        (
            {
                "event_id": "idempotent-1",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T19:40:00.000000+00:00",
                "ingest_time": "2026-09-04T19:40:00.000000+00:00",
                "speed_kmh": 60.0,
                "engine_temp": 85.0,
            },
            False,
            proc_time,
        ),
        (
            {
                "event_id": "idempotent-2",
                "vehicle_id": "veh-002",
                "event_time": "2026-09-04T19:41:00.000000+00:00",
                "ingest_time": "2026-09-04T19:41:00.000000+00:00",
                "speed_kmh": 65.0,
                "engine_temp": 88.0,
            },
            False,
            proc_time,
        ),
    ]
    _stage_sample_conformed(conformed_dir, events)

    # First build
    builder1 = CuratedStorageBuilder(conformed_dir=conformed_dir, curated_dir=curated_dir)
    count1 = builder1.build()
    builder1.close()
    assert count1 == 2

    # Second build (re-run)
    builder2 = CuratedStorageBuilder(conformed_dir=conformed_dir, curated_dir=curated_dir)
    count2 = builder2.build()
    builder2.close()
    assert count2 == 2

    # Query Parquet directly to verify no duplicate records exist
    con = duckdb.connect()
    final_count = con.execute(f"SELECT count(*) FROM read_parquet('{curated_dir}/**/*.parquet')").fetchone()[0]
    con.close()
    assert final_count == 2


def test_event_date_partitioning(tmp_path: Path):
    """Curated layer must partition by event_date=YYYY-MM-DD based on event_time."""
    conformed_dir = tmp_path / "conformed"
    curated_dir = tmp_path / "curated"

    proc_time = datetime(2026, 9, 5, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        (
            {
                "event_id": "part-day1",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T23:55:00.000000+00:00",  # Day 1
                "ingest_time": "2026-09-04T23:55:00.000000+00:00",
                "speed_kmh": 50.0,
                "engine_temp": 80.0,
            },
            False,
            proc_time,
        ),
        (
            {
                "event_id": "part-day2",
                "vehicle_id": "veh-002",
                "event_time": "2026-09-05T00:05:00.000000+00:00",  # Day 2
                "ingest_time": "2026-09-05T00:05:00.000000+00:00",
                "speed_kmh": 75.0,
                "engine_temp": 92.0,
            },
            False,
            proc_time,
        ),
    ]
    _stage_sample_conformed(conformed_dir, events)

    builder = CuratedStorageBuilder(conformed_dir=conformed_dir, curated_dir=curated_dir)
    builder.build()
    builder.close()

    # Check directory structure
    partition_dirs = {p.name for p in curated_dir.iterdir() if p.is_dir()}
    assert "event_date=2026-09-04" in partition_dirs
    assert "event_date=2026-09-05" in partition_dirs

    # Query individual partitions via DuckDB
    con = duckdb.connect()
    day1_row = con.execute(f"SELECT event_id FROM read_parquet('{curated_dir}/event_date=2026-09-04/*.parquet')").fetchone()
    day2_row = con.execute(f"SELECT event_id FROM read_parquet('{curated_dir}/event_date=2026-09-05/*.parquet')").fetchone()
    con.close()

    assert day1_row[0] == "part-day1"
    assert day2_row[0] == "part-day2"


def test_metrics_calculation_accurate(tmp_path: Path):
    """MetricsReporter must compute counts, rates, and lag metrics accurately on test data."""
    raw_dir = tmp_path / "raw"
    quarantine_dir = tmp_path / "quarantine"
    conformed_dir = tmp_path / "conformed"
    curated_dir = tmp_path / "curated"

    raw_dir.mkdir()
    quarantine_dir.mkdir()

    # 5 raw events
    for i in range(5):
        (raw_dir / f"r_{i}.jsonl").write_text(f'{{"event_id": "id-{i}"}}\n')

    # 1 quarantine event
    (quarantine_dir / "q.jsonl").write_text('{"rejection_reason": "test"}\n')

    # 3 conformed events (1 is late)
    proc_time = datetime(2026, 9, 5, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        (
            {
                "event_id": "id-1",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T19:40:00.000000+00:00",
                "ingest_time": "2026-09-04T19:40:10.000000+00:00",
                "speed_kmh": 60.0,
                "engine_temp": 85.0,
            },
            False,
            proc_time,
        ),
        (
            {
                "event_id": "id-2",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T19:41:00.000000+00:00",
                "ingest_time": "2026-09-04T19:41:10.000000+00:00",
                "speed_kmh": 65.0,
                "engine_temp": 87.0,
            },
            True,  # late event
            proc_time,
        ),
        (
            {
                "event_id": "id-3",
                "vehicle_id": "veh-002",
                "event_time": "2026-09-04T19:42:00.000000+00:00",
                "ingest_time": "2026-09-04T19:42:10.000000+00:00",
                "speed_kmh": 70.0,
                "engine_temp": 90.0,
            },
            False,
            proc_time,
        ),
    ]
    _stage_sample_conformed(conformed_dir, events)

    # Build curated
    builder = CuratedStorageBuilder(conformed_dir=conformed_dir, curated_dir=curated_dir)
    builder.build()
    builder.close()

    # Calculate metrics
    reporter = MetricsReporter(
        raw_dir=raw_dir,
        conformed_dir=conformed_dir,
        quarantine_dir=quarantine_dir,
        curated_dir=curated_dir,
    )
    report = reporter.calculate_metrics()
    reporter.close()

    assert report.raw_count == 5
    assert report.quarantine_count == 1
    assert report.valid_count == 4
    assert report.conformed_count == 3
    assert report.curated_count == 3
    # 4 valid - 3 conformed = 1 duplicate
    assert report.duplicate_count == 1
    assert report.duplicate_rate_pct == 20.0  # 1/5
    assert report.quarantine_rate_pct == 20.0 # 1/5
    assert report.late_event_count == 1
    assert report.late_event_rate_pct == 33.33 # 1/3

    # Reprocessing delay should be positive
    assert report.batch_reprocessing_delay.avg_seconds > 0

    # Events by vehicle
    veh_map = {v["vehicle_id"]: v["event_count"] for v in report.events_by_vehicle}
    assert veh_map == {"veh-001": 2, "veh-002": 1}


def test_curated_analytics_queries(tmp_path: Path):
    """CuratedAnalytics must compute speed, temp, and late event aggregations correctly."""
    conformed_dir = tmp_path / "conformed"
    curated_dir = tmp_path / "curated"

    proc_time = datetime(2026, 9, 5, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        (
            {
                "event_id": "a1",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T19:40:00.000000+00:00",
                "ingest_time": "2026-09-04T19:40:00.000000+00:00",
                "speed_kmh": 80.0,
                "engine_temp": 90.0,
            },
            False,
            proc_time,
        ),
        (
            {
                "event_id": "a2",
                "vehicle_id": "veh-001",
                "event_time": "2026-09-04T19:41:00.000000+00:00",
                "ingest_time": "2026-09-04T19:41:00.000000+00:00",
                "speed_kmh": 100.0,
                "engine_temp": 100.0,
            },
            True,
            proc_time,
        ),
    ]
    _stage_sample_conformed(conformed_dir, events)

    builder = CuratedStorageBuilder(conformed_dir=conformed_dir, curated_dir=curated_dir)
    builder.build()
    builder.close()

    analytics = CuratedAnalytics(curated_dir=curated_dir)

    speeds = analytics.get_vehicle_speed_summary()
    assert len(speeds) == 1
    assert speeds[0]["vehicle_id"] == "veh-001"
    assert speeds[0]["avg_speed_kmh"] == 90.0
    assert speeds[0]["min_speed_kmh"] == 80.0
    assert speeds[0]["max_speed_kmh"] == 100.0

    temps = analytics.get_vehicle_temperature_summary()
    assert temps[0]["avg_engine_temp"] == 95.0

    lates = analytics.get_late_events_by_vehicle()
    assert lates[0]["total_events"] == 2
    assert lates[0]["late_events_count"] == 1
    assert lates[0]["late_event_pct"] == 50.0

    fleet = analytics.get_fleet_summary()
    assert fleet["total_vehicles"] == 1
    assert fleet["total_curated_events"] == 2
    assert fleet["fleet_total_late_events"] == 1

    analytics.close()
