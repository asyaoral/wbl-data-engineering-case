"""Pipeline metrics reporting module using DuckDB.

Calculates and prints:
- Total raw event count, valid event count, unique conformed count
- Duplicate count & duplicate rate (%)
- Quarantine count & quarantine rate (%)
- Late-event count & late-event rate (%)
- Processing lag (processed_at - ingest_time): avg, p50, p95, p99
- Observed arrival offset (ingest_time - event_time): min, avg, p50, p95, p99, max
- Event distribution by vehicle

Critical Domain Distinction:
- Device Clock Skew vs Ingestion/Network Latency:
  In this prototype, devices exhibit simulated hardware clock drift of up to ±11 minutes.
  The delta between ingest_time and event_time is an *observed arrival offset* that reflects
  predominantly device clock drift rather than network latency. True network latency cannot
  be isolated from the current event schema without synchronized hardware clocks (e.g., GPS PPS).
- Batch Reprocessing Delay / Age at Processing (processed_at - ingest_time):
  Measures the elapsed duration between historical raw ingestion and batch processing execution.
  In this prototype run, records were ingested hours prior to processing, so this metric reflects
  the age of stored records at processing time, NOT live streaming pipeline latency.
  True processing/throughput latency will be measured during live load testing in Milestone 4.
"""

from dataclasses import dataclass, field
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "raw"
DEFAULT_CONFORMED_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "conformed"
DEFAULT_QUARANTINE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "quarantine"
DEFAULT_CURATED_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "curated"


@dataclass
class LagStats:
    avg_seconds: float = 0.0
    p50_seconds: float = 0.0
    p95_seconds: float = 0.0
    p99_seconds: float = 0.0
    min_seconds: float = 0.0
    max_seconds: float = 0.0


@dataclass
class PipelineMetricsReport:
    raw_count: int = 0
    valid_count: int = 0
    quarantine_count: int = 0
    conformed_count: int = 0
    curated_count: int = 0
    duplicate_count: int = 0
    duplicate_rate_pct: float = 0.0
    quarantine_rate_pct: float = 0.0
    late_event_count: int = 0
    late_event_rate_pct: float = 0.0
    batch_reprocessing_delay: LagStats = field(default_factory=LagStats)
    observed_arrival_offset: LagStats = field(default_factory=LagStats)
    events_by_vehicle: List[Dict[str, Any]] = field(default_factory=list)


class MetricsReporter:
    """Calculates and reports end-to-end pipeline metrics across storage layers."""

    def __init__(
        self,
        raw_dir: Path = DEFAULT_RAW_DIR,
        conformed_dir: Path = DEFAULT_CONFORMED_DIR,
        quarantine_dir: Path = DEFAULT_QUARANTINE_DIR,
        curated_dir: Path = DEFAULT_CURATED_DIR,
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.conformed_dir = Path(conformed_dir)
        self.quarantine_dir = Path(quarantine_dir)
        self.curated_dir = Path(curated_dir)
        self.con = duckdb.connect()

    def _get_raw_count(self) -> int:
        count = 0
        for f in self.raw_dir.glob("*.jsonl"):
            with open(f, "r", encoding="utf-8") as fp:
                for line in fp:
                    if line.strip():
                        count += 1
        return count

    def _get_quarantine_count(self) -> int:
        count = 0
        for f in self.quarantine_dir.glob("*.jsonl"):
            with open(f, "r", encoding="utf-8") as fp:
                for line in fp:
                    if line.strip():
                        count += 1
        return count

    def calculate_metrics(self) -> PipelineMetricsReport:
        """Query layers and calculate unified metrics report."""
        report = PipelineMetricsReport()

        report.raw_count = self._get_raw_count()
        report.quarantine_count = self._get_quarantine_count()
        report.valid_count = max(0, report.raw_count - report.quarantine_count)

        conformed_files = list(self.conformed_dir.glob("**/*.parquet"))
        if not conformed_files:
            return report

        conformed_path = f"{self.conformed_dir}/**/*.parquet"

        # Conformed counts and duplicates
        conformed_stats = self.con.execute(f"""
            SELECT 
                COUNT(*) AS total_conformed,
                COUNT(DISTINCT event_id) AS unique_conformed,
                SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_count
            FROM read_parquet('{conformed_path}')
        """).fetchone()

        report.conformed_count = conformed_stats[1] if conformed_stats else 0
        report.late_event_count = conformed_stats[2] if conformed_stats else 0

        # Curated count if present
        curated_files = list(self.curated_dir.glob("**/*.parquet"))
        if curated_files:
            curated_path = f"{self.curated_dir}/**/*.parquet"
            report.curated_count = self.con.execute(
                f"SELECT count(*) FROM read_parquet('{curated_path}')"
            ).fetchone()[0]

        # Duplicate calculation: valid events that did not make it to conformed because they were dups
        report.duplicate_count = max(0, report.valid_count - report.conformed_count)
        if report.raw_count > 0:
            report.duplicate_rate_pct = round(
                100.0 * report.duplicate_count / report.raw_count, 2
            )
            report.quarantine_rate_pct = round(
                100.0 * report.quarantine_count / report.raw_count, 2
            )

        if report.conformed_count > 0:
            report.late_event_rate_pct = round(
                100.0 * report.late_event_count / report.conformed_count, 2
            )

        # Batch reprocessing delay / age at processing: (epoch(processed_at) - epoch(ingest_time))
        # Note: Reflects historical storage age at processing run, NOT live streaming pipeline latency
        proc_lag_row = self.con.execute(f"""
            WITH deltas AS (
                SELECT 
                    epoch(processed_at::TIMESTAMPTZ) - epoch(ingest_time::TIMESTAMPTZ) AS delay_sec
                FROM read_parquet('{conformed_path}')
                WHERE processed_at IS NOT NULL AND ingest_time IS NOT NULL
            )
            SELECT 
                AVG(delay_sec),
                QUANTILE_CONT(delay_sec, 0.50),
                QUANTILE_CONT(delay_sec, 0.95),
                QUANTILE_CONT(delay_sec, 0.99),
                MIN(delay_sec),
                MAX(delay_sec)
            FROM deltas
        """).fetchone()

        if proc_lag_row and proc_lag_row[0] is not None:
            report.batch_reprocessing_delay = LagStats(
                avg_seconds=round(float(proc_lag_row[0]), 2),
                p50_seconds=round(float(proc_lag_row[1]), 2),
                p95_seconds=round(float(proc_lag_row[2]), 2),
                p99_seconds=round(float(proc_lag_row[3]), 2),
                min_seconds=round(float(proc_lag_row[4]), 2),
                max_seconds=round(float(proc_lag_row[5]), 2),
            )

        # Observed Arrival Offset: (epoch(ingest_time) - epoch(event_time))
        # Note: Conflates device clock skew (±11m) with network delay
        offset_row = self.con.execute(f"""
            WITH deltas AS (
                SELECT 
                    epoch(ingest_time::TIMESTAMPTZ) - epoch(event_time::TIMESTAMPTZ) AS offset_sec
                FROM read_parquet('{conformed_path}')
                WHERE ingest_time IS NOT NULL AND event_time IS NOT NULL
            )
            SELECT 
                AVG(offset_sec),
                QUANTILE_CONT(offset_sec, 0.50),
                QUANTILE_CONT(offset_sec, 0.95),
                QUANTILE_CONT(offset_sec, 0.99),
                MIN(offset_sec),
                MAX(offset_sec)
            FROM deltas
        """).fetchone()

        if offset_row and offset_row[0] is not None:
            report.observed_arrival_offset = LagStats(
                avg_seconds=round(float(offset_row[0]), 2),
                p50_seconds=round(float(offset_row[1]), 2),
                p95_seconds=round(float(offset_row[2]), 2),
                p99_seconds=round(float(offset_row[3]), 2),
                min_seconds=round(float(offset_row[4]), 2),
                max_seconds=round(float(offset_row[5]), 2),
            )

        # Events by vehicle
        vehicle_rows = self.con.execute(f"""
            SELECT 
                vehicle_id,
                COUNT(*) AS count
            FROM read_parquet('{conformed_path}')
            GROUP BY vehicle_id
            ORDER BY vehicle_id
        """).fetchall()

        report.events_by_vehicle = [
            {"vehicle_id": r[0], "event_count": r[1]} for r in vehicle_rows
        ]

        return report

    def print_report(self, report: PipelineMetricsReport) -> None:
        """Print formatted metrics report."""
        print("\n" + "=" * 65)
        print("          FLEET TELEMETRY PIPELINE - MILESTONE 3 METRICS")
        print("=" * 65)
        print(f"Total Raw Events:        {report.raw_count}")
        print(f"Valid Events:            {report.valid_count}")
        print(f"Quarantined Events:      {report.quarantine_count} (Quarantine Rate: {report.quarantine_rate_pct}%)")
        print(f"Duplicates Filtered:     {report.duplicate_count} (Duplicate Rate: {report.duplicate_rate_pct}%)")
        print(f"Conformed Output Events: {report.conformed_count}")
        print(f"Curated Output Events:   {report.curated_count}")
        print(f"Late Events Flagged:     {report.late_event_count} (Late Event Rate: {report.late_event_rate_pct}%)")
        print("-" * 65)

        print("\n--- Batch Reprocessing Delay / Age at Processing (processed_at - ingest_time) ---")
        print("    [NOTE: Dataset was stored historical data processed in batch mode.")
        print("     This reflects elapsed time since raw ingestion, NOT live pipeline throughput latency.")
        print("     True streaming latency will be evaluated during Milestone 4 live load testing.]")
        print(f"    Avg Delay: {report.batch_reprocessing_delay.avg_seconds:.2f}s "
              f"({report.batch_reprocessing_delay.avg_seconds / 60:.2f}m / {report.batch_reprocessing_delay.avg_seconds / 3600:.2f}h)")
        print(f"    p50: {report.batch_reprocessing_delay.p50_seconds:.2f}s | "
              f"p95: {report.batch_reprocessing_delay.p95_seconds:.2f}s | "
              f"p99: {report.batch_reprocessing_delay.p99_seconds:.2f}s")
        print(f"    Min: {report.batch_reprocessing_delay.min_seconds:.2f}s | "
              f"Max: {report.batch_reprocessing_delay.max_seconds:.2f}s")

        print("\n--- Observed Arrival Offset (ingest_time - event_time) ---")
        print("    [NOTE: Conflates device clock skew (±11m) with network delay.")
        print("     True network latency cannot be isolated without hardware NTP/GPS clocks.]")
        print(f"    Avg Offset: {report.observed_arrival_offset.avg_seconds:.2f}s "
              f"({report.observed_arrival_offset.avg_seconds / 60:.2f}m)")
        print(f"    p50: {report.observed_arrival_offset.p50_seconds:.2f}s | "
              f"p95: {report.observed_arrival_offset.p95_seconds:.2f}s | "
              f"p99: {report.observed_arrival_offset.p99_seconds:.2f}s")
        print(f"    Min Offset: {report.observed_arrival_offset.min_seconds:.2f}s "
              f"({report.observed_arrival_offset.min_seconds / 60:.2f}m) | "
              f"Max Offset: {report.observed_arrival_offset.max_seconds:.2f}s "
              f"({report.observed_arrival_offset.max_seconds / 60:.2f}m)")

        print("\n--- Event Distribution by Vehicle ---")
        for v in report.events_by_vehicle:
            print(f"    {v['vehicle_id']:<10} : {v['event_count']} events")
        print("=" * 65 + "\n")

    def close(self) -> None:
        self.con.close()
