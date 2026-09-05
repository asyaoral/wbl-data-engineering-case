"""CLI entrypoint for Milestone 3:
1. Builds Curated layer partitioned by event_date
2. Computes and displays end-to-end pipeline metrics
3. Executes and prints BI analytics queries
"""

import argparse
import logging
from pathlib import Path
import sys

from src.curated.analytics import CuratedAnalytics
from src.curated.curated_builder import (
    CuratedStorageBuilder,
    DEFAULT_CONFORMED_DIR,
    DEFAULT_CURATED_DIR,
)
from src.curated.metrics import (
    DEFAULT_QUARANTINE_DIR,
    DEFAULT_RAW_DIR,
    MetricsReporter,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("src.curated.main")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Execute Milestone 3: Curated layer build, pipeline metrics, and BI analytics."
    )
    parser.add_argument(
        "--conformed-dir",
        type=Path,
        default=DEFAULT_CONFORMED_DIR,
        help="Path to Conformed Parquet directory",
    )
    parser.add_argument(
        "--curated-dir",
        type=Path,
        default=DEFAULT_CURATED_DIR,
        help="Path to Curated Parquet directory",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help="Path to Raw JSONL directory",
    )
    parser.add_argument(
        "--quarantine-dir",
        type=Path,
        default=DEFAULT_QUARANTINE_DIR,
        help="Path to Quarantine JSONL directory",
    )

    args = parser.parse_args()

    print("\n" + "=" * 65)
    print("      WBL DATA ENGINEERING PIPELINE - MILESTONE 3 BUILD")
    print("=" * 65)

    # 1. Build Curated Layer
    builder = CuratedStorageBuilder(
        conformed_dir=args.conformed_dir,
        curated_dir=args.curated_dir,
    )
    curated_count = builder.build()
    builder.close()

    print(f"\nCurated Layer Build Completed:")
    print(f"  Source: Conformed Parquet at {args.conformed_dir}")
    print(f"  Target: Curated Parquet at   {args.curated_dir}")
    print(f"  Total Curated Records:       {curated_count}")
    print(f"  Partitioning Scheme:         event_date=YYYY-MM-DD")

    # 2. Pipeline Metrics Report
    reporter = MetricsReporter(
        raw_dir=args.raw_dir,
        conformed_dir=args.conformed_dir,
        quarantine_dir=args.quarantine_dir,
        curated_dir=args.curated_dir,
    )
    metrics_report = reporter.calculate_metrics()
    reporter.print_report(metrics_report)
    reporter.close()

    # 3. BI Analytics Demonstration
    analytics = CuratedAnalytics(curated_dir=args.curated_dir)

    print("-" * 65)
    print("              CURATED BI ANALYTICS DEMONSTRATIONS")
    print("-" * 65)

    print("\n[BI Query 1] Average Speed by Vehicle:")
    print(f"{'Vehicle ID':<12} {'Events':<8} {'Avg Speed':<12} {'Min Speed':<12} {'Max Speed':<12}")
    print("-" * 56)
    for row in analytics.get_vehicle_speed_summary():
        print(f"{row['vehicle_id']:<12} {row['event_count']:<8} {row['avg_speed_kmh']:<12.1f} {row['min_speed_kmh']:<12.1f} {row['max_speed_kmh']:<12.1f}")

    print("\n[BI Query 2] Engine Coolant Temperature by Vehicle:")
    print(f"{'Vehicle ID':<12} {'Avg Temp (°C)':<15} {'Min Temp (°C)':<15} {'Max Temp (°C)':<15}")
    print("-" * 57)
    for row in analytics.get_vehicle_temperature_summary():
        print(f"{row['vehicle_id']:<12} {row['avg_engine_temp']:<15.1f} {row['min_engine_temp']:<15.1f} {row['max_engine_temp']:<15.1f}")

    print("\n[BI Query 3] Late-Arriving Events by Vehicle:")
    print(f"{'Vehicle ID':<12} {'Total Events':<14} {'Late Events':<14} {'Late Event %':<12}")
    print("-" * 52)
    for row in analytics.get_late_events_by_vehicle():
        print(f"{row['vehicle_id']:<12} {row['total_events']:<14} {row['late_events_count']:<14} {row['late_event_pct']:<12.2f}%")

    fleet = analytics.get_fleet_summary()
    print("\n[BI Query 4] Overall Fleet Overview:")
    print(f"  Total Monitored Vehicles: {fleet['total_vehicles']}")
    print(f"  Total Curated Events:     {fleet['total_curated_events']}")
    print(f"  Fleet Average Speed:      {fleet['fleet_avg_speed']} km/h")
    print(f"  Fleet Average Engine Temp:{fleet['fleet_avg_temp']} °C")
    print(f"  Fleet Total Late Events:  {fleet['fleet_total_late_events']}")
    print(f"  Distinct Event Dates:     {fleet['distinct_event_dates']}")
    print("=" * 65 + "\n")

    analytics.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
