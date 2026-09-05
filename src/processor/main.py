"""CLI entrypoint to execute Milestone 2 processing pipeline.

Usage:
    python -m src.processor.main
    python -m src.processor.main --raw-dir data/raw --conformed-dir data/conformed --quarantine-dir data/quarantine
"""

import argparse
import logging
from pathlib import Path
import sys

from src.processor.conformed_writer import DEFAULT_CONFORMED_DIR, ConformedStorageWriter
from src.processor.pipeline import ConformedPipeline, DEFAULT_RAW_DIR
from src.processor.validator import DEFAULT_QUARANTINE_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("src.processor.main")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Execute Milestone 2 Raw -> Conformed pipeline."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help=f"Directory containing raw JSONL files (default: {DEFAULT_RAW_DIR})",
    )
    parser.add_argument(
        "--conformed-dir",
        type=Path,
        default=DEFAULT_CONFORMED_DIR,
        help=f"Directory for Conformed Parquet files (default: {DEFAULT_CONFORMED_DIR})",
    )
    parser.add_argument(
        "--quarantine-dir",
        type=Path,
        default=DEFAULT_QUARANTINE_DIR,
        help=f"Directory for quarantined JSONL records (default: {DEFAULT_QUARANTINE_DIR})",
    )
    parser.add_argument(
        "--allowed-lateness-min",
        type=int,
        default=15,
        help="Allowed watermark lateness window in minutes (default: 15)",
    )
    parser.add_argument(
        "--max-future-skew-min",
        type=int,
        default=12,
        help="Max allowed device future clock skew in minutes (default: 12)",
    )

    args = parser.parse_args()

    pipeline = ConformedPipeline(
        raw_dir=args.raw_dir,
        conformed_dir=args.conformed_dir,
        quarantine_dir=args.quarantine_dir,
        allowed_lateness_seconds=args.allowed_lateness_min * 60,
        max_future_skew_seconds=args.max_future_skew_min * 60,
    )

    print("\n" + "=" * 60)
    print("      WBL DATA ENGINEERING PIPELINE - MILESTONE 2")
    print("=" * 60)
    print(f"Source Raw Directory:       {args.raw_dir}")
    print(f"Target Conformed Directory: {args.conformed_dir}")
    print(f"Target Quarantine Directory:{args.quarantine_dir}")
    print(f"Allowed Lateness Window:    {args.allowed_lateness_min} minutes")
    print(f"Max Future Skew Threshold:  {args.max_future_skew_min} minutes")
    print("-" * 60)

    metrics = pipeline.process_raw_files()

    print("\n" + "=" * 60)
    print("               PROCESSING RESULTS REPORT")
    print("=" * 60)
    print(f"  Raw count:               {metrics.raw_count}")
    print(f"  Valid count:             {metrics.valid_count}")
    print(f"  Quarantine count:        {metrics.invalid_count}")
    print(f"  Duplicate count:         {metrics.duplicate_count}")
    print(f"  Unique Conformed count:  {metrics.unique_conformed_count}")
    print(f"  Late-event count:        {metrics.late_event_count}")
    print(f"  Observed Watermark:      {metrics.observed_watermark}")
    print(f"  Conformed Parquet Path:  {metrics.conformed_dir}")
    print(f"  Quarantine JSONL Path:   {metrics.quarantine_dir}")
    print("=" * 60 + "\n")

    # DuckDB Verification sample
    writer = ConformedStorageWriter(conformed_dir=args.conformed_dir)
    try:
        sample_rows = writer.query("""
            SELECT event_id, vehicle_id, speed_kmh, engine_temp, is_late, processing_date
            FROM conformed_events
            LIMIT 5
        """)
        print("Sample Conformed Records from DuckDB:")
        for r in sample_rows:
            print(f"  {r}")
        print("-" * 60 + "\n")
    finally:
        writer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
