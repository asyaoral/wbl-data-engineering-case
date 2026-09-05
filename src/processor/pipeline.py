"""End-to-end Milestone 2 processing pipeline:
Raw -> Validation -> Deduplication -> Late-Event Handling -> Conformed Parquet.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import glob
import logging
from pathlib import Path
from typing import List, Optional

from src.processor.conformed_writer import ConformedStorageWriter, DEFAULT_CONFORMED_DIR
from src.processor.deduplicator import EventDeduplicator
from src.processor.validator import EventValidator, DEFAULT_QUARANTINE_DIR, DEFAULT_SCHEMA_PATH
from src.processor.watermark import WatermarkTracker

logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "raw"


@dataclass
class ProcessingMetrics:
    """Detailed summary metrics for pipeline execution."""
    raw_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    duplicate_count: int = 0
    unique_conformed_count: int = 0
    late_event_count: int = 0
    observed_watermark: Optional[str] = None
    conformed_dir: str = ""
    quarantine_dir: str = ""


class ConformedPipeline:
    """Coordinates validation, deduplication, watermarking, and conformed Parquet writes."""

    def __init__(
        self,
        raw_dir: Path = DEFAULT_RAW_DIR,
        conformed_dir: Path = DEFAULT_CONFORMED_DIR,
        quarantine_dir: Path = DEFAULT_QUARANTINE_DIR,
        schema_path: Path = DEFAULT_SCHEMA_PATH,
        allowed_lateness_seconds: int = 15 * 60,
        max_future_skew_seconds: int = 12 * 60,
        dedup_retention_seconds: int = 30 * 60,
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.conformed_dir = Path(conformed_dir)
        self.quarantine_dir = Path(quarantine_dir)

        self.validator = EventValidator(
            schema_path=schema_path, quarantine_dir=self.quarantine_dir
        )
        self.deduplicator = EventDeduplicator(retention_seconds=dedup_retention_seconds)
        self.watermark_tracker = WatermarkTracker(
            allowed_lateness_seconds=allowed_lateness_seconds,
            max_future_skew_seconds=max_future_skew_seconds,
        )
        self.conformed_writer = ConformedStorageWriter(
            conformed_dir=self.conformed_dir
        )

    def process_raw_files(
        self, raw_file_pattern: Optional[str] = None
    ) -> ProcessingMetrics:
        """Process all raw jsonl files matching pattern.

        Guarantees:
        - Raw data is never modified or deleted
        - Pipeline does not halt on malformed records
        - Duplicates are filtered from Conformed
        - Late events are preserved and flagged with is_late=True
        - Conformed Parquet files are written partitioned by processing_date
        """
        if raw_file_pattern is None:
            raw_files = sorted(self.raw_dir.glob("*.jsonl"))
        else:
            raw_files = sorted(Path(p) for p in glob.glob(raw_file_pattern))

        metrics = ProcessingMetrics(
            conformed_dir=str(self.conformed_dir),
            quarantine_dir=str(self.quarantine_dir),
        )

        logger.info("Found %d raw files to process in %s", len(raw_files), self.raw_dir)
        current_processing_time = datetime.now(timezone.utc)

        for raw_file in raw_files:
            logger.info("Processing raw file: %s", raw_file)
            with open(raw_file, "r", encoding="utf-8") as f:
                for line in f:
                    line_str = line.strip()
                    if not line_str:
                        continue

                    metrics.raw_count += 1

                    # 1. Validation Stage
                    is_valid, event_dict, _ = self.validator.validate(line_str)
                    if not is_valid or event_dict is None:
                        metrics.invalid_count += 1
                        # Invalid record written to quarantine; pipeline continues
                        continue

                    metrics.valid_count += 1
                    event_id = event_dict["event_id"]

                    # 2. Deduplication Stage
                    is_duplicate = self.deduplicator.check_and_register(
                        event_id, seen_at=current_processing_time
                    )
                    if is_duplicate:
                        metrics.duplicate_count += 1
                        # Raw keeps duplicates; Conformed keeps only first occurrence
                        continue

                    # 3. Watermarking & Late-Event Handling Stage
                    is_late, _, watermark = self.watermark_tracker.process_event(
                        event_dict
                    )
                    if is_late:
                        metrics.late_event_count += 1

                    # 4. Conformed Staging Stage (late events preserved with is_late=True)
                    self.conformed_writer.write_event(
                        event=event_dict,
                        is_late=is_late,
                        processed_at=current_processing_time,
                    )

        # 5. Flush staged conformed events to partitioned Parquet
        metrics.unique_conformed_count = self.conformed_writer.flush()

        if self.watermark_tracker.watermark is not None:
            metrics.observed_watermark = self.watermark_tracker.watermark.isoformat()

        logger.info(
            "Processing completed. Raw: %d, Valid: %d, Quarantine: %d, Duplicates: %d, "
            "Conformed: %d, Late: %d, Watermark: %s",
            metrics.raw_count,
            metrics.valid_count,
            metrics.invalid_count,
            metrics.duplicate_count,
            metrics.unique_conformed_count,
            metrics.late_event_count,
            metrics.observed_watermark,
        )

        return metrics
