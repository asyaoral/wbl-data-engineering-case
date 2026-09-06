"""Conformed storage writer backed by DuckDB and partitioned Parquet files.

Requirements:
- Store conformed telemetry records in Apache Parquet format
- Partition by processing date (processing_date=YYYY-MM-DD), NOT by event time,
  to avoid scattered writes and small file fragmentation from late-arriving events
- Replay-safe record/cardinality idempotency: re-running on identical or overlapping records
  upserts by event_id primary key and does not double-count records. Metadata columns
  (e.g. processed_at) reflect the latest processing run timestamp, so output provides
  cardinality/business-key idempotency rather than byte-for-byte identical files.
- Enforce uniqueness on primary key: event_id
- Include metadata columns: is_late (boolean), processed_at (ISO timestamp), processing_date (partition)
"""

from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)

DEFAULT_CONFORMED_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "conformed"
)


class ConformedStorageWriter:
    """Manages writing cleaned and validated telemetry records to Conformed Parquet files."""

    def __init__(self, conformed_dir: Path = DEFAULT_CONFORMED_DIR) -> None:
        self.conformed_dir = Path(conformed_dir)
        self.conformed_dir.mkdir(parents=True, exist_ok=True)

        self.con = duckdb.connect()
        self._init_tables()
        self._load_existing_parquet()

    def _init_tables(self) -> None:
        """Create internal schema for staging conformed events."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS conformed_events (
                event_id VARCHAR PRIMARY KEY,
                vehicle_id VARCHAR,
                event_time VARCHAR,
                ingest_time VARCHAR,
                speed_kmh DOUBLE,
                engine_temp DOUBLE,
                is_late BOOLEAN,
                processed_at VARCHAR,
                processing_date VARCHAR
            )
        """)

    def _load_existing_parquet(self) -> None:
        """Load any pre-existing Parquet records to guarantee cross-run idempotency."""
        existing_parquet = list(self.conformed_dir.glob("**/*.parquet"))
        if existing_parquet:
            search_path = f"{self.conformed_dir}/**/*.parquet"
            logger.info("Found existing conformed Parquet files. Loading for idempotency: %s", search_path)
            try:
                self.con.execute(f"""
                    INSERT OR IGNORE INTO conformed_events
                    SELECT 
                        event_id,
                        vehicle_id,
                        event_time,
                        ingest_time,
                        speed_kmh,
                        engine_temp,
                        is_late,
                        processed_at,
                        CAST(processing_date AS VARCHAR) AS processing_date
                    FROM read_parquet('{search_path}')
                """)
            except Exception as e:
                logger.warning("Could not pre-load existing Parquet files (%s). Continuing fresh.", e)

    def write_event(
        self,
        event: Dict[str, Any],
        is_late: bool,
        processed_at: Optional[datetime] = None,
    ) -> None:
        """Stage a single conformed record."""
        if processed_at is None:
            processed_at = datetime.now(timezone.utc)

        processed_at_iso = processed_at.isoformat()
        processing_date = processed_at.strftime("%Y-%m-%d")

        self.con.execute(
            """
            INSERT OR REPLACE INTO conformed_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event["event_id"],
                event["vehicle_id"],
                event["event_time"],
                event["ingest_time"],
                float(event["speed_kmh"]),
                float(event["engine_temp"]),
                bool(is_late),
                processed_at_iso,
                processing_date,
            ],
        )

    def write_events_batch(
        self,
        events: List[Tuple[Dict[str, Any], bool, datetime]],
    ) -> int:
        """Stage a batch of conformed records.

        Each tuple contains (event_dict, is_late, processed_at).
        """
        if not events:
            return 0

        rows = []
        for event, is_late, processed_at in events:
            if processed_at is None:
                processed_at = datetime.now(timezone.utc)
            rows.append((
                event["event_id"],
                event["vehicle_id"],
                event["event_time"],
                event["ingest_time"],
                float(event["speed_kmh"]),
                float(event["engine_temp"]),
                bool(is_late),
                processed_at.isoformat(),
                processed_at.strftime("%Y-%m-%d"),
            ))

        self.con.executemany(
            """
            INSERT OR REPLACE INTO conformed_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)

    def flush(self) -> int:
        """Write staged records to Parquet partitioned by processing_date.

        Uses DuckDB's native partitioned Parquet export with overwrite protection.
        Returns total number of records written to Conformed storage.
        """
        count = self.con.execute("SELECT count(*) FROM conformed_events").fetchone()[0]
        if count == 0:
            logger.info("No records to write to conformed storage.")
            return 0

        # Partition by processing_date using Hive directory style
        target_path = str(self.conformed_dir)
        self.con.execute(f"""
            COPY (SELECT * FROM conformed_events) 
            TO '{target_path}' 
            (FORMAT PARQUET, PARTITION_BY (processing_date), OVERWRITE_OR_IGNORE 1)
        """)
        logger.info(
            "Flushed %d conformed records to partitioned Parquet at %s",
            count,
            target_path,
        )
        return count

    def get_count(self) -> int:
        """Return total unique records currently staged/stored."""
        return self.con.execute("SELECT count(*) FROM conformed_events").fetchone()[0]

    def query(self, sql: str) -> List[Tuple[Any, ...]]:
        """Run an arbitrary SQL query against the conformed table for verification."""
        return self.con.execute(sql).fetchall()

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.con.close()
