"""Curated layer builder backed by DuckDB and partitioned Parquet.

Key Characteristics:
- Reads ONLY from Conformed Parquet (never raw or quarantine)
- Simple, analytics-ready schema:
    * vehicle_id: string
    * event_time: string (ISO 8601 UTC)
    * speed_kmh: float
    * engine_temp: float
    * is_late: boolean
    * Derived/Lineage:
        - event_date: string (YYYY-MM-DD partition key)
        - event_id: string (lineage key & primary key for idempotency)
        - ingest_time: string
        - processed_at: string
- Partitioned by event_date=YYYY-MM-DD for downstream analytics/BI query performance
- Strict idempotency: re-running does not duplicate records
"""

import logging
from pathlib import Path
from typing import Any, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)

DEFAULT_CONFORMED_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "conformed"
)
DEFAULT_CURATED_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "curated"
)


class CuratedStorageBuilder:
    """Transforms Conformed Parquet records into Curated Parquet partitioned by event_date."""

    def __init__(
        self,
        conformed_dir: Path = DEFAULT_CONFORMED_DIR,
        curated_dir: Path = DEFAULT_CURATED_DIR,
    ) -> None:
        self.conformed_dir = Path(conformed_dir)
        self.curated_dir = Path(curated_dir)
        self.curated_dir.mkdir(parents=True, exist_ok=True)

        self.con = duckdb.connect()
        self._init_tables()
        self._load_existing_curated()

    def _init_tables(self) -> None:
        """Initialize internal staging table for curated records."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS curated_events (
                event_id VARCHAR PRIMARY KEY,
                vehicle_id VARCHAR,
                event_time VARCHAR,
                ingest_time VARCHAR,
                speed_kmh DOUBLE,
                engine_temp DOUBLE,
                is_late BOOLEAN,
                processed_at VARCHAR,
                event_date VARCHAR
            )
        """)

    def _load_existing_curated(self) -> None:
        """Pre-load existing curated files to ensure cross-run idempotency."""
        existing_files = list(self.curated_dir.glob("**/*.parquet"))
        if existing_files:
            search_path = f"{self.curated_dir}/**/*.parquet"
            logger.info("Found existing curated Parquet files. Loading for idempotency: %s", search_path)
            try:
                self.con.execute(f"""
                    INSERT OR IGNORE INTO curated_events
                    SELECT 
                        event_id,
                        vehicle_id,
                        event_time,
                        ingest_time,
                        speed_kmh,
                        engine_temp,
                        is_late,
                        processed_at,
                        CAST(event_date AS VARCHAR) AS event_date
                    FROM read_parquet('{search_path}')
                """)
            except Exception as e:
                logger.warning("Could not pre-load existing Curated files (%s). Continuing fresh.", e)

    def build(self) -> int:
        """Read from Conformed Parquet, transform, and write to Curated Parquet.

        Returns:
            Total unique Curated records stored.
        """
        conformed_files = list(self.conformed_dir.glob("**/*.parquet"))
        if not conformed_files:
            logger.warning("No Conformed Parquet files found in %s", self.conformed_dir)
            return 0

        conformed_path = f"{self.conformed_dir}/**/*.parquet"
        logger.info("Reading Conformed Parquet from: %s", conformed_path)

        # Ingest and derive event_date from event_time
        self.con.execute(f"""
            INSERT OR REPLACE INTO curated_events
            SELECT 
                event_id,
                vehicle_id,
                event_time,
                ingest_time,
                speed_kmh,
                engine_temp,
                is_late,
                processed_at,
                strftime(timezone('UTC', event_time::TIMESTAMPTZ), '%Y-%m-%d') AS event_date
            FROM read_parquet('{conformed_path}')
        """)

        total_count = self.con.execute("SELECT count(*) FROM curated_events").fetchone()[0]

        # Write to Curated Parquet partitioned by event_date
        target_path = str(self.curated_dir)
        self.con.execute(f"""
            COPY (SELECT * FROM curated_events)
            TO '{target_path}'
            (FORMAT PARQUET, PARTITION_BY (event_date), OVERWRITE_OR_IGNORE 1)
        """)

        logger.info(
            "Successfully wrote %d curated records partitioned by event_date to %s",
            total_count,
            target_path,
        )
        return total_count

    def get_count(self) -> int:
        """Return total unique records currently in curated table."""
        return self.con.execute("SELECT count(*) FROM curated_events").fetchone()[0]

    def query(self, sql: str) -> List[Tuple[Any, ...]]:
        """Run an arbitrary query against the curated layer."""
        return self.con.execute(sql).fetchall()

    def close(self) -> None:
        """Close DuckDB connection."""
        self.con.close()
