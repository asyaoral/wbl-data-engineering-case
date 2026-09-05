"""Curated layer analytical query demonstrations using DuckDB.

Demonstrates BI query capabilities:
1. Average speed by vehicle
2. Average engine temperature by vehicle
3. Count of late events by vehicle
4. Fleet performance summary table
"""

import logging
from pathlib import Path
from typing import Any, Dict, List

import duckdb

logger = logging.getLogger(__name__)

DEFAULT_CURATED_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "curated"
)


class CuratedAnalytics:
    """Provides analytical and BI queries over Curated Parquet files."""

    def __init__(self, curated_dir: Path = DEFAULT_CURATED_DIR) -> None:
        self.curated_dir = Path(curated_dir)
        self.curated_path = f"{self.curated_dir}/**/*.parquet"
        self.con = duckdb.connect()

    def get_vehicle_speed_summary(self) -> List[Dict[str, Any]]:
        """Calculate average, min, and max speed by vehicle."""
        sql = f"""
            SELECT 
                vehicle_id,
                COUNT(*) AS event_count,
                ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
                ROUND(MIN(speed_kmh), 2) AS min_speed_kmh,
                ROUND(MAX(speed_kmh), 2) AS max_speed_kmh
            FROM read_parquet('{self.curated_path}')
            GROUP BY vehicle_id
            ORDER BY avg_speed_kmh DESC
        """
        rows = self.con.execute(sql).fetchall()
        cols = ["vehicle_id", "event_count", "avg_speed_kmh", "min_speed_kmh", "max_speed_kmh"]
        return [dict(zip(cols, r)) for r in rows]

    def get_vehicle_temperature_summary(self) -> List[Dict[str, Any]]:
        """Calculate average, min, and max engine temperature by vehicle."""
        sql = f"""
            SELECT 
                vehicle_id,
                ROUND(AVG(engine_temp), 2) AS avg_engine_temp,
                ROUND(MIN(engine_temp), 2) AS min_engine_temp,
                ROUND(MAX(engine_temp), 2) AS max_engine_temp
            FROM read_parquet('{self.curated_path}')
            GROUP BY vehicle_id
            ORDER BY avg_engine_temp DESC
        """
        rows = self.con.execute(sql).fetchall()
        cols = ["vehicle_id", "avg_engine_temp", "min_engine_temp", "max_engine_temp"]
        return [dict(zip(cols, r)) for r in rows]

    def get_late_events_by_vehicle(self) -> List[Dict[str, Any]]:
        """Calculate late event count and percentage by vehicle."""
        sql = f"""
            SELECT 
                vehicle_id,
                COUNT(*) AS total_events,
                SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_events_count,
                ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_event_pct
            FROM read_parquet('{self.curated_path}')
            GROUP BY vehicle_id
            ORDER BY late_events_count DESC, vehicle_id
        """
        rows = self.con.execute(sql).fetchall()
        cols = ["vehicle_id", "total_events", "late_events_count", "late_event_pct"]
        return [dict(zip(cols, r)) for r in rows]

    def get_fleet_summary(self) -> Dict[str, Any]:
        """Aggregate high-level fleet metrics."""
        sql = f"""
            SELECT 
                COUNT(DISTINCT vehicle_id) AS total_vehicles,
                COUNT(*) AS total_curated_events,
                ROUND(AVG(speed_kmh), 2) AS fleet_avg_speed,
                ROUND(AVG(engine_temp), 2) AS fleet_avg_temp,
                SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS fleet_total_late_events,
                COUNT(DISTINCT event_date) AS distinct_event_dates
            FROM read_parquet('{self.curated_path}')
        """
        row = self.con.execute(sql).fetchone()
        cols = [
            "total_vehicles",
            "total_curated_events",
            "fleet_avg_speed",
            "fleet_avg_temp",
            "fleet_total_late_events",
            "distinct_event_dates",
        ]
        return dict(zip(cols, row))

    def close(self) -> None:
        self.con.close()
