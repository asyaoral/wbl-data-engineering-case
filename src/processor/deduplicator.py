"""Deduplication module for telemetry events.

Requirements:
- Deduplication key: event_id
- Raw layer preserves all records (including duplicates)
- Conformed layer receives exactly one record per event_id
- Deduplication state retained for at least 20-30 minutes to cover lateness window
- Idempotent processing
"""

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_SECONDS = 30 * 60  # 30 minutes (covers 15m lateness + buffer)


class EventDeduplicator:
    """Stateful deduplicator tracking event_ids within a retention time window."""

    def __init__(
        self,
        retention_seconds: int = DEFAULT_RETENTION_SECONDS,
        auto_prune_interval: int = 0,
    ) -> None:
        self.retention_window = timedelta(seconds=retention_seconds)
        self.auto_prune_interval = auto_prune_interval
        self._op_count = 0
        # Maps event_id -> arrival timestamp (for TTL cleanup)
        self._seen_events: Dict[str, datetime] = {}

    def is_duplicate(self, event_id: str) -> bool:
        """Check if event_id has already been observed."""
        return event_id in self._seen_events

    def register(self, event_id: str, seen_at: Optional[datetime] = None) -> None:
        """Register a new event_id in the deduplication state."""
        if seen_at is None:
            seen_at = datetime.now(timezone.utc)
        self._seen_events[event_id] = seen_at

    def check_and_register(
        self, event_id: str, seen_at: Optional[datetime] = None
    ) -> bool:
        """Check if event_id is a duplicate; if not, register it.

        Returns:
            True if event is a DUPLICATE (already seen)
            False if event is UNIQUE (newly registered)
        """
        if self.auto_prune_interval > 0:
            self._op_count += 1
            if self._op_count % self.auto_prune_interval == 0:
                self.prune_expired(seen_at)

        if self.is_duplicate(event_id):
            logger.debug("Duplicate event_id detected: %s", event_id)
            return True

        self.register(event_id, seen_at)
        return False

    def prune_expired(self, current_time: Optional[datetime] = None) -> int:
        """Prune event_ids older than the retention window.

        Returns:
            Number of pruned records.
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        cutoff = current_time - self.retention_window
        expired_keys = [k for k, ts in self._seen_events.items() if ts < cutoff]
        for k in expired_keys:
            del self._seen_events[k]

        if expired_keys:
            logger.debug("Pruned %d expired deduplication keys", len(expired_keys))
        return len(expired_keys)

    def count(self) -> int:
        """Current number of distinct event_ids tracked in memory."""
        return len(self._seen_events)

    def clear(self) -> None:
        """Clear all deduplication state."""
        self._seen_events.clear()
