"""Watermark and late-event tracking with future-skew protection.

Key Concepts:
- Clock Drift vs Network Latency:
  * Clock Drift: Physical skew between the vehicle sensor's onboard hardware clock
    and true UTC reference time (observed up to ±11 minutes in prototype simulation).
  * Network Latency: Elapsed transit duration between event dispatch at the vehicle
    and arrival at the ingestion pipeline (ingest_time - dispatch_time).
- Allowed Lateness Engineering Trade-off:
  * Baseline configuration: allowed_lateness = 15 minutes (900 seconds).
  * Scope: Balances completeness against finalization latency and state retention cost.
  * Note: While individual simulated device clocks drift up to ±11 minutes relative to UTC,
    the theoretical worst-case relative device-to-device skew across two opposing devices
    could reach up to ~22 minutes. A 15-minute window is an explicit operational trade-off:
    expanding to 25+ minutes would increase in-memory deduplication state retention and
    delay window finalization, whereas shortening it would increase late-event flags.
  * Late events (event_time < watermark) are NEVER dropped; they are flagged with
    is_late=True and preserved in storage for auditability and replayable reprocessing.
- Future-Skew Protection:
  * A device with an erroneous or maliciously advanced clock could emit timestamps far
    into the future. If untracked, this would aggressively advance the watermark and
    prematurely cause all subsequent valid events to be flagged as late.
  * Future-skew threshold: max_future_skew = 12 minutes beyond ingest_time.
  * Future-skewed timestamps are excluded from advancing the watermark.
- Late-Event Preservation:
  * Events with event_time < watermark are flagged with is_late = True.
  * Late events are NEVER dropped or deleted; they are preserved in Conformed storage
    for auditability and late-arriving reprocessing.
"""

from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_ALLOWED_LATENESS_SECONDS = 15 * 60  # 15 minutes
DEFAULT_MAX_FUTURE_SKEW_SECONDS = 12 * 60   # 12 minutes (safely above ±11m device drift)


class WatermarkTracker:
    """Manages pipeline watermark progression and event lateness classification."""

    def __init__(
        self,
        allowed_lateness_seconds: int = DEFAULT_ALLOWED_LATENESS_SECONDS,
        max_future_skew_seconds: int = DEFAULT_MAX_FUTURE_SKEW_SECONDS,
    ) -> None:
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.max_future_skew = timedelta(seconds=max_future_skew_seconds)
        self.max_trustworthy_event_time: Optional[datetime] = None
        self.watermark: Optional[datetime] = None

    def _parse_iso_datetime(self, dt_str: str) -> datetime:
        """Parse ISO datetime string, normalizing to UTC."""
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def process_event(
        self, event: Dict[str, Any]
    ) -> Tuple[bool, bool, Optional[datetime]]:
        """Evaluate event against watermark and update watermark progression.

        Args:
            event: Parsed event dictionary containing 'event_time' and 'ingest_time'.

        Returns:
            Tuple of (is_late, is_future_skew, current_watermark)
        """
        event_time = self._parse_iso_datetime(event["event_time"])
        ingest_time = self._parse_iso_datetime(event["ingest_time"])

        # 1. Future-skew guard:
        # Check if event_time is unreasonably far ahead of ingest_time
        # (exceeding known device clock drift bounds)
        is_future_skew = event_time > (ingest_time + self.max_future_skew)
        if is_future_skew:
            logger.warning(
                "Event %s has future skew (event_time=%s, ingest_time=%s, delta=%.1f min). "
                "Event timestamp will NOT advance watermark.",
                event.get("event_id"),
                event_time.isoformat(),
                ingest_time.isoformat(),
                (event_time - ingest_time).total_seconds() / 60.0,
            )

        # 2. Check lateness against current watermark BEFORE potentially updating watermark
        # If watermark is established and event_time is behind it, flag as late
        is_late = False
        if self.watermark is not None and event_time < self.watermark:
            is_late = True
            logger.info(
                "Event %s is late (event_time=%s < watermark=%s)",
                event.get("event_id"),
                event_time.isoformat(),
                self.watermark.isoformat(),
            )

        # 3. Update watermark using only trustworthy (non future-skewed) event times
        if not is_future_skew:
            if (
                self.max_trustworthy_event_time is None
                or event_time > self.max_trustworthy_event_time
            ):
                self.max_trustworthy_event_time = event_time
                candidate_watermark = self.max_trustworthy_event_time - self.allowed_lateness
                # Watermark must be monotonically non-decreasing
                if self.watermark is None or candidate_watermark > self.watermark:
                    self.watermark = candidate_watermark

        return is_late, is_future_skew, self.watermark
