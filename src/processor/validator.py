"""Validation module for telemetry events against formal JSON Schema.

Features:
- Validates records against schemas/telemetry_event.json
- Preserves raw record on validation failure
- Writes quarantined records with rejection reason to data/quarantine/
- Allows pipeline execution to continue uninterrupted
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import uuid

import jsonschema
from jsonschema import Draft202012Validator

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent.parent / "schemas" / "telemetry_event.json"
)
DEFAULT_QUARANTINE_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "quarantine"
)


def _build_format_checker() -> jsonschema.FormatChecker:
    """Build a format checker that validates date-time and uuid formats."""
    fc = jsonschema.FormatChecker()

    @fc.checks("date-time")
    def check_datetime(val: Any) -> bool:
        if not isinstance(val, str):
            return True
        try:
            # Handles ISO 8601 strings with Z or offsets
            datetime.fromisoformat(val.replace("Z", "+00:00"))
            return True
        except (ValueError, TypeError):
            return False

    @fc.checks("uuid")
    def check_uuid(val: Any) -> bool:
        if not isinstance(val, str):
            return True
        try:
            uuid.UUID(val)
            return True
        except (ValueError, TypeError):
            return False

    return fc


class EventValidator:
    """Validates raw event payloads against JSON schema and handles quarantine."""

    def __init__(
        self,
        schema_path: Path = DEFAULT_SCHEMA_PATH,
        quarantine_dir: Path = DEFAULT_QUARANTINE_DIR,
    ) -> None:
        self.schema_path = Path(schema_path)
        self.quarantine_dir = Path(quarantine_dir)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

        with open(self.schema_path, "r", encoding="utf-8") as f:
            self.schema = json.load(f)

        self._format_checker = _build_format_checker()
        self._validator = Draft202012Validator(
            self.schema, format_checker=self._format_checker
        )
        self._quarantined_payloads = self._load_existing_quarantined()

    def _load_existing_quarantined(self) -> set:
        seen = set()
        for q_file in self.quarantine_dir.glob("*.jsonl"):
            try:
                with open(q_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line_str = line.strip()
                        if line_str:
                            entry = json.loads(line_str)
                            if "raw_payload" in entry:
                                seen.add(entry["raw_payload"])
            except Exception:
                pass
        return seen

    def _get_quarantine_filepath(self) -> Path:
        """Partition quarantine files by current date."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self.quarantine_dir / f"telemetry_quarantine_{date_str}.jsonl"

    def quarantine_record(
        self, raw_payload: str, rejection_reason: str, error_details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Persist invalid record and its rejection reason to quarantine storage.

        Keeps the raw payload verbatim inside the quarantine metadata entry.
        Avoids duplicate entries if the record has already been quarantined.
        """
        if raw_payload in self._quarantined_payloads:
            return
        self._quarantined_payloads.add(raw_payload)

        quarantine_entry = {
            "quarantined_at": datetime.now(timezone.utc).isoformat(),
            "rejection_reason": rejection_reason,
            "error_details": error_details or {},
            "raw_payload": raw_payload,
        }
        target_file = self._get_quarantine_filepath()
        with open(target_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(quarantine_entry) + "\n")

    def validate(self, raw_record: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """Validate a raw record string.

        Returns:
            Tuple of (is_valid, parsed_dict, rejection_reason)
            If invalid, automatically writes to quarantine and returns (False, None, reason).
        """
        # 1. Parse JSON
        try:
            record_dict = json.loads(raw_record)
        except json.JSONDecodeError as exc:
            reason = f"Malformed JSON: {exc.msg} at line {exc.lineno} col {exc.colno}"
            logger.warning("Record failed JSON parsing: %s", reason)
            self.quarantine_record(raw_record, reason, {"error_type": "JSONDecodeError"})
            return False, None, reason

        if not isinstance(record_dict, dict):
            reason = f"Invalid root type: expected object, got {type(record_dict).__name__}"
            logger.warning("Record root is not a dictionary: %s", reason)
            self.quarantine_record(raw_record, reason, {"error_type": "TypeMismatch"})
            return False, None, reason

        # 2. Validate against schema
        errors = list(self._validator.iter_errors(record_dict))
        if errors:
            primary_error = errors[0]
            field = ".".join(str(p) for p in primary_error.path) if primary_error.path else "root"
            reason = f"Schema validation error on field '{field}': {primary_error.message}"
            details = {
                "validator": primary_error.validator,
                "validator_value": str(primary_error.validator_value),
                "field": field,
                "all_error_count": len(errors),
            }
            logger.warning("Record failed schema validation: %s", reason)
            self.quarantine_record(raw_record, reason, details)
            return False, None, reason

        return True, record_dict, None
