from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from pydantic import ValidationError

from telemetry_etl.schemas import TelemetryEvent


@dataclass(frozen=True)
class QualityIssue:
    # One failed rule is stored as one quality issue.
    rule: str
    event_id: str | None
    message: str


@dataclass(frozen=True)
class QualityResult:
    # Valid records move forward; issues explain what failed and why.
    valid_records: list[dict]
    issues: list[QualityIssue]

    @property
    def passed(self) -> bool:
        # The batch passes only when no quality issues were found.
        return not self.issues


def read_jsonl(path: str | Path) -> list[dict]:
    # Read newline-delimited JSON where each line is one vehicle event.
    records = []
    with Path(path).open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    # Keep parse failures as records so validation can report them later.
                    records.append({"_invalid_json": str(exc), "_line_number": line_number})
    return records


def validate_records(records: Iterable[dict]) -> QualityResult:
    # Collect clean records and failed rule details in the same pass.
    valid_records: list[dict] = []
    issues: list[QualityIssue] = []
    event_ids: list[str] = []

    for raw in records:
        if "_invalid_json" in raw:
            # Invalid JSON cannot be checked against the schema.
            issues.append(QualityIssue("json_parse", None, raw["_invalid_json"]))
            continue

        event_id = raw.get("event_id")
        try:
            # Pydantic checks required fields, data types, allowed values, and ranges.
            event = TelemetryEvent.model_validate(raw)
        except ValidationError as exc:
            issues.append(QualityIssue("schema", event_id, exc.errors()[0]["msg"]))
            continue

        # Convert timestamps and other values into JSON-friendly Python types.
        record = event.model_dump(mode="json")
        valid_records.append(record)
        event_ids.append(record["event_id"])

        # Business rule: an event cannot happen after it was ingested.
        if record["event_ts"] > record["ingest_ts"]:
            issues.append(QualityIssue("timestamp_order", event_id, "event_ts is after ingest_ts"))
        # Business rule: a moving vehicle should have a real odometer reading.
        if record["odometer_miles"] == 0 and record["speed_mph"] > 0:
            issues.append(QualityIssue("odometer_speed", event_id, "moving vehicle has zero odometer"))
        # Business rule: alert events must explain which alert happened.
        if record["event_type"] == "alert" and not record["alert_code"]:
            issues.append(QualityIssue("alert_code", event_id, "alert event requires alert_code"))

    # Duplicate event IDs would cause incorrect warehouse merges, so flag them.
    duplicate_ids = [event_id for event_id, count in Counter(event_ids).items() if count > 1]
    for event_id in duplicate_ids:
        issues.append(QualityIssue("duplicate_event", event_id, "duplicate event_id in batch"))

    return QualityResult(valid_records=valid_records, issues=issues)


def validate_jsonl_file(path: str | Path) -> QualityResult:
    # Convenience helper for the common flow: read a file, then validate it.
    return validate_records(read_jsonl(path))
