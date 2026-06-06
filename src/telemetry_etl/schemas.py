from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


# These are the only event types this ETL job accepts.
EventType = Literal["telemetry", "charge", "alert"]


class TelemetryEvent(BaseModel):
    # Reject unknown fields so bad producer payloads are caught early.
    model_config = ConfigDict(extra="forbid")

    # Required identifiers and timestamps for every telemetry event.
    event_id: str = Field(min_length=8)
    vin: str = Field(min_length=11, max_length=17)
    event_ts: datetime
    ingest_ts: datetime
    event_type: EventType
    # Range checks stop impossible location, speed, and battery values.
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    speed_mph: float = Field(ge=0, le=180)
    battery_soc: float = Field(ge=0, le=100)
    battery_temp_c: float = Field(ge=-40, le=90)
    odometer_miles: float = Field(ge=0)
    charging_state: str
    gear: str
    autopilot_engaged: bool
    alert_code: str | None = None
    software_version: str

    @field_validator("vin")
    @classmethod
    def vin_is_upper(cls, value: str) -> str:
        # Normalize VINs so the same vehicle does not appear under mixed casing.
        normalized = value.strip().upper()
        if " " in normalized:
            raise ValueError("VIN cannot contain spaces")
        return normalized
