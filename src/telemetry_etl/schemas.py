# =====================================================================
# schemas.py
# Job of this file: describe what ONE telemetry event must look like.
#
# A car sends many small messages called events. Each event is one line
# in a JSONL file. This file lists every field an event must have, the
# type of each field, and the allowed range of values.
# Pydantic uses this description to check every event for us.
# =====================================================================

# Literal lets us say "only these exact words are allowed".
from typing import Literal

# BaseModel is the pydantic class we build our description on.
# ConfigDict holds extra rules for the whole event.
# Field lets us set limits such as "between 0 and 100".
# AwareDatetime is a date and time that must include a time zone.
# field_validator lets us write our own check for one field.
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


# Our description of one event. "(BaseModel)" means it gets all of pydantic's checking powers.
# Each line below is one field: name, then type, then (optional) limits.
class TelemetryEvent(BaseModel):
    # Reject events that have extra fields we do not know about.
    # This catches mistakes on the car side early.
    model_config = ConfigDict(extra="forbid")

    # Unique id of the event, at least 8 characters, for example evt_00000001
    event_id: str = Field(min_length=8)
    # Vehicle Identification Number: between 11 and 17 characters
    vin: str = Field(min_length=11, max_length=17)
    # When the event happened in the car (must include a time zone, for example Z for UTC)
    event_ts: AwareDatetime
    # When our system received the event (must include a time zone)
    ingest_ts: AwareDatetime
    # Type of event: only these three words are allowed
    event_type: Literal["telemetry", "charge", "alert"]
    # GPS position: latitude is between -90 and 90 (ge = greater or equal, le = less or equal)
    latitude: float = Field(ge=-90, le=90)
    # GPS position: longitude is between -180 and 180
    longitude: float = Field(ge=-180, le=180)
    # Speed in miles per hour: between 0 and 180
    speed_mph: float = Field(ge=0, le=180)
    # Battery state of charge in percent: between 0 and 100
    battery_soc: float = Field(ge=0, le=100)
    # Battery temperature in Celsius: between -40 and 90
    battery_temp_c: float = Field(ge=-40, le=90)
    # Total miles the car has driven: cannot be negative
    odometer_miles: float = Field(ge=0)
    # Charging state as text, for example Charging or Disconnected
    charging_state: str
    # Gear as text, for example P (park) or D (drive)
    gear: str
    # True if Autopilot was on, False if it was off
    autopilot_engaged: bool
    # Alert code, only filled for alert events. "str | None" means text or empty.
    alert_code: str | None = None
    # Software version running in the car
    software_version: str

    # Our own check for the vin field. Pydantic runs it every time it reads a vin.
    @field_validator("vin")
    # "@classmethod" is required by pydantic here. "cls" is the class itself (we do not use it).
    @classmethod
    def clean_vin(cls, value: str) -> str:
        # Remove spaces around the VIN and turn it into capital letters.
        cleaned = value.strip().upper()
        # A VIN must not have spaces inside it.
        if " " in cleaned:
            raise ValueError("VIN cannot contain spaces")
        # Give back the cleaned VIN. Pydantic stores this cleaned value.
        return cleaned
