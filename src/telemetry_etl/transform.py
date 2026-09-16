# =====================================================================
# transform.py
# Job of this file: the "T" in ETL (Transform).
# It turns the clean events into 5 tables that are useful for reports,
# and saves each table as a Parquet file.
#
# The 5 tables:
#   1. telemetry_enriched      every event, plus a few helpful extra columns
#   2. vehicle_hourly_metrics  one row per car per hour (speed, battery, ...)
#   3. trip_metrics            one row per car per day, only while moving
#   4. battery_health          one row per car per day about the battery
#   5. alerts                  only the alert events
#
# Parquet is a file format made for tables. It keeps the column types
# (numbers stay numbers, dates stay dates) and Snowflake can load it directly.
# =====================================================================

# Path helps us work with file and folder paths.
from pathlib import Path

# pandas lets us work with data as a table (a "DataFrame"). "pd" is its usual short name.
import pandas as pd


# Put the battery percentage into a simple group that is easy to read in a report.
def battery_band(battery_soc: float) -> str:
    # 0 to 20 percent
    if battery_soc <= 20:
        return "critical"
    # above 20, up to 50 percent
    if battery_soc <= 50:
        return "low"
    # above 50, up to 80 percent
    if battery_soc <= 80:
        return "normal"
    # above 80 percent
    return "high"


# Build the 5 tables. The result is a dictionary: table name -> DataFrame.
def build_tables(records: list[dict]) -> dict[str, pd.DataFrame]:
    # Turn the list of events into a table. Each event becomes one row.
    events = pd.DataFrame(records)

    # --- Step 1: fix the types and add helpful columns -------------------

    # The times are text right now. Turn them into real date-time values in UTC.
    # format="ISO8601" accepts times with and without parts of a second, for example 08:00:00Z and 08:00:00.5Z.
    events["event_ts"] = pd.to_datetime(events["event_ts"], utc=True, format="ISO8601")
    events["ingest_ts"] = pd.to_datetime(events["ingest_ts"], utc=True, format="ISO8601")
    # The day of the event, for example 2026-06-06.
    events["event_date"] = events["event_ts"].dt.date
    # The hour of the event, for example 08:25 becomes 08:00.
    events["event_hour"] = events["event_ts"].dt.floor("h")
    # True when the car is moving (faster than 1 mile per hour).
    events["is_moving"] = events["speed_mph"] > 1
    # True when the charging state is "Charging" (we ignore upper and lower case).
    events["is_charging"] = events["charging_state"].str.lower() == "charging"
    # Battery group (critical, low, normal, high). apply runs battery_band on every row.
    events["battery_band"] = events["battery_soc"].apply(battery_band)
    # Sort the rows by car and by time, so the table is easy to read.
    events = events.sort_values(["vin", "event_ts"])

    # --- Step 2: table 2, one row per car per hour -----------------------

    # groupby puts rows with the same car and the same hour together.
    # as_index=False keeps vin and event_hour as normal columns in the result.
    # agg then calculates one value per group. The format is:
    #   new_column=("existing_column", "calculation")
    # Note: "sum" of a True/False column counts the True values (True = 1, False = 0).
    hourly = events.groupby(["vin", "event_hour"], as_index=False).agg(
        events=("event_id", "count"),
        avg_speed_mph=("speed_mph", "mean"),
        max_speed_mph=("speed_mph", "max"),
        avg_battery_soc=("battery_soc", "mean"),
        min_battery_soc=("battery_soc", "min"),
        max_battery_temp_c=("battery_temp_c", "max"),
        moving_events=("is_moving", "sum"),
        charging_events=("is_charging", "sum"),
        latest_odometer_miles=("odometer_miles", "max"),
    )

    # --- Step 3: table 3, one row per car per day, only moving events ----

    # Keep only the rows where the car was moving.
    moving = events[events["is_moving"]]
    # Group by car and day, and calculate the trip numbers.
    trips = moving.groupby(["vin", "event_date"], as_index=False).agg(
        first_event_ts=("event_ts", "min"),
        last_event_ts=("event_ts", "max"),
        start_odometer_miles=("odometer_miles", "min"),
        end_odometer_miles=("odometer_miles", "max"),
        avg_speed_mph=("speed_mph", "mean"),
        max_speed_mph=("speed_mph", "max"),
        autopilot_events=("autopilot_engaged", "sum"),
    )
    # Distance driven = odometer at the end minus odometer at the start.
    trips["distance_miles"] = trips["end_odometer_miles"] - trips["start_odometer_miles"]

    # --- Step 4: table 4, one row per car per day about the battery ------

    # Group by car and day, and calculate the battery numbers.
    battery = events.groupby(["vin", "event_date"], as_index=False).agg(
        avg_battery_soc=("battery_soc", "mean"),
        min_battery_soc=("battery_soc", "min"),
        avg_battery_temp_c=("battery_temp_c", "mean"),
        max_battery_temp_c=("battery_temp_c", "max"),
        charge_events=("is_charging", "sum"),
    )

    # --- Step 5: table 5, only the alert events --------------------------

    # Keep only the rows where event_type is "alert".
    alerts = events[events["event_type"] == "alert"]

    # Give back all 5 tables. The names match the Snowflake table names.
    return {
        "telemetry_enriched": events,
        "vehicle_hourly_metrics": hourly,
        "trip_metrics": trips,
        "battery_health": battery,
        "alerts": alerts,
    }


# Save each table as a Parquet file. The result is a dictionary: table name -> file path.
def save_tables_as_parquet(tables: dict[str, pd.DataFrame], folder: Path) -> dict[str, Path]:
    # Make sure the output folder exists.
    folder.mkdir(parents=True, exist_ok=True)
    # This dictionary will hold: table name -> Parquet file path.
    saved_files = {}
    # Save the tables one by one.
    for name, table in tables.items():
        # File name is the table name, for example trip_metrics.parquet
        file_path = folder / f"{name}.parquet"
        # Write the table. index=False means "do not save the pandas row numbers".
        table.to_parquet(file_path, index=False)
        # Remember where we saved it.
        saved_files[name] = file_path
    # Give back the list of saved files.
    return saved_files
