from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from telemetry_etl.quality import validate_jsonl_file


def build_curated_frames(records: list[dict]) -> dict[str, pd.DataFrame]:
    # Convert validated event dictionaries into a DataFrame for transformation.
    df = pd.DataFrame.from_records(records)
    if df.empty:
        # Return every expected output name even when the input batch is empty.
        return {
            "telemetry_enriched": df,
            "vehicle_hourly_metrics": df,
            "trip_metrics": df,
            "battery_health": df,
            "alerts": df,
        }

    # Convert timestamp strings to timezone-aware pandas timestamps.
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True)
    # Add time fields used for daily and hourly warehouse models.
    df["event_date"] = df["event_ts"].dt.date.astype(str)
    df["event_hour"] = df["event_ts"].dt.floor("h")
    # Add simple flags that make reporting queries easier.
    df["is_moving"] = df["speed_mph"] > 1
    df["is_charging"] = df["charging_state"].str.lower().eq("charging")
    # Bucket battery percentage into human-friendly monitoring bands.
    df["battery_band"] = pd.cut(
        df["battery_soc"],
        bins=[-1, 20, 50, 80, 101],
        labels=["critical", "low", "normal", "high"],
    ).astype(str)

    # Build hourly vehicle metrics for dashboards and monitoring.
    hourly = (
        df.groupby(["vin", "event_hour"], as_index=False)
        .agg(
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
        .sort_values(["vin", "event_hour"])
    )

    # Build daily trip metrics from only events where the vehicle was moving.
    trip_metrics = (
        df[df["is_moving"]]
        .groupby(["vin", "event_date"], as_index=False)
        .agg(
            first_event_ts=("event_ts", "min"),
            last_event_ts=("event_ts", "max"),
            start_odometer_miles=("odometer_miles", "min"),
            end_odometer_miles=("odometer_miles", "max"),
            avg_speed_mph=("speed_mph", "mean"),
            max_speed_mph=("speed_mph", "max"),
            autopilot_events=("autopilot_engaged", "sum"),
        )
    )
    if not trip_metrics.empty:
        # Distance is estimated from the odometer difference in the daily moving window.
        trip_metrics["distance_miles"] = (
            trip_metrics["end_odometer_miles"] - trip_metrics["start_odometer_miles"]
        )

    # Build daily battery health metrics per vehicle.
    battery_health = (
        df.groupby(["vin", "event_date"], as_index=False)
        .agg(
            avg_battery_soc=("battery_soc", "mean"),
            min_battery_soc=("battery_soc", "min"),
            avg_battery_temp_c=("battery_temp_c", "mean"),
            max_battery_temp_c=("battery_temp_c", "max"),
            charge_events=("is_charging", "sum"),
        )
        .sort_values(["vin", "event_date"])
    )

    # Keep alert rows separately so operations teams can monitor them directly.
    alerts = df[df["event_type"].eq("alert")].copy()

    # Return all curated datasets by the same names used in Snowflake.
    return {
        "telemetry_enriched": df.sort_values(["vin", "event_ts"]),
        "vehicle_hourly_metrics": hourly,
        "trip_metrics": trip_metrics,
        "battery_health": battery_health,
        "alerts": alerts,
    }


def write_curated_parquet(input_path: str | Path, output_dir: str | Path) -> dict[str, Path]:
    # Quality checks must pass before any curated files are written.
    result = validate_jsonl_file(input_path)
    if result.issues:
        issue_text = "; ".join(f"{issue.rule}:{issue.event_id}" for issue in result.issues)
        raise ValueError(f"Data quality failed: {issue_text}")

    # Create the output folder if it does not already exist.
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    # Transform valid raw records into curated DataFrames.
    frames = build_curated_frames(result.valid_records)
    written = {}
    for name, frame in frames.items():
        # Each curated DataFrame is written as one Parquet file.
        path = output / f"{name}.parquet"
        frame.to_parquet(path, index=False)
        written[name] = path
    return written


def main() -> None:
    # This command-line entry point runs the local sample ETL.
    parser = argparse.ArgumentParser(description="Run local telemetry transformations.")
    parser.add_argument("input_jsonl")
    parser.add_argument("output_dir")
    args = parser.parse_args()

    written = write_curated_parquet(args.input_jsonl, args.output_dir)
    for name, path in written.items():
        # Print output paths so the user can inspect generated files.
        print(f"{name}: {path}")


if __name__ == "__main__":
    # Run the CLI only when this file is executed as a module or script.
    main()
