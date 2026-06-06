#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

from telemetry_etl.transform import write_curated_parquet


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_INPUT = PROJECT_ROOT / "data" / "sample" / "tesla_telemetry_sample.jsonl"
CURATED_DIR = PROJECT_ROOT / "build" / "curated"
SNOWFLAKE_SQL_FILES = [
    PROJECT_ROOT / "snowflake" / "001_create_database_schema.sql",
    PROJECT_ROOT / "snowflake" / "002_create_tables.sql",
    PROJECT_ROOT / "snowflake" / "003_curated_models.sql",
]
CURATED_TO_STAGING_TABLE = {
    "telemetry_enriched": "STAGING.TELEMETRY_ENRICHED",
    "vehicle_hourly_metrics": "STAGING.VEHICLE_HOURLY_METRICS",
    "trip_metrics": "STAGING.TRIP_METRICS",
    "battery_health": "STAGING.BATTERY_HEALTH",
    "alerts": "STAGING.ALERTS",
}


def print_step(message: str) -> None:
    # Keep runner output easy to scan in the terminal.
    print(f"\n==> {message}", flush=True)


def run_command(command: list[str]) -> None:
    # Run a shell command and stop immediately if it fails.
    print(f"$ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def env_value(name: str) -> str:
    # Strip values so accidental spaces in .env do not break checks.
    return os.getenv(name, "").strip()


def missing_env_vars(names: list[str]) -> list[str]:
    # Treat empty values and .env.example placeholders as missing credentials.
    missing = []
    for name in names:
        value = env_value(name)
        if not value or value == "replace_me":
            missing.append(name)
    return missing


def require_env(names: list[str], action: str) -> None:
    # Cloud actions need real credentials loaded from .env.
    missing = missing_env_vars(names)
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"Cannot {action}. Missing or placeholder values in .env: {joined}")


def run_tests(skip_tests: bool) -> None:
    if skip_tests:
        print_step("Skipping tests")
        return
    print_step("Running tests")
    run_command([sys.executable, "-m", "pytest"])


def run_local_etl(skip_local_etl: bool) -> dict[str, Path]:
    if skip_local_etl:
        print_step("Skipping local ETL")
        return {}
    print_step("Running local ETL sample")
    written = write_curated_parquet(SAMPLE_INPUT, CURATED_DIR)
    for name, path in written.items():
        print(f"{name}: {path.relative_to(PROJECT_ROOT)}")
    return written


def connect_for_snowflake_setup():
    # Setup SQL creates the database and warehouse, so connect without requiring them first.
    import snowflake.connector

    require_env(
        ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ROLE"],
        "set up Snowflake",
    )
    return snowflake.connector.connect(
        account=env_value("SNOWFLAKE_ACCOUNT"),
        user=env_value("SNOWFLAKE_USER"),
        password=env_value("SNOWFLAKE_PASSWORD"),
        role=env_value("SNOWFLAKE_ROLE"),
    )


def setup_snowflake(enabled: bool) -> None:
    if not enabled:
        print_step("Skipping Snowflake SQL setup")
        return

    print_step("Running Snowflake SQL setup files")
    conn = connect_for_snowflake_setup()
    try:
        for sql_file in SNOWFLAKE_SQL_FILES:
            print(f"Running {sql_file.relative_to(PROJECT_ROOT)}")
            sql_text = sql_file.read_text(encoding="utf-8")
            conn.execute_string(sql_text)
    finally:
        conn.close()


def load_curated_to_snowflake(enabled: bool) -> None:
    if not enabled:
        print_step("Skipping Snowflake curated load")
        return

    from telemetry_etl.config import load_settings
    from telemetry_etl.load import (
        connect,
        copy_stage_to_staging,
        run_curated_merges,
        upload_parquet_to_stage,
    )

    require_env(
        [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
        ],
        "load curated files to Snowflake",
    )
    print_step("Loading curated Parquet files to Snowflake")
    settings = load_settings()
    conn = connect(settings)
    try:
        for name, table_name in CURATED_TO_STAGING_TABLE.items():
            parquet_file = CURATED_DIR / f"{name}.parquet"
            if not parquet_file.exists():
                raise FileNotFoundError(f"Missing curated file: {parquet_file}")
            print(f"Uploading {parquet_file.relative_to(PROJECT_ROOT)} -> {table_name}")
            upload_parquet_to_stage(conn, parquet_file)
            copy_stage_to_staging(conn, table_name, parquet_file.name)
        print("Running Snowflake merge procedures")
        run_curated_merges(conn)
    finally:
        conn.close()


def start_airflow(enabled: bool) -> None:
    if not enabled:
        print_step("Skipping Airflow startup")
        return

    require_env(["AIRFLOW_UID"], "start Airflow")
    print_step("Starting Airflow with Docker Compose")
    run_command(["docker", "compose", "up", "airflow-init"])
    run_command(["docker", "compose", "up"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Tesla telemetry project from one file.",
    )
    parser.add_argument("--skip-tests", action="store_true", help="Do not run pytest.")
    parser.add_argument("--skip-local-etl", action="store_true", help="Do not create local Parquet files.")
    parser.add_argument(
        "--setup-snowflake",
        action="store_true",
        help="Run the Snowflake SQL setup files using credentials from .env.",
    )
    parser.add_argument(
        "--load-snowflake",
        action="store_true",
        help="Upload local curated Parquet files and merge them into Snowflake.",
    )
    parser.add_argument(
        "--start-airflow",
        action="store_true",
        help="Start Airflow using docker compose after local/cloud setup.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    try:
        print_step("Starting Tesla telemetry end-to-end runner")
        run_tests(args.skip_tests)
        run_local_etl(args.skip_local_etl)
        setup_snowflake(args.setup_snowflake)
        load_curated_to_snowflake(args.load_snowflake)
        start_airflow(args.start_airflow)
    except Exception as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        return 1

    print_step("Runner finished successfully")
    print("Local outputs are in build/curated")
    if not args.start_airflow:
        print("To start Airflow later, run: python scripts/run_end_to_end.py --start-airflow")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
