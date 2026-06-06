from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from telemetry_etl.config import load_settings
from telemetry_etl.extract import list_partitioned_objects
from telemetry_etl.load import (
    connect,
    copy_stage_to_staging,
    record_processed_object,
    run_curated_merges,
    upload_parquet_to_stage,
)
from telemetry_etl.transform import write_curated_parquet


# These settings apply to every task in this DAG.
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# This decorator tells Airflow how often to run the pipeline and how to show it in the UI.
@dag(
    dag_id="tesla_telemetry_etl",
    default_args=DEFAULT_ARGS,
    description="Ingest Tesla-style vehicle telemetry from S3 and load curated Snowflake tables.",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["telemetry", "snowflake", "s3"],
)
def tesla_telemetry_etl():
    @task
    def extract_s3_manifest() -> list[dict]:
        # Read AWS and Snowflake settings from environment variables.
        settings = load_settings()
        # Build a list of raw files waiting under the configured S3 prefix.
        objects = list_partitioned_objects(settings.s3_bucket, settings.s3_prefix, settings.aws_region)
        # Airflow task outputs must be JSON-friendly, so convert dataclass objects to dictionaries.
        return [obj.__dict__ for obj in objects]

    @task
    def validate_manifest(objects: list[dict]) -> list[dict]:
        # Only JSONL files are valid raw telemetry inputs for this pipeline.
        return [obj for obj in objects if obj["key"].endswith(".jsonl")]

    @task
    def transform_local_sample(objects: list[dict]) -> dict[str, str]:
        # Portfolio/local mode transforms sample data. Replace download logic here for production S3 files.
        del objects
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write first to a temporary folder so partial files do not look finished.
            output_dir = Path(temp_dir) / "curated"
            written = write_curated_parquet(
                "/opt/airflow/data/sample/tesla_telemetry_sample.jsonl",
                output_dir,
            )
            # Copy final Parquet files into a mounted project folder Airflow can reuse.
            durable_output = Path("/opt/airflow/build/curated")
            durable_output.mkdir(parents=True, exist_ok=True)
            copied = {}
            for name, path in written.items():
                # Store each curated table file path by its logical table name.
                target = durable_output / path.name
                target.write_bytes(path.read_bytes())
                copied[name] = str(target)
            return copied

    @task
    def load_to_snowflake(curated_files: dict[str, str], objects: list[dict]) -> str:
        # Open one Snowflake connection for all stage, copy, and merge work.
        settings = load_settings()
        conn = connect(settings)
        # Map local curated file names to their Snowflake staging tables.
        table_map = {
            "telemetry_enriched": "STAGING.TELEMETRY_ENRICHED",
            "vehicle_hourly_metrics": "STAGING.VEHICLE_HOURLY_METRICS",
            "trip_metrics": "STAGING.TRIP_METRICS",
            "battery_health": "STAGING.BATTERY_HEALTH",
            "alerts": "STAGING.ALERTS",
        }
        try:
            for name, path in curated_files.items():
                # Upload the Parquet file to Snowflake internal stage.
                upload_parquet_to_stage(conn, path)
                # Copy the staged file into the matching staging table.
                copy_stage_to_staging(conn, table_map[name], Path(path).name)
            # Merge all staging tables into curated warehouse tables.
            run_curated_merges(conn)
            for obj in objects:
                # Mark source files as processed only after the Snowflake load succeeds.
                record_processed_object(conn, obj["bucket"], obj["key"], obj["etag"])
        finally:
            # Always close the warehouse connection, even if a task fails.
            conn.close()
        return "loaded"

    # Define the task order shown in the Airflow graph.
    manifest = extract_s3_manifest()
    valid_manifest = validate_manifest(manifest)
    curated = transform_local_sample(valid_manifest)
    load_to_snowflake(curated, valid_manifest)


# This creates the DAG object Airflow discovers when it scans the dags folder.
tesla_telemetry_etl()
