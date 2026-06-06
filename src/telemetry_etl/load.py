from __future__ import annotations

from pathlib import Path

import snowflake.connector

from telemetry_etl.config import Settings


def connect(settings: Settings):
    # Open a Snowflake connection using values loaded from environment variables.
    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        role=settings.snowflake_role,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )


def upload_parquet_to_stage(conn, local_file: str | Path, stage: str = "TELEMETRY_INTERNAL_STAGE") -> None:
    # Snowflake PUT uploads the local Parquet file into the internal stage.
    path = Path(local_file).resolve()
    with conn.cursor() as cursor:
        cursor.execute(f"PUT file://{path} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")


def copy_stage_to_staging(conn, table_name: str, stage_file: str, stage: str = "TELEMETRY_INTERNAL_STAGE") -> None:
    # COPY INTO reads the staged Parquet file and appends it to a staging table.
    sql = f"""
    COPY INTO {table_name}
    FROM @{stage}/{stage_file}
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)


def record_processed_object(conn, bucket: str, key: str, etag: str) -> None:
    # This control table prevents loading the same S3 object more than once.
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO PROCESSED_S3_OBJECTS (bucket_name, object_key, etag, processed_at)
            SELECT %s, %s, %s, CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
              SELECT 1 FROM PROCESSED_S3_OBJECTS
              WHERE bucket_name = %s AND object_key = %s AND etag = %s
            )
            """,
            (bucket, key, etag, bucket, key, etag),
        )


def run_curated_merges(conn) -> None:
    # Each stored procedure merges one staging table into its final curated table.
    merge_files = [
        "MERGE_TELEMETRY_ENRICHED",
        "MERGE_VEHICLE_HOURLY_METRICS",
        "MERGE_TRIP_METRICS",
        "MERGE_BATTERY_HEALTH",
        "MERGE_ALERTS",
    ]
    with conn.cursor() as cursor:
        for proc in merge_files:
            # Stored procedures keep warehouse merge logic close to the Snowflake tables.
            cursor.execute(f"CALL {proc}()")
