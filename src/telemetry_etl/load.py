# =====================================================================
# load.py
# Job of this file: the "L" in ETL (Load).
# It logs in to Snowflake and moves our Parquet files into tables.
#
# The load happens in 4 small moves.
# Moves 1 to 3 run once for each of the 5 tables:
#   1. TRUNCATE  empty the staging table (it should only hold this run)
#   2. PUT       upload the Parquet file from our computer to a Snowflake stage
#                (a stage is a folder for files inside Snowflake)
#   3. COPY INTO copy the rows from the staged file into the staging table
# Move 4 runs only once, after all 5 tables are copied:
#   4. MERGE     update or insert rows into the final curated tables,
#                using one stored procedure
# =====================================================================

# Path helps us work with file paths.
from pathlib import Path

# The official Snowflake library for Python.
import snowflake.connector

# Fixed names and the settings box from config.py.
from telemetry_etl.config import (
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_KEY_FILE,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE,
    Settings,
)

# Which staging table each Parquet file goes into.
STAGING_TABLES = {
    "telemetry_enriched": "STAGING.TELEMETRY_ENRICHED",
    "vehicle_hourly_metrics": "STAGING.VEHICLE_HOURLY_METRICS",
    "trip_metrics": "STAGING.TRIP_METRICS",
    "battery_health": "STAGING.BATTERY_HEALTH",
    "alerts": "STAGING.ALERTS",
}


# Log in to Snowflake and give back an open connection.
def connect(settings: Settings, use_project_database: bool = True):
    # The login details. We log in with a key file instead of a password,
    # because Snowflake is blocking password-only logins for scripts.
    options = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "role": SNOWFLAKE_ROLE,
        "private_key_file": str(SNOWFLAKE_KEY_FILE),
        # Work in UTC time, so the times we load keep their "+00:00" (UTC) label.
        "session_parameters": {"TIMEZONE": "UTC"},
    }
    # Normally we also pick our warehouse and database.
    # The helper commands in scripts/pipeline_helper.py skip this, because the database may not exist yet.
    if use_project_database:
        options["warehouse"] = SNOWFLAKE_WAREHOUSE
        options["database"] = SNOWFLAKE_DATABASE
    # Open the connection and give it back.
    # "**options" passes every item of the dictionary as a named value, for example account=...
    return snowflake.connector.connect(**options)


# Ask Snowflake which S3 files we already loaded. The result is a set of (key, etag) pairs.
def get_processed_files(connection) -> set[tuple[str, str]]:
    # A cursor is the object that sends SQL to Snowflake. fetchall gives back all result rows.
    rows = connection.cursor().execute("SELECT object_key, etag FROM RAW.PROCESSED_FILES").fetchall()
    # Give back a set of (file path, fingerprint) pairs. A set is fast to search.
    return {(object_key, etag) for object_key, etag in rows}


# Load the 5 Parquet files into Snowflake, then merge them into the curated tables.
def load_tables(connection, parquet_files: dict[str, str]) -> None:
    # A cursor is the object that sends SQL to Snowflake.
    cursor = connection.cursor()
    # Load the 5 Parquet files one by one.
    for name, file_path in parquet_files.items():
        # Find the staging table for this file, for example STAGING.TRIP_METRICS.
        table = STAGING_TABLES[name]
        # The full path of the file on this computer, with forward slashes.
        full_path = Path(file_path).resolve().as_posix()
        # Just the file name, for example trip_metrics.parquet.
        file_name = Path(file_path).name

        # Move 1: empty the staging table. It must only hold the rows of this run.
        cursor.execute(f"TRUNCATE TABLE {table}")
        # Move 2: upload the file to the stage called RAW.TELEMETRY_STAGE.
        # The quotes around the path allow spaces in folder names.
        # AUTO_COMPRESS = FALSE keeps the file as it is (no .gz added to the name).
        # OVERWRITE = TRUE replaces an older file with the same name.
        cursor.execute(f"PUT 'file://{full_path}' @RAW.TELEMETRY_STAGE AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        # Move 3: copy the rows from the staged file into the staging table.
        # MATCH_BY_COLUMN_NAME puts each Parquet column into the table column with the same name.
        cursor.execute(
            f"COPY INTO {table} FROM @RAW.TELEMETRY_STAGE/{file_name} "
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        )

    # Move 4: one stored procedure merges all 5 staging tables into the curated tables.
    cursor.execute("CALL CURATED.MERGE_STAGING_INTO_CURATED()")


# Write the loaded S3 files into RAW.PROCESSED_FILES, so the next run skips them.
def mark_files_as_processed(connection, files: list[dict]) -> None:
    # A cursor sends SQL to Snowflake.
    cursor = connection.cursor()
    # Write one row per S3 file.
    for file in files:
        # %s are placeholders. The connector fills them in safely with the values in the tuple.
        cursor.execute(
            "INSERT INTO RAW.PROCESSED_FILES (bucket_name, object_key, etag, processed_at) "
            "VALUES (%s, %s, %s, CURRENT_TIMESTAMP())",
            (file["bucket"], file["key"], file["etag"]),
        )
