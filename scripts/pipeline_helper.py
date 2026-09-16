# =====================================================================
# pipeline_helper.py
# Job of this file: small helper commands for create_pipeline.sh and
# remove_pipeline.sh. Each command does one simple thing.
#
# How to run one command (from the project folder):
#   PYTHONPATH=src .venv/bin/python scripts/pipeline_helper.py check-snowflake
#
# Commands:
#   check-snowflake        log in to Snowflake and print who we are
#   check-s3               list the raw JSONL files in the S3 bucket
#   run-sql FILE [FILE..]  run one or more SQL files in Snowflake
#   show-results           print row counts of the curated tables
# =====================================================================

# sys gives us the words typed after the script name (sys.argv) and lets us stop the script (sys.exit).
import sys

# Our own code.
from telemetry_etl import extract, load
from telemetry_etl.config import PROJECT_ROOT, S3_PREFIX, load_settings

# The 5 curated tables we want to count in show-results.
CURATED_TABLES = [
    "CURATED.TELEMETRY_ENRICHED",
    "CURATED.VEHICLE_HOURLY_METRICS",
    "CURATED.TRIP_METRICS",
    "CURATED.BATTERY_HEALTH",
    "CURATED.ALERTS",
]


# Command: check-snowflake
def check_snowflake() -> None:
    # Log in without choosing a database (it may not exist yet).
    connection = load.connect(load_settings(), use_project_database=False)
    # Ask Snowflake who we are. fetchone gives back the one result row.
    user, role, region = connection.cursor().execute(
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_REGION()"
    ).fetchone()
    # Print the answer.
    print(f"Snowflake login OK: user={user} role={role} region={region}")
    # Close the connection.
    connection.close()


# Command: check-s3
def check_s3() -> None:
    # Read the settings and list the JSONL files in the bucket.
    settings = load_settings()
    files = extract.list_s3_files(settings)
    # Print each file we found.
    for file in files:
        print(f"Found in S3: s3://{file['bucket']}/{file['key']}")
    # If there are no files, the pipeline has nothing to load. Stop with an error (exit code 1).
    if not files:
        print(f"No .jsonl files found in s3://{settings.s3_bucket}/{S3_PREFIX}")
        sys.exit(1)


# Command: run-sql. "*file_names" collects all file names given after the command into one list.
def run_sql(*file_names: str) -> None:
    # Log in without choosing a database (the SQL files create or drop it).
    connection = load.connect(load_settings(), use_project_database=False)
    # Run the files in the order they were given.
    for file_name in file_names:
        print(f"Running {file_name}")
        # Read the whole SQL file as text.
        sql_text = (PROJECT_ROOT / file_name).read_text(encoding="utf-8")
        # execute_string runs every statement in the file, one after another.
        connection.execute_string(sql_text)
    # Close the connection.
    connection.close()


# Command: show-results
def show_results() -> None:
    # Log in with our warehouse and database.
    connection = load.connect(load_settings())
    cursor = connection.cursor()
    # Count the rows in each curated table.
    print("Rows in the curated tables:")
    for table in CURATED_TABLES:
        # The result is one row with one number. [0] takes that number out of the row.
        count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        # {table:35} writes the name in a 35-character wide column, so the numbers line up.
        print(f"  {table:35} {count}")
    # Show which S3 files have been loaded.
    print("Files already loaded (RAW.PROCESSED_FILES):")
    rows = cursor.execute(
        "SELECT object_key, processed_at FROM RAW.PROCESSED_FILES ORDER BY processed_at"
    ).fetchall()
    for object_key, processed_at in rows:
        print(f"  {object_key}  (loaded at {processed_at:%Y-%m-%d %H:%M:%S %Z})")
    # Close the connection.
    connection.close()


# Link each command word to its function.
COMMANDS = {
    "check-snowflake": check_snowflake,
    "check-s3": check_s3,
    "run-sql": run_sql,
    "show-results": show_results,
}

# This part runs only when the file is started as a script (not when it is imported).
if __name__ == "__main__":
    # sys.argv is the list of words on the command line. sys.argv[0] is the script name.
    # If no command or an unknown command was given, print the list of commands and stop.
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("Usage: pipeline_helper.py <command>. Commands: " + ", ".join(COMMANDS))
        sys.exit(2)
    # Take the command word and any extra words after it.
    command, arguments = sys.argv[1], sys.argv[2:]
    try:
        # Run the matching function with the extra words (for run-sql these are file names).
        COMMANDS[command](*arguments)
    except Exception as error:
        # Print a short, readable error instead of a long Python traceback.
        print(f"ERROR in '{command}': {error}")
        sys.exit(1)
