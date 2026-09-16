# =====================================================================
# tesla_telemetry_dag.py
# Job of this file: tell Airflow WHICH steps to run and in WHAT order.
#
# A DAG (Directed Acyclic Graph) is Airflow's word for a pipeline:
# a list of tasks connected by arrows, with no loops. Our pipeline has 4 tasks:
#
#   extract_from_s3  ->  validate_data  ->  transform_data  ->  load_to_snowflake
#
# The real work is done by the files in src/telemetry_etl.
# This file only calls them in the right order.
# =====================================================================

# datetime is used to give the DAG a start date.
from datetime import datetime

# dag and task are "decorators" (the lines that start with @): they turn normal Python
# functions into an Airflow pipeline (dag) and Airflow steps (task).
from airflow.decorators import dag, task

# Raising AirflowSkipException marks a task as "skipped" instead of "failed".
from airflow.exceptions import AirflowSkipException

# Our own code: one module per ETL step.
from telemetry_etl import extract, load, quality, transform

# BUILD_DIR is the working folder. load_settings reads the .env values.
from telemetry_etl.config import BUILD_DIR, load_settings


# Describe the pipeline.
@dag(
    # The name you see in the Airflow web page.
    dag_id="tesla_telemetry_etl",
    # Airflow needs a start date. Any date in the past is fine.
    start_date=datetime(2026, 1, 1),
    # None = no timer. The DAG runs only when someone starts it (the play button, or create_pipeline.sh).
    # In a company you might write "@hourly" here to run it every hour.
    schedule=None,
    # Do not try to run for old dates we missed.
    catchup=False,
    # Only one run at a time, so two runs never write the same files.
    max_active_runs=1,
    # Labels that help you find the DAG in the web page.
    tags=["telemetry", "s3", "snowflake"],
)
# The pipeline itself is a function. The 4 tasks are written inside it,
# and the last lines of the function connect them in order.
def tesla_telemetry_etl():

    # ---------------- Task 1: Extract ----------------
    @task
    def extract_from_s3() -> list[dict]:
        # Read our settings from the .env values.
        settings = load_settings()
        # Get the list of all JSONL files in the S3 bucket.
        all_files = extract.list_s3_files(settings)

        # Ask Snowflake which files were already loaded in earlier runs.
        connection = load.connect(settings)
        try:
            done_files = load.get_processed_files(connection)
        finally:
            # Always close the connection, even if something failed.
            connection.close()

        # Keep only the files we have not loaded yet.
        # We compare the file path AND its fingerprint (etag),
        # so a file that was changed in S3 is loaded again.
        new_files = []
        for file in all_files:
            if (file["key"], file["etag"]) not in done_files:
                new_files.append(file)

        # If there is nothing new, skip this task and all tasks after it.
        if not new_files:
            raise AirflowSkipException("No new files in S3. Nothing to do.")

        # Download the new files to build/raw and give back their details.
        return extract.download_files(settings, new_files, BUILD_DIR / "raw")

    # ---------------- Task 2: Validate ----------------
    @task
    def validate_data(files: list[dict]) -> str:
        # Read every downloaded file and put all events into one list.
        records = []
        for file in files:
            records.extend(quality.read_jsonl(file["local_path"]))

        # An empty file has no events, so there is nothing to check or load. Stop with a clear message.
        if not records:
            raise ValueError("The new files have no events. Delete the empty files from S3.")

        # Run the schema and business checks.
        clean_records, problems = quality.check_records(records)

        # If anything is wrong, stop the pipeline here and show every problem.
        # Bad data never reaches Snowflake.
        if problems:
            raise ValueError("Data quality check failed:\n" + "\n".join(problems))

        # Save the clean events to build/clean and give back the file path.
        clean_file = quality.write_jsonl(clean_records, BUILD_DIR / "clean" / "clean_events.jsonl")
        return str(clean_file)

    # ---------------- Task 3: Transform ----------------
    @task
    def transform_data(clean_file: str) -> dict[str, str]:
        # Read the clean events.
        records = quality.read_jsonl(clean_file)
        # Build the 5 report tables.
        tables = transform.build_tables(records)
        # Save them as Parquet files in build/curated.
        saved_files = transform.save_tables_as_parquet(tables, BUILD_DIR / "curated")
        # Airflow can only pass simple values between tasks, so we turn the paths into text.
        return {name: str(path) for name, path in saved_files.items()}

    # ---------------- Task 4: Load ----------------
    @task
    def load_to_snowflake(parquet_files: dict[str, str], files: list[dict]) -> None:
        # Read our settings and log in to Snowflake.
        settings = load_settings()
        connection = load.connect(settings)
        try:
            # Upload, copy and merge the 5 tables.
            load.load_tables(connection, parquet_files)
            # Only after a successful load, remember which S3 files we used.
            load.mark_files_as_processed(connection, files)
        finally:
            # Always close the connection.
            connection.close()

    # ---------------- The order of the tasks ----------------
    # Calling the tasks like normal functions tells Airflow how they connect.
    # The output of one task becomes the input of the next one.
    files = extract_from_s3()
    clean_file = validate_data(files)
    parquet_files = transform_data(clean_file)
    load_to_snowflake(parquet_files, files)


# Create the pipeline, so Airflow finds it when it reads this folder.
tesla_telemetry_etl()
