# Tesla Vehicle Telemetry ETL - End-to-End Flow

This document explains the complete project flow, the files used in each step, and the setup work already completed.

## 1. Project Goal

This project processes Tesla-style connected vehicle telemetry data.

The pipeline reads raw JSONL telemetry events, validates the schema and data quality, transforms the events into curated analytics tables, and prepares the data for loading into Snowflake through an Airflow DAG.

## 2. Main Flow

```text
Raw JSONL telemetry data
  -> schema validation
  -> data quality checks
  -> transformation
  -> curated Parquet files
  -> Snowflake staging
  -> Snowflake curated tables
  -> Airflow orchestration
```

## 3. Step-by-Step Flow With File Names

### Step 1: Read Configuration

File:

- `src/telemetry_etl/config.py`

What happens:

- Reads environment variables from `.env`.
- Loads AWS settings such as S3 bucket, prefix, and region.
- Loads Snowflake settings such as account, user, warehouse, database, and schema.

Related file:

- `.env.example`

Purpose:

- Shows which environment variables the user must fill in.

## Step 2: Read Raw Telemetry Data

Files:

- `data/sample/tesla_telemetry_sample.jsonl`
- `src/telemetry_etl/extract.py`

What happens:

- Local development uses the sample JSONL file.
- Cloud mode lists telemetry files from Amazon S3.
- S3 files are expected under partition-style paths such as:

```text
telemetry/year=2026/month=06/day=06/hour=08/events.jsonl
```

## Step 3: Validate Schema

File:

- `src/telemetry_etl/schemas.py`

What happens:

- Defines the required structure for each telemetry event.
- Checks fields such as:
  - `event_id`
  - `vin`
  - `event_ts`
  - `latitude`
  - `longitude`
  - `speed_mph`
  - `battery_soc`
  - `odometer_miles`
  - `alert_code`

Important rules:

- Battery percentage must be between 0 and 100.
- Speed must be between 0 and 180 mph.
- Latitude and longitude must be valid.
- Unknown fields are rejected.
- VIN is normalized to uppercase.

## Step 4: Run Data Quality Checks

File:

- `src/telemetry_etl/quality.py`

What happens:

- Reads JSONL records.
- Validates each record against the schema.
- Collects valid records.
- Collects quality issues for failed records.

Quality checks include:

- Invalid JSON.
- Missing or invalid fields.
- Duplicate event IDs.
- Event timestamp after ingest timestamp.
- Moving vehicle with zero odometer.
- Alert event without an alert code.

## Step 5: Transform Data Into Curated Tables

File:

- `src/telemetry_etl/transform.py`

What happens:

- Converts valid telemetry records into Pandas DataFrames.
- Adds useful derived fields.
- Creates curated datasets.
- Writes curated data as Parquet files.

Generated curated files:

- `build/curated/telemetry_enriched.parquet`
- `build/curated/vehicle_hourly_metrics.parquet`
- `build/curated/trip_metrics.parquet`
- `build/curated/battery_health.parquet`
- `build/curated/alerts.parquet`

Curated models:

- `telemetry_enriched`: all valid events with extra fields.
- `vehicle_hourly_metrics`: hourly metrics per vehicle.
- `trip_metrics`: daily trip distance and speed metrics.
- `battery_health`: daily battery status metrics.
- `alerts`: only alert events.

## Step 6: Load Data Into Snowflake

File:

- `src/telemetry_etl/load.py`

What happens:

- Connects to Snowflake.
- Uploads Parquet files to the Snowflake internal stage.
- Copies staged files into Snowflake staging tables.
- Runs merge procedures to load curated tables.
- Records processed S3 objects after successful load.

## Step 7: Create Snowflake Objects

Files:

- `snowflake/001_create_database_schema.sql`
- `snowflake/002_create_tables.sql`
- `snowflake/003_curated_models.sql`

What happens:

`001_create_database_schema.sql`:

- Creates the Snowflake database.
- Creates raw, staging, and curated schemas.
- Creates the warehouse.
- Creates the internal stage.

`002_create_tables.sql`:

- Creates the processed object tracking table.
- Creates staging tables.
- Creates curated tables.

`003_curated_models.sql`:

- Creates Snowflake merge procedures.
- Merges staging data into final curated tables.

## Step 8: Orchestrate Pipeline With Airflow

File:

- `dags/tesla_telemetry_etl_dag.py`

What happens:

- Defines the Airflow DAG.
- Runs every 15 minutes.
- Lists S3 files.
- Filters valid JSONL files.
- Runs transformation.
- Loads curated files into Snowflake.
- Marks source files as processed.

Main Airflow tasks:

- `extract_s3_manifest`
- `validate_manifest`
- `transform_local_sample`
- `load_to_snowflake`

## Step 9: Run Airflow Locally

File:

- `docker-compose.yml`

What happens:

- Starts Postgres for Airflow metadata.
- Initializes the Airflow database.
- Creates the Airflow admin user.
- Starts the Airflow webserver.
- Starts the Airflow scheduler.

Airflow UI:

```text
http://localhost:8080
```

Login:

```text
username: airflow
password: airflow
```

## Step 10: Run Tests

Files:

- `tests/test_quality.py`
- `tests/test_transform.py`

What happens:

- Tests schema and quality validation.
- Tests duplicate event detection.
- Tests alert code validation.
- Tests curated transformation output.
- Tests trip distance calculation.

Run tests:

```bash
pytest
```

## Step 11: Run Local ETL

Command:

```bash
python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
```

What happens:

- Reads sample JSONL data.
- Validates records.
- Runs transformations.
- Writes curated Parquet files to `build/curated`.

Windows helper script:

- `scripts/run_local_etl.ps1`

Command:

```powershell
.\scripts\run_local_etl.ps1
```

## Step 12: Continuous Integration

File:

- `.github/workflows/ci.yml`

What happens:

- Runs automatically on GitHub push or pull request.
- Installs Python dependencies.
- Runs the test suite.

## 4. Documentation Files Created

Files:

- `README.md`
- `docs/setup.md`
- `docs/architecture.md`
- `docs/data_dictionary.md`
- `docs/end_to_end_flow.md`

Purpose:

- `README.md`: quick project overview and commands.
- `docs/setup.md`: full setup instructions.
- `docs/architecture.md`: architecture and pipeline design.
- `docs/data_dictionary.md`: field and table definitions.
- `docs/end_to_end_flow.md`: complete project flow with file names and completed steps.

## 5. Steps Already Completed

The following work has been completed:

- Created full Python package under `src/telemetry_etl`.
- Created sample telemetry data under `data/sample`.
- Created schema validation logic.
- Created data quality checks.
- Created transformation logic.
- Created Snowflake setup SQL files.
- Created Airflow DAG.
- Created Docker Compose Airflow setup.
- Created test files.
- Created GitHub Actions CI workflow.
- Created setup and architecture documentation.
- Added simple inline comments in DAG, ETL files, tests, and Docker Compose.
- Ran tests successfully.
- Ran lint successfully.
- Ran local ETL successfully and generated curated Parquet files.

## 6. Verification Commands Already Run

These commands were run successfully:

```bash
pip install -e ".[dev]"
pytest
python3 -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
ruff check .
```

Result:

```text
6 tests passed
lint clean
curated Parquet files generated
```

## 7. What You Need To Do Next

1. Copy `.env.example` to `.env`.
2. Add your real AWS and Snowflake credentials.
3. Run the Snowflake SQL files in order.
4. Upload real JSONL telemetry files to S3.
5. Start Airflow with Docker Compose.
6. Enable the `tesla_telemetry_etl` DAG in the Airflow UI.
7. Monitor DAG runs and Snowflake curated tables.

Commands:

```bash
cp .env.example .env
docker compose up airflow-init
docker compose up
```

Snowflake SQL order:

```text
snowflake/001_create_database_schema.sql
snowflake/002_create_tables.sql
snowflake/003_curated_models.sql
```
