# Running This Project End to End

This file explains how to run the Tesla Vehicle Telemetry ETL Pipeline from start to finish.

## 1. Open Project Folder

Go to the project root:

```bash
cd "/Users/admin/Tesla Elementry Project"
```

## 2. Create Python Virtual Environment

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

For Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

## 3. Install Project Dependencies

```bash
pip install -e ".[dev]"
```

This installs:

- ETL source code from `src/telemetry_etl`
- Testing tools
- Pandas
- PyArrow
- Pydantic
- AWS SDK
- Snowflake connector

## 4. Run Tests

```bash
pytest
```

Expected result:

```text
6 passed
```

Test files:

- `tests/test_quality.py`
- `tests/test_transform.py`

## 5. Run Local ETL Sample

Run this command:

```bash
python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
```

Input file:

```text
data/sample/tesla_telemetry_sample.jsonl
```

Output folder:

```text
build/curated
```

Expected output files:

```text
build/curated/telemetry_enriched.parquet
build/curated/vehicle_hourly_metrics.parquet
build/curated/trip_metrics.parquet
build/curated/battery_health.parquet
build/curated/alerts.parquet
```

## 6. Optional: Inspect Output Parquet File

Run:

```bash
python - <<'PY'
import pandas as pd

df = pd.read_parquet("build/curated/vehicle_hourly_metrics.parquet")
print(df)
PY
```

This confirms the transformation created readable curated data.

## 7. Create Environment File

Copy the example environment file:

```bash
cp .env.example .env
```

Open `.env` and fill in your real values:

```text
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET=your_s3_bucket
S3_PREFIX=telemetry/

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=TELEMETRY_WH
SNOWFLAKE_DATABASE=TESLA_TELEMETRY
SNOWFLAKE_SCHEMA=RAW

AIRFLOW_UID=50000
```

Do not commit `.env` to GitHub.

## 8. Set Up Snowflake

Open a Snowflake worksheet and run these SQL files in this order:

```text
snowflake/001_create_database_schema.sql
snowflake/002_create_tables.sql
snowflake/003_curated_models.sql
```

What these files create:

- Database
- Schemas
- Warehouse
- Internal stage
- Processed S3 object tracking table
- Staging tables
- Curated tables
- Merge procedures

## 9. Upload Raw Files to S3

Upload telemetry JSONL files to your S3 bucket.

Recommended path format:

```text
telemetry/year=2026/month=06/day=06/hour=08/events.jsonl
```

The raw file format should match:

```text
data/sample/tesla_telemetry_sample.jsonl
```

## 10. Start Airflow With Docker

First initialize Airflow:

```bash
docker compose up airflow-init
```

Then start all Airflow services:

```bash
docker compose up
```

This starts:

- Postgres
- Airflow webserver
- Airflow scheduler

## 11. Open Airflow UI

Open this URL:

```text
http://localhost:8080
```

Login:

```text
username: airflow
password: airflow
```

## 12. Run the DAG

In Airflow:

1. Find the DAG named `tesla_telemetry_etl`.
2. Turn the DAG on.
3. Click the play button to trigger a manual run.
4. Open the DAG run graph.
5. Check each task status.

Tasks:

```text
extract_s3_manifest
validate_manifest
transform_local_sample
load_to_snowflake
```

## 13. Check Snowflake Tables

After the DAG runs successfully, check Snowflake:

```sql
SELECT * FROM TESLA_TELEMETRY.CURATED.TELEMETRY_ENRICHED;
SELECT * FROM TESLA_TELEMETRY.CURATED.VEHICLE_HOURLY_METRICS;
SELECT * FROM TESLA_TELEMETRY.CURATED.TRIP_METRICS;
SELECT * FROM TESLA_TELEMETRY.CURATED.BATTERY_HEALTH;
SELECT * FROM TESLA_TELEMETRY.CURATED.ALERTS;
```

Check processed files:

```sql
SELECT * FROM TESLA_TELEMETRY.RAW.PROCESSED_S3_OBJECTS;
```

## 14. Stop Airflow

Press `CTRL+C` in the Docker terminal.

Then run:

```bash
docker compose down
```

## 15. Full Local Run Command Summary

```bash
cd "/Users/admin/Tesla Elementry Project"
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
```

## 16. Single Runner File

You can also run the project from one Python file:

```bash
python scripts/run_end_to_end.py
```

This command runs:

- Tests.
- Local sample ETL.
- Curated Parquet generation.

Runner file:

```text
scripts/run_end_to_end.py
```

### Run Local Project Only

```bash
python scripts/run_end_to_end.py
```

### Run Snowflake SQL Setup

First fill `.env` with Snowflake credentials.

Then run:

```bash
python scripts/run_end_to_end.py --setup-snowflake
```

### Load Curated Parquet Files to Snowflake

First make sure local Parquet files exist in `build/curated`.

Then run:

```bash
python scripts/run_end_to_end.py --load-snowflake
```

### Run Snowflake Setup and Load Together

```bash
python scripts/run_end_to_end.py --setup-snowflake --load-snowflake
```

### Start Airflow From the Runner

```bash
python scripts/run_end_to_end.py --start-airflow
```

### Run Everything From One Command

Use this after `.env` has real AWS, Snowflake, and Airflow values:

```bash
python scripts/run_end_to_end.py --setup-snowflake --load-snowflake --start-airflow
```

### Useful Runner Options

Skip tests:

```bash
python scripts/run_end_to_end.py --skip-tests
```

Skip local ETL:

```bash
python scripts/run_end_to_end.py --skip-local-etl
```

## 17. Full Airflow Run Command Summary

```bash
cp .env.example .env
docker compose up airflow-init
docker compose up
```

Then open:

```text
http://localhost:8080
```

## 18. Common Problems

### Problem: `pytest` command not found

Fix:

```bash
pip install -e ".[dev]"
```

### Problem: Docker cannot find `.env`

Fix:

```bash
cp .env.example .env
```

### Problem: Airflow cannot connect to Snowflake

Check:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

### Problem: No S3 files found

Check:

- `S3_BUCKET`
- `S3_PREFIX`
- AWS credentials
- Raw files uploaded to the correct S3 path

## 19. End-to-End Success Checklist

The project is running end to end when:

- Tests pass.
- Local ETL creates Parquet files.
- Snowflake SQL files run successfully.
- Airflow starts at `localhost:8080`.
- DAG `tesla_telemetry_etl` runs successfully.
- Curated Snowflake tables contain telemetry data.
- `RAW.PROCESSED_S3_OBJECTS` records processed files.
