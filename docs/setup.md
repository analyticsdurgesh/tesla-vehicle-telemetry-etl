# End-to-End Setup Guide

This guide takes the project from local development to a cloud-ready Airflow plus Snowflake pipeline.

## 1. Local Machine Setup

Install:

- Python 3.10 or newer.
- Docker Desktop.
- Git.
- A Snowflake account.
- An AWS account with an S3 bucket for raw telemetry.

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Run the sample ETL:

```bash
python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
```

Expected files:

- `build/curated/telemetry_enriched.parquet`
- `build/curated/vehicle_hourly_metrics.parquet`
- `build/curated/trip_metrics.parquet`
- `build/curated/battery_health.parquet`
- `build/curated/alerts.parquet`

## 2. Configure Environment Variables

Copy the example file:

```bash
cp .env.example .env
```

Fill these values:

- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `S3_BUCKET`
- `S3_PREFIX`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

For local Airflow, keep:

```text
AIRFLOW_UID=50000
```

## 3. Create AWS S3 Raw Zone

Create a bucket such as:

```text
tesla-telemetry-raw
```

Use partition-aware paths:

```text
telemetry/year=2026/month=06/day=06/hour=08/events.jsonl
```

Upload JSONL telemetry files matching `docs/data_dictionary.md`.

Minimum IAM permissions for the Airflow user:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::tesla-telemetry-raw"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::tesla-telemetry-raw/telemetry/*"]
    }
  ]
}
```

## 4. Create Snowflake Objects

In a Snowflake worksheet, run these files in order:

```sql
-- 1
snowflake/001_create_database_schema.sql

-- 2
snowflake/002_create_tables.sql

-- 3
snowflake/003_curated_models.sql
```

Confirm:

```sql
SHOW DATABASES LIKE 'TESLA_TELEMETRY';
SHOW SCHEMAS IN DATABASE TESLA_TELEMETRY;
SHOW TABLES IN SCHEMA TESLA_TELEMETRY.CURATED;
SHOW PROCEDURES IN SCHEMA TESLA_TELEMETRY.RAW;
```

## 5. Run Airflow Locally

Create `.env` first, then run:

```bash
docker compose up airflow-init
docker compose up
```

Open:

```text
http://localhost:8080
```

Login:

```text
username: airflow
password: airflow
```

Enable the `tesla_telemetry_etl` DAG.

## 6. Development Workflow

Use this loop while building:

```bash
pytest
python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
```

Check generated Parquet files:

```bash
python - <<'PY'
import pandas as pd
print(pd.read_parquet("build/curated/vehicle_hourly_metrics.parquet"))
PY
```

## 7. Production Hardening Checklist

- Replace the local sample transform step in `dags/tesla_telemetry_etl_dag.py` with S3 object download logic.
- Add Snowflake external stage credentials or use cloud storage integration.
- Store credentials in Airflow Connections or a secrets backend instead of `.env`.
- Add dead-letter handling for failed telemetry files.
- Add alerting for DAG failures and quality-gate failures.
- Add partition pruning by processing only unprocessed S3 keys.
- Add dashboard queries on `CURATED.VEHICLE_HOURLY_METRICS`, `CURATED.BATTERY_HEALTH`, and `CURATED.ALERTS`.

## 8. Portfolio Talking Points

This project demonstrates:

- Cloud object storage ingestion.
- Incremental file processing.
- Data validation and quality gates.
- Warehouse staging and curated modeling.
- Batch orchestration with Airflow.
- Snowflake `COPY INTO` and `MERGE` patterns.
- Modular Python package design.
- CI-backed tests.
