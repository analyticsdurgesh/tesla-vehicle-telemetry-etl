# Tesla Vehicle Telemetry ETL Pipeline

Portfolio-grade data engineering project for processing Tesla-style connected vehicle telemetry. The pipeline ingests raw JSONL telemetry, validates schema and data quality, transforms records into curated monitoring datasets, and prepares the output for Snowflake loading through Apache Airflow orchestration.

## Project Summary

This project demonstrates a full modern ETL workflow:

- Raw telemetry ingestion from Amazon S3 style partitioned paths.
- Schema validation using Pydantic.
- Data quality gates for battery, speed, location, odometer, timestamps, duplicate events, and alert records.
- Curated transformation logic using Pandas and Parquet.
- Snowflake staging, internal stage upload, `COPY INTO`, and `MERGE` loading.
- Incremental processing using a `PROCESSED_S3_OBJECTS` control table.
- Airflow DAG orchestration.
- Docker Compose local Airflow runtime.
- Automated tests and GitHub Actions CI.

## Architecture

```text
Vehicle Telemetry
  -> Raw JSONL files
  -> Amazon S3 raw zone
  -> Airflow DAG
  -> Schema validation
  -> Data quality checks
  -> Curated transformations
  -> Parquet output
  -> Snowflake internal stage
  -> Snowflake staging tables
  -> Snowflake curated tables
  -> Monitoring and analytics
```

Detailed architecture diagram:

- [docs/architecture_flow_diagram.md](docs/architecture_flow_diagram.md)

## Repository Structure

```text
.
|-- .github/workflows/ci.yml
|-- dags/
|   `-- tesla_telemetry_etl_dag.py
|-- data/
|   `-- sample/
|       `-- tesla_telemetry_sample.jsonl
|-- docs/
|   |-- architecture.md
|   |-- architecture_flow_diagram.md
|   |-- data_dictionary.md
|   |-- end_to_end_flow.md
|   |-- run_project_end_to_end.md
|   `-- setup.md
|-- scripts/
|   |-- run_end_to_end.py
|   `-- run_local_etl.ps1
|-- snowflake/
|   |-- 001_create_database_schema.sql
|   |-- 002_create_tables.sql
|   `-- 003_curated_models.sql
|-- src/
|   `-- telemetry_etl/
|       |-- config.py
|       |-- extract.py
|       |-- load.py
|       |-- quality.py
|       |-- schemas.py
|       `-- transform.py
|-- tests/
|   |-- test_quality.py
|   `-- test_transform.py
|-- docker-compose.yml
|-- pyproject.toml
|-- requirements.txt
`-- .env.example
```

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
```

Generated files:

```text
build/curated/telemetry_enriched.parquet
build/curated/vehicle_hourly_metrics.parquet
build/curated/trip_metrics.parquet
build/curated/battery_health.parquet
build/curated/alerts.parquet
```

## Single Runner

Run tests and local ETL from one file:

```bash
python scripts/run_end_to_end.py
```

Run Snowflake setup using credentials from `.env`:

```bash
python scripts/run_end_to_end.py --setup-snowflake
```

Load local curated Parquet files to Snowflake:

```bash
python scripts/run_end_to_end.py --load-snowflake
```

Run setup, load, and Airflow startup:

```bash
python scripts/run_end_to_end.py --setup-snowflake --load-snowflake --start-airflow
```

## Airflow Local Run

Copy the environment file and fill in credentials:

```bash
cp .env.example .env
```

Start Airflow:

```bash
docker compose up airflow-init
docker compose up
```

Airflow UI:

```text
http://localhost:8080
```

Login:

```text
username: airflow
password: airflow
```

DAG name:

```text
tesla_telemetry_etl
```

## Snowflake Setup

Run SQL files in order:

```text
snowflake/001_create_database_schema.sql
snowflake/002_create_tables.sql
snowflake/003_curated_models.sql
```

Created objects include:

- `TESLA_TELEMETRY` database.
- `RAW`, `STAGING`, and `CURATED` schemas.
- `TELEMETRY_WH` warehouse.
- `TELEMETRY_INTERNAL_STAGE` internal stage.
- `RAW.PROCESSED_S3_OBJECTS` incremental tracking table.
- Curated telemetry, hourly metrics, trip metrics, battery health, and alerts tables.
- Merge procedures for curated loading.

## Curated Datasets

| Dataset | Description |
| --- | --- |
| `telemetry_enriched` | Valid telemetry events with derived date, hour, movement, charging, and battery-band fields. |
| `vehicle_hourly_metrics` | Hourly speed, battery, movement, charging, and odometer metrics per VIN. |
| `trip_metrics` | Daily trip distance, speed, odometer, and autopilot metrics per VIN. |
| `battery_health` | Daily battery state-of-charge and temperature metrics per VIN. |
| `alerts` | Alert-only telemetry events for operations monitoring. |

## Environment Variables

Copy `.env.example` to `.env` and fill in:

- AWS region, access key, secret key, bucket, and prefix.
- Snowflake account, user, password, role, warehouse, database, and schema.
- Airflow local runtime values.

Never commit `.env`.

## Testing

```bash
pytest
ruff check .
```

Current verification:

```text
6 tests passed
lint clean
local ETL generates curated Parquet files
```

## Documentation

- [docs/setup.md](docs/setup.md): full setup checklist.
- [docs/run_project_end_to_end.md](docs/run_project_end_to_end.md): exact commands to run the project.
- [docs/end_to_end_flow.md](docs/end_to_end_flow.md): step-by-step project flow with file names.
- [docs/architecture.md](docs/architecture.md): architecture explanation.
- [docs/architecture_flow_diagram.md](docs/architecture_flow_diagram.md): visual text diagram.
- [docs/data_dictionary.md](docs/data_dictionary.md): raw and curated field definitions.

## Portfolio Highlights

This project is suitable for a data engineering portfolio because it includes ingestion, orchestration, validation, quality checks, warehouse modeling, incremental loading, CI, documentation, and a reproducible local development workflow.
