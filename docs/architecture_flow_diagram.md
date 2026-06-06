# End-to-End Architecture Flow Diagram

This diagram shows the complete flow of the Tesla Vehicle Telemetry ETL Pipeline from raw vehicle events to curated Snowflake monitoring tables.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TESLA VEHICLE TELEMETRY                            │
│                                                                             │
│   Vehicle sends telemetry events such as speed, battery, location, alerts   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            RAW JSONL DATA                                  │
│                                                                             │
│   File: data/sample/tesla_telemetry_sample.jsonl                            │
│                                                                             │
│   Example fields:                                                           │
│   event_id, vin, event_ts, latitude, longitude, speed_mph, battery_soc      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AMAZON S3 RAW ZONE                                 │
│                                                                             │
│   File: src/telemetry_etl/extract.py                                        │
│                                                                             │
│   S3 path format:                                                           │
│   telemetry/year=2026/month=06/day=06/hour=08/events.jsonl                 │
│                                                                             │
│   Purpose:                                                                  │
│   Store raw partitioned telemetry files                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AIRFLOW ORCHESTRATION                            │
│                                                                             │
│   File: dags/tesla_telemetry_etl_dag.py                                     │
│                                                                             │
│   DAG Name: tesla_telemetry_etl                                             │
│   Schedule: Every 15 minutes                                                │
│                                                                             │
│   Airflow Tasks:                                                            │
│   1. extract_s3_manifest                                                    │
│   2. validate_manifest                                                      │
│   3. transform_local_sample                                                 │
│   4. load_to_snowflake                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONFIGURATION LAYER                              │
│                                                                             │
│   Files:                                                                    │
│   .env.example                                                              │
│   src/telemetry_etl/config.py                                               │
│                                                                             │
│   Purpose:                                                                  │
│   Load AWS, Snowflake, and Airflow runtime settings                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SCHEMA VALIDATION                                 │
│                                                                             │
│   File: src/telemetry_etl/schemas.py                                        │
│                                                                             │
│   Checks:                                                                   │
│   ✓ Required fields exist                                                   │
│   ✓ VIN length is valid                                                     │
│   ✓ Battery percentage is 0 to 100                                          │
│   ✓ Speed is 0 to 180 mph                                                   │
│   ✓ Latitude and longitude are valid                                        │
│   ✓ Event type is telemetry, charge, or alert                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA QUALITY GATES                                 │
│                                                                             │
│   File: src/telemetry_etl/quality.py                                        │
│                                                                             │
│   Quality Rules:                                                            │
│   ✓ Reject invalid JSON                                                     │
│   ✓ Reject duplicate event IDs                                              │
│   ✓ Reject event_ts after ingest_ts                                         │
│   ✓ Reject moving vehicle with zero odometer                                │
│   ✓ Reject alert event without alert_code                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │       QUALITY PASSED?        │
                       └─────────────────────────────┘
                              │                 │
                            YES                 NO
                              │                 │
                              ▼                 ▼
┌─────────────────────────────────────┐   ┌──────────────────────────────────┐
│        TRANSFORMATION LAYER          │   │       STOP PIPELINE RUN          │
│                                      │   │                                  │
│ File: src/telemetry_etl/transform.py │   │ Bad records are reported as      │
│                                      │   │ quality issues                   │
└─────────────────────────────────────┘   └──────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CURATED DATASETS                                   │
│                                                                             │
│   Output folder: build/curated                                              │
│                                                                             │
│   Generated Parquet files:                                                  │
│   1. telemetry_enriched.parquet                                             │
│   2. vehicle_hourly_metrics.parquet                                         │
│   3. trip_metrics.parquet                                                   │
│   4. battery_health.parquet                                                 │
│   5. alerts.parquet                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE LOAD LAYER                               │
│                                                                             │
│   File: src/telemetry_etl/load.py                                           │
│                                                                             │
│   Steps:                                                                    │
│   1. Connect to Snowflake                                                   │
│   2. Upload Parquet files to internal stage                                 │
│   3. COPY INTO staging tables                                               │
│   4. Run MERGE procedures                                                   │
│   5. Record processed S3 objects                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE SETUP SQL                                │
│                                                                             │
│   Files:                                                                    │
│   snowflake/001_create_database_schema.sql                                  │
│   snowflake/002_create_tables.sql                                           │
│   snowflake/003_curated_models.sql                                          │
│                                                                             │
│   Creates:                                                                  │
│   ✓ Database                                                                │
│   ✓ Schemas                                                                 │
│   ✓ Warehouse                                                               │
│   ✓ Internal stage                                                          │
│   ✓ Staging tables                                                          │
│   ✓ Curated tables                                                          │
│   ✓ Merge procedures                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE STAGING TABLES                           │
│                                                                             │
│   STAGING.TELEMETRY_ENRICHED                                                │
│   STAGING.VEHICLE_HOURLY_METRICS                                            │
│   STAGING.TRIP_METRICS                                                      │
│   STAGING.BATTERY_HEALTH                                                    │
│   STAGING.ALERTS                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE CURATED TABLES                           │
│                                                                             │
│   CURATED.TELEMETRY_ENRICHED                                                │
│   CURATED.VEHICLE_HOURLY_METRICS                                            │
│   CURATED.TRIP_METRICS                                                      │
│   CURATED.BATTERY_HEALTH                                                    │
│   CURATED.ALERTS                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       INCREMENTAL LOAD TRACKING                            │
│                                                                             │
│   Table: RAW.PROCESSED_S3_OBJECTS                                           │
│                                                                             │
│   Purpose:                                                                  │
│   Track bucket name, object key, and ETag after successful load             │
│                                                                             │
│   Benefit:                                                                  │
│   Prevents the same S3 file from being processed again                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MONITORING AND ANALYTICS                           │
│                                                                             │
│   Use curated tables for:                                                   │
│   ✓ Vehicle hourly monitoring                                               │
│   ✓ Battery health dashboards                                               │
│   ✓ Alert tracking                                                          │
│   ✓ Trip metrics                                                            │
│   ✓ Fleet-level analytics                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Local Development Flow

```text
┌────────────────────────────┐
│ Install Python package      │
│ pip install -e ".[dev]"     │
└────────────────────────────┘
              │
              ▼
┌────────────────────────────┐
│ Run tests                   │
│ pytest                      │
└────────────────────────────┘
              │
              ▼
┌────────────────────────────┐
│ Run local ETL sample        │
│ python -m telemetry_etl...  │
└────────────────────────────┘
              │
              ▼
┌────────────────────────────┐
│ Check curated Parquet files │
│ build/curated              │
└────────────────────────────┘
```

## Airflow Local Runtime Flow

```text
┌────────────────────────────┐
│ docker-compose.yml          │
└────────────────────────────┘
              │
              ▼
┌────────────────────────────┐
│ Postgres                    │
│ Airflow metadata database   │
└────────────────────────────┘
              │
              ▼
┌────────────────────────────┐
│ airflow-init                │
│ Initialize Airflow DB       │
└────────────────────────────┘
              │
              ▼
┌────────────────────────────┐
│ airflow-webserver           │
│ UI at localhost:8080        │
└────────────────────────────┘
              │
              ▼
┌────────────────────────────┐
│ airflow-scheduler           │
│ Runs DAG tasks              │
└────────────────────────────┘
```

## Testing and CI Flow

```text
┌────────────────────────────────────┐
│ Developer pushes code to GitHub     │
└────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────┐
│ .github/workflows/ci.yml            │
└────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────┐
│ Install dependencies                │
│ pip install -e ".[dev]"             │
└────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────┐
│ Run tests                           │
│ pytest                              │
└────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────┐
│ Pass or fail CI                     │
└────────────────────────────────────┘
```

## One-Line Summary

```text
Vehicle Telemetry -> S3 Raw Zone -> Airflow DAG -> Validation -> Quality Gates
-> Transformations -> Parquet -> Snowflake Stage -> Staging Tables
-> Merge Procedures -> Curated Tables -> Monitoring and Analytics
```
