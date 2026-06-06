# Architecture

## Flow

```text
S3 raw JSONL telemetry
  -> Airflow extract manifest
  -> schema and quality validation
  -> Pandas transformations
  -> curated Parquet files
  -> Snowflake internal stage
  -> staging tables
  -> curated MERGE procedures
  -> monitoring tables
```

## Components

- `src/telemetry_etl/extract.py`: Lists partitioned S3 telemetry objects.
- `src/telemetry_etl/schemas.py`: Defines the required event contract.
- `src/telemetry_etl/quality.py`: Enforces schema and data quality rules.
- `src/telemetry_etl/transform.py`: Builds curated telemetry, hourly, trip, battery, and alert datasets.
- `src/telemetry_etl/load.py`: Uploads Parquet files to Snowflake and runs merge procedures.
- `dags/tesla_telemetry_etl_dag.py`: Orchestrates the pipeline.
- `snowflake/`: Creates database, stages, tables, and merge procedures.

## Incremental Processing

Snowflake table `RAW.PROCESSED_S3_OBJECTS` stores each successfully processed S3 object by bucket, key, and ETag. A production DAG should filter out keys already present in this control table before transforming the next batch.

## Quality Gates

The local quality layer rejects:

- Invalid JSON.
- Schema mismatches.
- Battery state of charge outside 0 to 100.
- Speed outside 0 to 180 mph.
- Invalid latitude or longitude.
- Invalid odometer values.
- Future event timestamps relative to ingest timestamps.
- Duplicate event IDs in a batch.
- Alert events without alert codes.
