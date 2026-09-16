-- =====================================================================
-- 03_create_merge_procedure.sql
-- Job of this file: create ONE stored procedure that moves the rows
-- from the 5 staging tables into the 5 curated tables.
--
-- It uses MERGE. For every row in staging, MERGE looks for the same
-- row in the curated table (using a key such as event_id):
--   * found     -> UPDATE the curated row with the new values
--   * not found -> INSERT it as a new row
-- So running the pipeline twice never creates duplicate rows.
--
-- ALL BY NAME means: match the columns by their names.
-- This works because each staging table has exactly the same columns
-- as its curated table (see LIKE in 02_create_tables.sql).
-- =====================================================================

-- Work as SYSADMIN.
USE ROLE SYSADMIN;

-- CREATE OR REPLACE: build the procedure, or replace an older version.
-- RETURNS STRING: the procedure gives back a short text message.
-- LANGUAGE SQL: the body is written in Snowflake SQL.
-- The body sits between the two $$ signs.
CREATE OR REPLACE PROCEDURE TESLA_TELEMETRY.CURATED.MERGE_STAGING_INTO_CURATED()
RETURNS STRING
LANGUAGE SQL
AS
$$
-- BEGIN and END mark the start and the end of the procedure body.
BEGIN
  -- Table 1: events. One row per event_id.
  -- "AS target" and "AS source" are short names for the two tables, used in the ON line.
  MERGE INTO TESLA_TELEMETRY.CURATED.TELEMETRY_ENRICHED AS target
  USING TESLA_TELEMETRY.STAGING.TELEMETRY_ENRICHED AS source
  ON target.event_id = source.event_id
  WHEN MATCHED THEN UPDATE ALL BY NAME
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;

  -- Table 2: hourly numbers. One row per car and hour.
  MERGE INTO TESLA_TELEMETRY.CURATED.VEHICLE_HOURLY_METRICS AS target
  USING TESLA_TELEMETRY.STAGING.VEHICLE_HOURLY_METRICS AS source
  ON target.vin = source.vin AND target.event_hour = source.event_hour
  WHEN MATCHED THEN UPDATE ALL BY NAME
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;

  -- Table 3: trips. One row per car and day.
  MERGE INTO TESLA_TELEMETRY.CURATED.TRIP_METRICS AS target
  USING TESLA_TELEMETRY.STAGING.TRIP_METRICS AS source
  ON target.vin = source.vin AND target.event_date = source.event_date
  WHEN MATCHED THEN UPDATE ALL BY NAME
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;

  -- Table 4: battery. One row per car and day.
  MERGE INTO TESLA_TELEMETRY.CURATED.BATTERY_HEALTH AS target
  USING TESLA_TELEMETRY.STAGING.BATTERY_HEALTH AS source
  ON target.vin = source.vin AND target.event_date = source.event_date
  WHEN MATCHED THEN UPDATE ALL BY NAME
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;

  -- Table 5: alerts. One row per event_id.
  MERGE INTO TESLA_TELEMETRY.CURATED.ALERTS AS target
  USING TESLA_TELEMETRY.STAGING.ALERTS AS source
  ON target.event_id = source.event_id
  WHEN MATCHED THEN UPDATE ALL BY NAME
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;

  -- Send back a short message to show that everything worked.
  RETURN 'Merged 5 staging tables into curated tables';
END;
$$;
