-- =====================================================================
-- 99_drop_everything.sql
-- Job of this file: delete everything this project created in Snowflake.
-- remove_pipeline.sh runs this file. You can also paste it into Snowsight.
-- WARNING: all project data in Snowflake is deleted.
-- For a short time (1 day by default) you can still get it back with UNDROP DATABASE.
-- Snowflake calls this Time Travel. After that, the data cannot be brought back.
-- =====================================================================

-- Work as SYSADMIN, the role that created the objects.
USE ROLE SYSADMIN;

-- Dropping the database also drops its 3 schemas, all tables, the stage and the procedure.
DROP DATABASE IF EXISTS TESLA_TELEMETRY;

-- Drop the warehouse (the compute engine).
DROP WAREHOUSE IF EXISTS TELEMETRY_WH;
