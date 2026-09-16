-- =====================================================================
-- 01_create_database.sql
-- Job of this file: create the empty places that will hold our data.
--   * a warehouse  = the compute engine that runs our SQL
--   * a database   = the top folder for all our tables
--   * 3 schemas    = sub folders inside the database
--   * a stage      = a landing area where we upload Parquet files
-- create_pipeline.sh runs this file. You can also paste it into a Snowsight worksheet.
-- =====================================================================

-- A role is a set of rights. SYSADMIN is the normal role for creating databases and warehouses.
-- USE ROLE switches to it.
USE ROLE SYSADMIN;

-- IF NOT EXISTS means: create it only if it is not there yet, so this file can run many times.
-- The warehouse. XSMALL is the smallest and cheapest size.
-- AUTO_SUSPEND = 60 turns it off after 60 seconds without work, so we do not pay for idle time.
-- AUTO_RESUME = TRUE turns it on again by itself when a query arrives.
-- INITIALLY_SUSPENDED = TRUE means it starts in the off state.
CREATE WAREHOUSE IF NOT EXISTS TELEMETRY_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- The database that holds everything for this project.
CREATE DATABASE IF NOT EXISTS TESLA_TELEMETRY;

-- RAW schema: the upload stage and the list of S3 files we already loaded.
CREATE SCHEMA IF NOT EXISTS TESLA_TELEMETRY.RAW;

-- STAGING schema: waiting tables that hold only the rows of the current run.
CREATE SCHEMA IF NOT EXISTS TESLA_TELEMETRY.STAGING;

-- CURATED schema: the final tables that reports and dashboards use.
CREATE SCHEMA IF NOT EXISTS TESLA_TELEMETRY.CURATED;

-- The internal stage: a place inside Snowflake where we upload our Parquet files.
-- FILE_FORMAT tells Snowflake how to read the files:
--   TYPE = PARQUET               the files are Parquet files
--   USE_VECTORIZED_SCANNER = TRUE use the newer Parquet reader. It reads the
--                                pandas date-time columns as real timestamps.
--                                Without it, Snowflake reads them as big numbers
--                                and the dates come out wrong.
CREATE STAGE IF NOT EXISTS TESLA_TELEMETRY.RAW.TELEMETRY_STAGE
  FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE);
