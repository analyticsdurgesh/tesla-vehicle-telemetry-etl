-- =====================================================================
-- 02_create_tables.sql
-- Job of this file: create all the tables.
--   * RAW.PROCESSED_FILES   the list of S3 files we already loaded
--   * 5 CURATED tables      the final report tables
--   * 5 STAGING tables      same columns as the curated tables, used as a waiting area
-- =====================================================================

-- Work as SYSADMIN inside our database, so we can write RAW.x instead of TESLA_TELEMETRY.RAW.x
USE ROLE SYSADMIN;
USE DATABASE TESLA_TELEMETRY;

-- Column types used below:
--   STRING         text
--   FLOAT          a number with decimals
--   NUMBER         a whole number
--   BOOLEAN        TRUE or FALSE
--   DATE           a day
--   TIMESTAMP_TZ   a date and time with its time zone
--   TIMESTAMP_LTZ  a date and time shown in the time zone of the person who reads it
--   NOT NULL       the column must always have a value

-- ---------------------------------------------------------------------
-- The list of S3 files we already loaded.
-- The pipeline reads this list to skip old files, and adds a row after every load.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.PROCESSED_FILES (
  bucket_name  STRING NOT NULL,         -- S3 bucket name
  object_key   STRING NOT NULL,         -- full path of the file inside the bucket
  etag         STRING NOT NULL,         -- fingerprint of the file content
  processed_at TIMESTAMP_LTZ NOT NULL   -- when we loaded it
);

-- ---------------------------------------------------------------------
-- Curated table 1: every event plus helpful extra columns.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CURATED.TELEMETRY_ENRICHED (
  event_id          STRING NOT NULL,    -- unique id of the event
  vin               STRING NOT NULL,    -- which car
  event_ts          TIMESTAMP_TZ NOT NULL, -- when it happened in the car
  ingest_ts         TIMESTAMP_TZ NOT NULL, -- when we received it
  event_type        STRING NOT NULL,    -- telemetry, charge or alert
  latitude          FLOAT,              -- GPS position
  longitude         FLOAT,              -- GPS position
  speed_mph         FLOAT,              -- speed in miles per hour
  battery_soc       FLOAT,              -- battery percent
  battery_temp_c    FLOAT,              -- battery temperature in Celsius
  odometer_miles    FLOAT,              -- total miles driven
  charging_state    STRING,             -- for example Charging or Disconnected
  gear              STRING,             -- for example P or D
  autopilot_engaged BOOLEAN,            -- was Autopilot on
  alert_code        STRING,             -- only filled for alerts
  software_version  STRING,             -- car software version
  event_date        DATE,               -- extra: day of the event
  event_hour        TIMESTAMP_TZ,       -- extra: hour of the event
  is_moving         BOOLEAN,            -- extra: speed above 1 mph
  is_charging       BOOLEAN,            -- extra: charging right now
  battery_band      STRING              -- extra: critical, low, normal or high
);

-- Curated table 2: one row per car per hour.
CREATE TABLE IF NOT EXISTS CURATED.VEHICLE_HOURLY_METRICS (
  vin                   STRING NOT NULL,
  event_hour            TIMESTAMP_TZ NOT NULL,
  events                NUMBER,         -- how many events in this hour
  avg_speed_mph         FLOAT,
  max_speed_mph         FLOAT,
  avg_battery_soc       FLOAT,
  min_battery_soc       FLOAT,
  max_battery_temp_c    FLOAT,
  moving_events         NUMBER,
  charging_events       NUMBER,
  latest_odometer_miles FLOAT
);

-- Curated table 3: one row per car per day, only while the car was moving.
CREATE TABLE IF NOT EXISTS CURATED.TRIP_METRICS (
  vin                  STRING NOT NULL,
  event_date           DATE NOT NULL,
  first_event_ts       TIMESTAMP_TZ,    -- first moving event of the day
  last_event_ts        TIMESTAMP_TZ,    -- last moving event of the day
  start_odometer_miles FLOAT,
  end_odometer_miles   FLOAT,
  avg_speed_mph        FLOAT,
  max_speed_mph        FLOAT,
  autopilot_events     NUMBER,
  distance_miles       FLOAT            -- end odometer minus start odometer
);

-- Curated table 4: one row per car per day about the battery.
CREATE TABLE IF NOT EXISTS CURATED.BATTERY_HEALTH (
  vin                STRING NOT NULL,
  event_date         DATE NOT NULL,
  avg_battery_soc    FLOAT,
  min_battery_soc    FLOAT,
  avg_battery_temp_c FLOAT,
  max_battery_temp_c FLOAT,
  charge_events      NUMBER
);

-- Curated table 5: only alert events. LIKE copies all columns of TELEMETRY_ENRICHED.
CREATE TABLE IF NOT EXISTS CURATED.ALERTS LIKE CURATED.TELEMETRY_ENRICHED;

-- ---------------------------------------------------------------------
-- Staging tables: exact copies (same columns) of the 5 curated tables.
-- The pipeline empties them and fills them again on every run.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STAGING.TELEMETRY_ENRICHED LIKE CURATED.TELEMETRY_ENRICHED;
CREATE TABLE IF NOT EXISTS STAGING.VEHICLE_HOURLY_METRICS LIKE CURATED.VEHICLE_HOURLY_METRICS;
CREATE TABLE IF NOT EXISTS STAGING.TRIP_METRICS LIKE CURATED.TRIP_METRICS;
CREATE TABLE IF NOT EXISTS STAGING.BATTERY_HEALTH LIKE CURATED.BATTERY_HEALTH;
CREATE TABLE IF NOT EXISTS STAGING.ALERTS LIKE CURATED.ALERTS;
