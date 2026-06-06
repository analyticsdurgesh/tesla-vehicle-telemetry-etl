USE DATABASE TESLA_TELEMETRY;

CREATE TABLE IF NOT EXISTS RAW.PROCESSED_S3_OBJECTS (
  bucket_name STRING NOT NULL,
  object_key STRING NOT NULL,
  etag STRING NOT NULL,
  processed_at TIMESTAMP_LTZ NOT NULL,
  CONSTRAINT processed_s3_objects_unique UNIQUE (bucket_name, object_key, etag)
);

CREATE TABLE IF NOT EXISTS CURATED.TELEMETRY_ENRICHED (
  event_id STRING NOT NULL,
  vin STRING NOT NULL,
  event_ts TIMESTAMP_TZ NOT NULL,
  ingest_ts TIMESTAMP_TZ NOT NULL,
  event_type STRING NOT NULL,
  latitude FLOAT,
  longitude FLOAT,
  speed_mph FLOAT,
  battery_soc FLOAT,
  battery_temp_c FLOAT,
  odometer_miles FLOAT,
  charging_state STRING,
  gear STRING,
  autopilot_engaged BOOLEAN,
  alert_code STRING,
  software_version STRING,
  event_date DATE,
  event_hour TIMESTAMP_TZ,
  is_moving BOOLEAN,
  is_charging BOOLEAN,
  battery_band STRING
);

CREATE TABLE IF NOT EXISTS STAGING.TELEMETRY_ENRICHED LIKE CURATED.TELEMETRY_ENRICHED;

CREATE TABLE IF NOT EXISTS CURATED.VEHICLE_HOURLY_METRICS (
  vin STRING NOT NULL,
  event_hour TIMESTAMP_TZ NOT NULL,
  events NUMBER,
  avg_speed_mph FLOAT,
  max_speed_mph FLOAT,
  avg_battery_soc FLOAT,
  min_battery_soc FLOAT,
  max_battery_temp_c FLOAT,
  moving_events NUMBER,
  charging_events NUMBER,
  latest_odometer_miles FLOAT
);

CREATE TABLE IF NOT EXISTS STAGING.VEHICLE_HOURLY_METRICS LIKE CURATED.VEHICLE_HOURLY_METRICS;

CREATE TABLE IF NOT EXISTS CURATED.TRIP_METRICS (
  vin STRING NOT NULL,
  event_date DATE NOT NULL,
  first_event_ts TIMESTAMP_TZ,
  last_event_ts TIMESTAMP_TZ,
  start_odometer_miles FLOAT,
  end_odometer_miles FLOAT,
  avg_speed_mph FLOAT,
  max_speed_mph FLOAT,
  autopilot_events NUMBER,
  distance_miles FLOAT
);

CREATE TABLE IF NOT EXISTS STAGING.TRIP_METRICS LIKE CURATED.TRIP_METRICS;

CREATE TABLE IF NOT EXISTS CURATED.BATTERY_HEALTH (
  vin STRING NOT NULL,
  event_date DATE NOT NULL,
  avg_battery_soc FLOAT,
  min_battery_soc FLOAT,
  avg_battery_temp_c FLOAT,
  max_battery_temp_c FLOAT,
  charge_events NUMBER
);

CREATE TABLE IF NOT EXISTS STAGING.BATTERY_HEALTH LIKE CURATED.BATTERY_HEALTH;

CREATE TABLE IF NOT EXISTS CURATED.ALERTS LIKE CURATED.TELEMETRY_ENRICHED;
CREATE TABLE IF NOT EXISTS STAGING.ALERTS LIKE CURATED.ALERTS;
