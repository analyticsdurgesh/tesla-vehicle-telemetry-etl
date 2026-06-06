USE DATABASE TESLA_TELEMETRY;

CREATE OR REPLACE PROCEDURE RAW.MERGE_TELEMETRY_ENRICHED()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO CURATED.TELEMETRY_ENRICHED target
  USING STAGING.TELEMETRY_ENRICHED source
  ON target.event_id = source.event_id
  WHEN MATCHED THEN UPDATE SET
    vin = source.vin,
    event_ts = source.event_ts,
    ingest_ts = source.ingest_ts,
    event_type = source.event_type,
    latitude = source.latitude,
    longitude = source.longitude,
    speed_mph = source.speed_mph,
    battery_soc = source.battery_soc,
    battery_temp_c = source.battery_temp_c,
    odometer_miles = source.odometer_miles,
    charging_state = source.charging_state,
    gear = source.gear,
    autopilot_engaged = source.autopilot_engaged,
    alert_code = source.alert_code,
    software_version = source.software_version,
    event_date = source.event_date,
    event_hour = source.event_hour,
    is_moving = source.is_moving,
    is_charging = source.is_charging,
    battery_band = source.battery_band
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;
  RETURN 'merged telemetry_enriched';
END;
$$;

CREATE OR REPLACE PROCEDURE RAW.MERGE_VEHICLE_HOURLY_METRICS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO CURATED.VEHICLE_HOURLY_METRICS target
  USING STAGING.VEHICLE_HOURLY_METRICS source
  ON target.vin = source.vin AND target.event_hour = source.event_hour
  WHEN MATCHED THEN UPDATE SET
    events = source.events,
    avg_speed_mph = source.avg_speed_mph,
    max_speed_mph = source.max_speed_mph,
    avg_battery_soc = source.avg_battery_soc,
    min_battery_soc = source.min_battery_soc,
    max_battery_temp_c = source.max_battery_temp_c,
    moving_events = source.moving_events,
    charging_events = source.charging_events,
    latest_odometer_miles = source.latest_odometer_miles
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;
  RETURN 'merged vehicle_hourly_metrics';
END;
$$;

CREATE OR REPLACE PROCEDURE RAW.MERGE_TRIP_METRICS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO CURATED.TRIP_METRICS target
  USING STAGING.TRIP_METRICS source
  ON target.vin = source.vin AND target.event_date = source.event_date
  WHEN MATCHED THEN UPDATE SET
    first_event_ts = source.first_event_ts,
    last_event_ts = source.last_event_ts,
    start_odometer_miles = source.start_odometer_miles,
    end_odometer_miles = source.end_odometer_miles,
    avg_speed_mph = source.avg_speed_mph,
    max_speed_mph = source.max_speed_mph,
    autopilot_events = source.autopilot_events,
    distance_miles = source.distance_miles
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;
  RETURN 'merged trip_metrics';
END;
$$;

CREATE OR REPLACE PROCEDURE RAW.MERGE_BATTERY_HEALTH()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO CURATED.BATTERY_HEALTH target
  USING STAGING.BATTERY_HEALTH source
  ON target.vin = source.vin AND target.event_date = source.event_date
  WHEN MATCHED THEN UPDATE SET
    avg_battery_soc = source.avg_battery_soc,
    min_battery_soc = source.min_battery_soc,
    avg_battery_temp_c = source.avg_battery_temp_c,
    max_battery_temp_c = source.max_battery_temp_c,
    charge_events = source.charge_events
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;
  RETURN 'merged battery_health';
END;
$$;

CREATE OR REPLACE PROCEDURE RAW.MERGE_ALERTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO CURATED.ALERTS target
  USING STAGING.ALERTS source
  ON target.event_id = source.event_id
  WHEN MATCHED THEN UPDATE SET
    vin = source.vin,
    event_ts = source.event_ts,
    ingest_ts = source.ingest_ts,
    event_type = source.event_type,
    latitude = source.latitude,
    longitude = source.longitude,
    speed_mph = source.speed_mph,
    battery_soc = source.battery_soc,
    battery_temp_c = source.battery_temp_c,
    odometer_miles = source.odometer_miles,
    charging_state = source.charging_state,
    gear = source.gear,
    autopilot_engaged = source.autopilot_engaged,
    alert_code = source.alert_code,
    software_version = source.software_version,
    event_date = source.event_date,
    event_hour = source.event_hour,
    is_moving = source.is_moving,
    is_charging = source.is_charging,
    battery_band = source.battery_band
  WHEN NOT MATCHED THEN INSERT ALL BY NAME;
  RETURN 'merged alerts';
END;
$$;
