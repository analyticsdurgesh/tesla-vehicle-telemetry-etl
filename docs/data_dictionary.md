# Data Dictionary

## Raw Telemetry Event

| Field | Type | Description |
| --- | --- | --- |
| `event_id` | string | Unique telemetry event identifier. |
| `vin` | string | Vehicle Identification Number. |
| `event_ts` | timestamp | Time the vehicle event occurred. |
| `ingest_ts` | timestamp | Time the event was ingested. |
| `event_type` | string | One of `telemetry`, `charge`, or `alert`. |
| `latitude` | float | Vehicle latitude, -90 to 90. |
| `longitude` | float | Vehicle longitude, -180 to 180. |
| `speed_mph` | float | Vehicle speed in miles per hour. |
| `battery_soc` | float | Battery state of charge percentage. |
| `battery_temp_c` | float | Battery temperature in Celsius. |
| `odometer_miles` | float | Odometer reading in miles. |
| `charging_state` | string | Charging status such as `Charging` or `Disconnected`. |
| `gear` | string | Vehicle gear state. |
| `autopilot_engaged` | boolean | Whether assisted driving was engaged. |
| `alert_code` | string | Alert code for alert events. |
| `software_version` | string | Vehicle software version. |

## Curated Tables

### `CURATED.TELEMETRY_ENRICHED`

Raw event fields plus:

- `event_date`
- `event_hour`
- `is_moving`
- `is_charging`
- `battery_band`

### `CURATED.VEHICLE_HOURLY_METRICS`

Hourly rollups per VIN:

- Event count.
- Average and max speed.
- Average and minimum battery state of charge.
- Maximum battery temperature.
- Moving and charging event counts.
- Latest odometer reading.

### `CURATED.TRIP_METRICS`

Daily moving-event metrics per VIN:

- First and last trip event timestamps.
- Start and end odometer.
- Distance.
- Average and max speed.
- Autopilot event count.

### `CURATED.BATTERY_HEALTH`

Daily battery health metrics per VIN:

- Average and minimum battery state of charge.
- Average and maximum battery temperature.
- Charge event count.

### `CURATED.ALERTS`

Filtered enriched telemetry records where `event_type = 'alert'`.
