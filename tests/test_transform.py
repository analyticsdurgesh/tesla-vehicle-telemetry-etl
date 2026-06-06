from telemetry_etl.quality import validate_jsonl_file
from telemetry_etl.transform import build_curated_frames


def test_build_curated_frames_from_sample():
    # The sample file should pass validation before transformation starts.
    result = validate_jsonl_file("data/sample/tesla_telemetry_sample.jsonl")

    # Build all curated datasets from the validated sample records.
    frames = build_curated_frames(result.valid_records)

    assert result.passed
    # The enriched table should keep every valid input event.
    assert len(frames["telemetry_enriched"]) == 6
    # The transform should always return the complete curated model set.
    assert set(frames) == {
        "telemetry_enriched",
        "vehicle_hourly_metrics",
        "trip_metrics",
        "battery_health",
        "alerts",
    }
    # The sample data includes exactly one alert event.
    assert len(frames["alerts"]) == 1


def test_trip_metrics_calculates_distance():
    # Trip metrics are calculated from moving vehicle events in the sample file.
    result = validate_jsonl_file("data/sample/tesla_telemetry_sample.jsonl")

    trip_metrics = build_curated_frames(result.valid_records)["trip_metrics"]

    # Distance should be present and positive when odometer readings increase.
    assert "distance_miles" in trip_metrics.columns
    assert trip_metrics["distance_miles"].max() > 0
