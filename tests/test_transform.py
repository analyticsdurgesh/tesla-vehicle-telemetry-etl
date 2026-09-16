# =====================================================================
# test_transform.py
# Job of this file: prove that transform.py builds the right tables
# from the sample file in data/sample.
# =====================================================================

# The project folder, so we can find the sample file from anywhere.
from telemetry_etl.config import PROJECT_ROOT

# The functions we want to test.
from telemetry_etl.quality import check_records, read_jsonl
from telemetry_etl.transform import battery_band, build_tables

# Path of the sample file with 6 events.
SAMPLE_FILE = PROJECT_ROOT / "data" / "sample" / "tesla_telemetry_sample.jsonl"


# A helper (not a test): read the sample file, check it, and build the tables.
def build_sample_tables():
    clean_records, problems = check_records(read_jsonl(SAMPLE_FILE))
    # The sample file is correct, so there must be no problems.
    assert problems == []
    return build_tables(clean_records)


def test_sample_file_gives_five_tables():
    tables = build_sample_tables()
    # We always get the same 5 table names.
    assert set(tables) == {
        "telemetry_enriched",
        "vehicle_hourly_metrics",
        "trip_metrics",
        "battery_health",
        "alerts",
    }
    # All 6 events are kept.
    assert len(tables["telemetry_enriched"]) == 6
    # 2 cars: one has events in 2 different hours, so there are 3 hourly rows.
    assert len(tables["vehicle_hourly_metrics"]) == 3
    # The sample has exactly 1 alert event.
    assert len(tables["alerts"]) == 1


def test_trip_distance_is_positive():
    # The cars in the sample drive, so the distance must be above 0.
    trips = build_sample_tables()["trip_metrics"]
    assert trips["distance_miles"].max() > 0


def test_times_with_and_without_parts_of_a_second():
    # One time has milliseconds (.250) and one does not. Both must work together.
    records = read_jsonl(SAMPLE_FILE)
    records[1]["event_ts"] = "2026-06-06T08:10:00.250Z"
    clean_records, problems = check_records(records)
    assert problems == []
    assert len(build_tables(clean_records)["telemetry_enriched"]) == 6


def test_battery_band_groups():
    # Check each battery group, including the edges 20, 50 and 80.
    assert battery_band(15) == "critical"
    assert battery_band(20) == "critical"
    assert battery_band(45) == "low"
    assert battery_band(50) == "low"
    assert battery_band(51) == "normal"
    assert battery_band(80) == "normal"
    assert battery_band(95) == "high"
