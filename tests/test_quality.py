# =====================================================================
# test_quality.py
# Job of this file: prove that the quality checks in quality.py work.
# Run all tests with:  .venv/bin/python -m pytest
# Each function whose name starts with "test_" is one test. pytest finds and runs them.
# "assert" means "this must be true, otherwise the test fails".
# =====================================================================

# The function we want to test.
from telemetry_etl.quality import check_records


# A helper (not a test, because its name does not start with "test_").
# "**changes" collects any named values we pass in, for example battery_soc=120.
def good_event(**changes) -> dict:
    # One correct event. Every test starts from this.
    event = {
        "event_id": "evt_test_001",
        "vin": "5YJ3E1EA7KF000001",
        "event_ts": "2026-06-06T08:00:00Z",
        "ingest_ts": "2026-06-06T08:00:10Z",
        "event_type": "telemetry",
        "latitude": 37.7749,
        "longitude": -122.4194,
        "speed_mph": 10.0,
        "battery_soc": 80.0,
        "battery_temp_c": 28.0,
        "odometer_miles": 1000.0,
        "charging_state": "Disconnected",
        "gear": "D",
        "autopilot_engaged": False,
        "alert_code": None,
        "software_version": "2026.14.3",
    }
    # Apply the changes a test asks for, for example battery_soc=120.
    event.update(changes)
    return event


def test_good_event_passes():
    # A correct event gives no problems and one clean record.
    clean_records, problems = check_records([good_event()])
    assert problems == []
    assert len(clean_records) == 1


def test_battery_above_100_is_rejected():
    # Battery percent cannot be 120, so the schema check must complain about battery_soc.
    clean_records, problems = check_records([good_event(battery_soc=120)])
    assert len(problems) == 1
    assert "battery_soc" in problems[0]


def test_duplicate_event_id_is_rejected():
    # The same event twice in one batch is a problem.
    clean_records, problems = check_records([good_event(), good_event()])
    # any(...) is True if at least one problem message contains the word "duplicate".
    assert any("duplicate" in problem for problem in problems)


def test_alert_without_code_is_rejected():
    # An alert event must have an alert_code.
    clean_records, problems = check_records([good_event(event_type="alert", alert_code=None)])
    assert any("alert_code" in problem for problem in problems)


def test_time_without_time_zone_is_rejected():
    # "2026-06-06T08:00:00" has no time zone (no Z at the end), so it is not accepted.
    clean_records, problems = check_records([good_event(event_ts="2026-06-06T08:00:00")])
    assert any("event_ts" in problem for problem in problems)
