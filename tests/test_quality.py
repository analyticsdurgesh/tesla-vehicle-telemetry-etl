from telemetry_etl.quality import validate_records


def valid_record(**overrides):
    # Start from one clean event so tests can change only the field they care about.
    record = {
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
    # Overrides make it easy to create invalid records for negative tests.
    record.update(overrides)
    return record


def test_validate_records_accepts_valid_payload():
    # A complete and realistic record should pass all checks.
    result = validate_records([valid_record()])

    assert result.passed
    assert len(result.valid_records) == 1


def test_validate_records_rejects_invalid_battery_soc():
    # Battery state of charge cannot be above 100 percent.
    result = validate_records([valid_record(battery_soc=120)])

    assert not result.passed
    assert result.issues[0].rule == "schema"


def test_validate_records_flags_duplicate_event_ids():
    # Duplicate event IDs can break merge logic, so the batch should fail.
    result = validate_records([valid_record(), valid_record()])

    assert not result.passed
    assert any(issue.rule == "duplicate_event" for issue in result.issues)


def test_alert_requires_alert_code():
    # Alert events must include an alert code so operators know what happened.
    result = validate_records([valid_record(event_type="alert", alert_code=None)])

    assert not result.passed
    assert any(issue.rule == "alert_code" for issue in result.issues)
