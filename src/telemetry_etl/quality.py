# =====================================================================
# quality.py
# Job of this file: read JSONL files and check the quality of the events.
#
# JSONL means "JSON Lines": every line of the file is one JSON object.
# We check each event in two ways:
#   1. Schema check: does the event match TelemetryEvent in schemas.py?
#   2. Business checks: rules that need common sense, for example
#      "an event cannot happen after we received it".
# If any check fails, we collect a problem message.
# All the events we check together in one pipeline run are called a "batch".
# =====================================================================

# json turns a line of text into a Python dictionary, and back.
import json

# Path helps us work with file paths.
from pathlib import Path

# ValidationError is the error pydantic raises when an event breaks the schema.
from pydantic import ValidationError

# The description of one event (see schemas.py).
from telemetry_etl.schemas import TelemetryEvent


# Read a JSONL file. "-> list[dict]" means the result is a list of dictionaries (one per event).
def read_jsonl(file_path) -> list[dict]:
    # This list will hold one dictionary per line of the file.
    records = []
    # Open the file for reading as text.
    # "with" closes the file for us when the block ends, even after an error.
    with open(file_path, encoding="utf-8") as file:
        # Go through the file line by line. line_number starts at 1.
        for line_number, line in enumerate(file, start=1):
            # Skip empty lines.
            if line.strip() == "":
                continue
            # Try to turn the text into a dictionary.
            try:
                records.append(json.loads(line))
            # If the line is not valid JSON, stop and say which line is broken.
            except json.JSONDecodeError as error:
                raise ValueError(f"{file_path} line {line_number} is not valid JSON: {error}")
    # Give back all the events we read.
    return records


# Write a list of events to a JSONL file and give back the file path.
def write_jsonl(records: list[dict], file_path: Path) -> Path:
    # Make sure the folder for the file exists.
    file_path.parent.mkdir(parents=True, exist_ok=True)
    # Open the file for writing ("w"). This replaces any old file with the same name.
    with open(file_path, "w", encoding="utf-8") as file:
        # Write each event as one line of JSON.
        for record in records:
            file.write(json.dumps(record) + "\n")
    # Give back the path so the next step knows where the file is.
    return file_path


# Check a batch of events.
# The result is a pair (a "tuple") of two lists: the clean events, and the problem messages.
def check_records(records: list[dict]) -> tuple[list[dict], list[str]]:
    # Events that passed the schema check go into this list.
    clean_records = []
    # Every problem we find is written into this list as a sentence.
    problems = []
    # Event ids we have already seen, so we can find duplicates.
    seen_event_ids = set()

    # Check the events one by one.
    for record in records:
        # Take the event id for our messages. Use "unknown" if it is missing.
        event_id = record.get("event_id", "unknown")

        # Check 1: the schema. Pydantic compares the event with TelemetryEvent.
        try:
            event = TelemetryEvent.model_validate(record)
        except ValidationError as error:
            # Take the first mistake pydantic found.
            first_error = error.errors()[0]
            # "loc" tells us which field is wrong, for example battery_soc.
            field_name = ".".join(str(part) for part in first_error["loc"])
            # Save a readable problem message.
            problems.append(f"{event_id}: field '{field_name}' is wrong: {first_error['msg']}")
            # Skip the other checks for this broken event and go to the next event.
            continue

        # Check 2: the same event id must not appear twice in one batch.
        if event.event_id in seen_event_ids:
            problems.append(f"{event.event_id}: duplicate event_id in this batch")
        # Remember this id for the next events.
        seen_event_ids.add(event.event_id)

        # Check 3: an event cannot happen after we received it.
        if event.event_ts > event.ingest_ts:
            problems.append(f"{event.event_id}: event_ts is later than ingest_ts")

        # Check 4: a car with a speed above 0 must have a real odometer reading (not 0).
        if event.speed_mph > 0 and event.odometer_miles == 0:
            problems.append(f"{event.event_id}: speed is above 0 but odometer is 0")

        # Check 5: an alert event must say which alert it is.
        if event.event_type == "alert" and not event.alert_code:
            problems.append(f"{event.event_id}: alert event has no alert_code")

        # Keep the checked event as a plain dictionary.
        # mode="json" turns dates into text like "2026-06-06T08:00:00Z" so we can save it.
        clean_records.append(event.model_dump(mode="json"))

    # Give back the clean events and the list of problems.
    # If problems is empty, the batch is good.
    return clean_records, problems
