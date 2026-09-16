# =====================================================================
# extract.py
# Job of this file: the "E" in ETL (Extract).
# It finds the raw telemetry files in Amazon S3 and downloads them.
#
# The files sit in S3 under folders by date and hour, for example:
#   telemetry/year=2026/month=06/day=06/hour=08/events.jsonl
# =====================================================================

# shutil can delete a whole folder with everything inside it.
import shutil

# Path helps us work with file and folder paths.
from pathlib import Path

# boto3 is the official Python library for AWS.
import boto3

# S3_PREFIX is the folder in the bucket we read from ("telemetry/").
# Settings is the box with our private settings (bucket name, region, ...).
from telemetry_etl.config import S3_PREFIX, Settings


# List the JSONL files in the bucket. The result is a list with one dictionary per file.
def list_s3_files(settings: Settings) -> list[dict]:
    # Create an S3 client. It uses the keys from the .env file automatically.
    s3 = boto3.client("s3", region_name=settings.aws_region)
    # S3 returns at most 1000 files per call. A paginator asks again until all files are listed.
    paginator = s3.get_paginator("list_objects_v2")
    # Ask for all files whose name starts with "telemetry/".
    pages = paginator.paginate(Bucket=settings.s3_bucket, Prefix=S3_PREFIX)

    # This list will hold the files we care about.
    files = []
    # Go through every page of results.
    for page in pages:
        # "Contents" is the list of files on this page. An empty page has no "Contents".
        for item in page.get("Contents", []):
            # We only want JSONL files. Skip folders and other file types.
            if not item["Key"].endswith(".jsonl"):
                continue
            # Save three facts about the file:
            #   bucket: where it is
            #   key:    its full path inside the bucket
            #   etag:   a fingerprint of the file content (it changes when the content changes).
            #           S3 sends it inside double quotes, so strip('"') removes them.
            files.append(
                {
                    "bucket": settings.s3_bucket,
                    "key": item["Key"],
                    "etag": item["ETag"].strip('"'),
                }
            )
    # Give back the list of JSONL files.
    return files


# Download the given files into a local folder. The result is the same list, with local paths added.
def download_files(settings: Settings, files: list[dict], folder: Path) -> list[dict]:
    # Start with an empty folder, so files from an older run are not mixed in.
    if folder.exists():
        shutil.rmtree(folder)
    # Create the folder (and its parent folders if needed).
    folder.mkdir(parents=True)

    # Create an S3 client, same as above.
    s3 = boto3.client("s3", region_name=settings.aws_region)
    # Download the files one by one. number counts 1, 2, 3 ...
    for number, file in enumerate(files, start=1):
        # Many files have the same name (events.jsonl) in different S3 folders.
        # We add a number in front so they do not overwrite each other: 001_events.jsonl
        # {number:03d} writes the number with 3 digits (1 -> 001).
        # Path(...).name keeps only the file name from the S3 key (events.jsonl).
        local_path = folder / f"{number:03d}_{Path(file['key']).name}"
        # Copy the file from S3 to our computer.
        s3.download_file(file["bucket"], file["key"], str(local_path))
        # Remember where we saved it. The next step reads the file from here.
        file["local_path"] = str(local_path)
    # Give back the same list, now with local_path filled in.
    return files
