#!/usr/bin/env bash
# =====================================================================
# remove_pipeline.sh
# ONE script that removes everything create_pipeline.sh created.
#
# How to run it (from the project folder, in Terminal on Mac or Git Bash on Windows):
#   bash remove_pipeline.sh                keeps your logins (.env and the Snowflake key)
#   bash remove_pipeline.sh --everything   also deletes your logins and the Docker images
#
# What it does, step by step:
#   1. Deletes the Snowflake database and warehouse
#   2. Stops Airflow and deletes its containers and its database
#      (with --everything: also the downloaded Docker images)
#   3. Deletes the files the pipeline wrote on this computer
#   4. Deletes the Python virtual environment
#   5. (--everything only) Deletes .env and the Snowflake key
#   6. Prints what you delete by hand in the AWS console
#
# Your code, the sample data and the S3 bucket are NOT touched.
# =====================================================================

# Treat unknown variables as errors (-u), and fail a pipe (cmd1 | cmd2) if any part of it fails (pipefail).
# We do NOT use "-e" here: if one step fails, we still want to try the next steps.
set -uo pipefail

# Go to the folder of this script (the project folder).
# Stop if we are not there, so we never delete files in another folder.
cd "$(dirname "$0")" || exit 1
[ -f remove_pipeline.sh ] && [ -f docker-compose.yml ] || { echo "Run this script from the project folder with: bash remove_pipeline.sh"; exit 1; }

# Normal mode, or --everything mode?
# ${1:-} is the first word after the script name, or empty if there is none.
MODE="normal"
if [ "${1:-}" = "--everything" ]; then
  MODE="everything"
fi

# docker-compose.yml needs AIRFLOW_UID (your user id), so we set it here too.
export AIRFLOW_UID="$(id -u)"

# Print a step title.
step() {
  echo
  echo "=== STEP $1 of 6: $2 ==="
}

# Print one value from .env (empty if the file or the value is missing).
env_value() {
  if [ -f .env ]; then
    grep -E "^$1=" .env | head -n 1 | cut -d= -f2- | tr -d '\r'
  fi
}

# Path of Python inside the virtual environment (Mac/Linux: .venv/bin, Windows: .venv/Scripts).
VENV_PYTHON=".venv/bin/python"
if [ -f .venv/Scripts/python.exe ]; then
  VENV_PYTHON=".venv/Scripts/python.exe"
fi

# Read the names we print at the end now, because step 5 may delete .env.
SNOWFLAKE_USER_NAME="$(env_value SNOWFLAKE_USER)"
BUCKET_NAME="$(env_value S3_BUCKET)"

echo "Removing the Tesla telemetry pipeline (mode: $MODE)"

# ---------------------------------------------------------------------
step 1 "Delete the Snowflake database and warehouse"
# ---------------------------------------------------------------------
# To log in to Snowflake we need Python (-f: the file exists), the .env settings and the key.
if [ -f "$VENV_PYTHON" ] && [ -f .env ] && [ -f .secrets/snowflake_rsa_key.p8 ]; then
  # Run the SQL file that drops the database and the warehouse.
  if PYTHONPATH=src "$VENV_PYTHON" scripts/pipeline_helper.py run-sql snowflake/99_drop_everything.sql; then
    echo "Snowflake: database TESLA_TELEMETRY and warehouse TELEMETRY_WH deleted."
  else
    echo "Snowflake: could not delete. Paste snowflake/99_drop_everything.sql into a Snowsight worksheet and Run All."
  fi
else
  echo "Snowflake: skipped (no .venv, .env or key). Paste snowflake/99_drop_everything.sql into Snowsight to delete by hand."
fi

# ---------------------------------------------------------------------
step 2 "Stop Airflow and delete its containers and database"
# ---------------------------------------------------------------------
# Docker must be installed and running.
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  if [ "$MODE" = "everything" ]; then
    # down stops and deletes the containers. --volumes deletes Airflow's database and logs.
    # --remove-orphans also removes old containers of this project. --rmi all deletes the downloaded images.
    docker compose down --volumes --remove-orphans --rmi all
  else
    # Same, but the images stay, so the next start is fast.
    docker compose down --volumes --remove-orphans
  fi
else
  echo "Docker is not running, so this step was skipped. Open Docker Desktop and run this script again."
fi

# ---------------------------------------------------------------------
step 3 "Delete the files the pipeline wrote on this computer"
# ---------------------------------------------------------------------
# build: raw, clean and curated files from the pipeline runs. .pytest_cache: pytest's notes.
rm -rf build .pytest_cache
# find looks in all folders (except .venv) for folders named __pycache__ and deletes them.
# These folders hold Python's temporary compiled files.
find . -name "__pycache__" -type d -not -path "./.venv/*" -prune -exec rm -rf {} +
echo "Deleted build/, .pytest_cache/ and __pycache__/ folders."

# ---------------------------------------------------------------------
step 4 "Delete the Python virtual environment"
# ---------------------------------------------------------------------
rm -rf .venv
echo "Deleted .venv/"

# ---------------------------------------------------------------------
step 5 "Delete your login files (only with --everything)"
# ---------------------------------------------------------------------
if [ "$MODE" = "everything" ]; then
  # Delete the settings file and the key folder.
  rm -f .env
  rm -rf .secrets
  echo "Deleted .env and .secrets/"
  # Snowflake still knows the old public key. Show how to remove it (optional).
  echo "Optional: also remove the old public key from your Snowflake user."
  echo "Paste this in a Snowsight worksheet and Run All:"
  echo "  USE ROLE ACCOUNTADMIN;"
  echo "  ALTER USER ${SNOWFLAKE_USER_NAME:-<your_user>} UNSET RSA_PUBLIC_KEY;"
else
  echo "Kept .env and .secrets/ (your logins). Run with --everything to delete them too."
fi

# ---------------------------------------------------------------------
step 6 "Things you delete by hand in the AWS console"
# ---------------------------------------------------------------------
# The AWS key in .env can only read files, so this script cannot delete AWS things.
echo "Only if you want to remove the AWS part too:"
echo "  1. S3 > select ${BUCKET_NAME:-your bucket} > Empty > type: permanently delete > Empty."
echo "     Empty also deletes all old versions (versioning is on), so the bucket can then be deleted."
echo "  2. S3 > select the bucket > Delete > type the bucket name > Delete bucket."
echo "  3. IAM > Users > tesla-telemetry-etl > Security credentials > Access keys > Actions > Deactivate, then Delete."
echo "  4. IAM > Users > select tesla-telemetry-etl > Delete."

# Final message.
echo
echo "============================================================"
echo " DONE. Build it again with: bash create_pipeline.sh"
echo "============================================================"
