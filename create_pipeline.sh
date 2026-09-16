#!/usr/bin/env bash
# =====================================================================
# create_pipeline.sh
# ONE script that builds the whole project and runs the pipeline once.
#
# How to run it (from the project folder, in Terminal on Mac or Git Bash on Windows):
#   bash create_pipeline.sh
#
# What it does, step by step:
#   1. Checks that Python, Docker and openssl are installed
#   2. Checks the .env file (your private settings)
#   3. Creates the Python virtual environment and installs the libraries
#   4. Creates the Snowflake login key (only the first time)
#   5. Checks that Snowflake accepts the key
#   6. Runs the unit tests
#   7. Checks that the raw file is in S3
#   8. Creates the Snowflake warehouse, database, tables and procedure
#   9. Starts Airflow in Docker
#  10. Runs the Airflow DAG once and waits for it to finish
#  11. Prints the results from Snowflake
#
# It is safe to run again. Things that already exist are kept or updated.
# =====================================================================

# Stop at the first error (-e), treat unknown variables as errors (-u),
# and fail a pipe (cmd1 | cmd2) if any part of it fails (pipefail).
set -euo pipefail

# $0 is this script's path, and dirname keeps only its folder.
# Go to that folder (the project folder), wherever you start the script from.
cd "$(dirname "$0")"

# ---------------------------------------------------------------------
# Names we use many times
# ---------------------------------------------------------------------
# Name of our Airflow DAG (see dags/tesla_telemetry_dag.py).
DAG_ID="tesla_telemetry_etl"
# Airflow web page. If you change the port in docker-compose.yml, change it here too.
AIRFLOW_URL="http://localhost:8090"
# Private key for Snowflake.
KEY_FILE=".secrets/snowflake_rsa_key.p8"

# docker-compose.yml reads AIRFLOW_UID. Setting it to your user id (id -u)
# makes Airflow create files that belong to you.
export AIRFLOW_UID="$(id -u)"

# ---------------------------------------------------------------------
# Small helper functions
# ---------------------------------------------------------------------

# Print a step title, for example: === STEP 3 of 11: ... ===
# $1 is the first word given to the function, $2 the second.
step() {
  echo
  echo "=== STEP $1 of 11: $2 ==="
}

# Print a message and stop the script with an error code (exit 1).
fail() {
  echo
  echo "STOPPED: $1"
  exit 1
}

# Print the path of Python inside our virtual environment.
# Mac and Linux keep it in .venv/bin, Windows keeps it in .venv/Scripts.
venv_python() {
  if [ -f .venv/Scripts/python.exe ]; then
    echo ".venv/Scripts/python.exe"
  else
    echo ".venv/bin/python"
  fi
}

# Run our Python helper. PYTHONPATH=src tells Python to look for our code in the src folder.
# "$@" passes on all the words given to this function, for example: helper check-s3
helper() {
  PYTHONPATH=src "$(venv_python)" scripts/pipeline_helper.py "$@"
}

# Run an Airflow command inside the scheduler container.
# -T means "no keyboard input needed". 2>/dev/null hides Airflow's warning messages.
airflow_cli() {
  docker compose exec -T airflow-scheduler airflow "$@" 2>/dev/null
}

# Print one value from the .env file. Example: env_value S3_BUCKET
env_value() {
  # grep finds the line that starts with NAME=, head keeps the first one,
  # cut keeps the text after the first "=", and tr removes Windows line-end characters.
  # "|| true" means: if the name is missing, print nothing instead of stopping the script.
  grep -E "^$1=" .env | head -n 1 | cut -d= -f2- | tr -d '\r' || true
}

# ---------------------------------------------------------------------
step 1 "Check the tools on this computer"
# ---------------------------------------------------------------------
# Use the first Python we find, starting with 3.11 (the version we tested).
# "python" and "py" (the Python launcher) are the names on Windows.
# We keep a Python only if it really runs and is 3.10 or newer.
# (The Python that comes with macOS is 3.9. It is too old for our libraries.)
PYTHON=""
for candidate in python3.11 python3.12 python3.13 python3 python py; do
  if "$candidate" -c 'import sys; sys.exit(sys.version_info < (3, 10))' >/dev/null 2>&1; then
    PYTHON="$candidate"
    break
  fi
done
# If no good Python was found (the name is still empty), stop.
[ -n "$PYTHON" ] || fail "Python 3.10 or newer was not found. Please install Python 3.11."
# "command -v" checks that a program exists. Docker must be installed...
command -v docker >/dev/null 2>&1 || fail "Docker is not installed. Please install Docker Desktop."
# ...and running ("docker info" only works when Docker Desktop is open).
docker info >/dev/null 2>&1 || fail "Docker is not running. Open Docker Desktop, wait until it is ready, then run again."
# openssl creates the Snowflake key.
command -v openssl >/dev/null 2>&1 || fail "openssl is not installed."
# Show which versions we found.
echo "Python : $("$PYTHON" --version)"
echo "Docker : $(docker --version)"

# ---------------------------------------------------------------------
step 2 "Check the .env settings file"
# ---------------------------------------------------------------------
# First run: make .env from the template, make it private, and ask the user to fill it in.
if [ ! -f .env ]; then
  cp .env.example .env
  chmod 600 .env
  fail "I created the file .env for you. Open it, fill in your AWS and Snowflake values, then run this script again."
fi
# Every value must be filled in (not empty and not "replace_me").
for name in AWS_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY S3_BUCKET SNOWFLAKE_ACCOUNT SNOWFLAKE_USER; do
  value="$(env_value "$name")"
  if [ -z "$value" ] || [ "$value" = "replace_me" ]; then
    fail "$name is not filled in inside .env. Fill it in and run again."
  fi
done
# Only you may read the file (chmod 600), because it holds secrets.
chmod 600 .env
echo ".env is filled in."

# ---------------------------------------------------------------------
step 3 "Create the Python virtual environment and install the libraries"
# ---------------------------------------------------------------------
# A virtual environment is a private Python folder (.venv) just for this project.
# Create it if it is missing or broken (a working .venv can run "python -m pip").
if ! "$(venv_python)" -m pip --version >/dev/null 2>&1; then
  # Delete a half-made .venv from an earlier failed try.
  rm -rf .venv
  if "$PYTHON" -m venv .venv; then
    # Normal case (Mac, Windows, most Linux): the venv comes with pip.
    echo "Created .venv"
  else
    # Ubuntu and WSL often ship Python without "ensurepip", the part that puts pip into a new venv.
    # Then we make the venv without pip, and download pip from its official site (bootstrap.pypa.io).
    echo "Python could not add pip to .venv. Trying again and downloading pip instead..."
    rm -rf .venv
    if "$PYTHON" -m venv --without-pip .venv && curl -sSfL https://bootstrap.pypa.io/get-pip.py | "$(venv_python)" - --quiet; then
      echo "Created .venv (pip was downloaded)"
    else
      # Both ways failed. Delete the half-made folder and show the one-time fix for Ubuntu/WSL.
      rm -rf .venv
      # The package name has the Python version in it, for example python3.12-venv.
      VENV_PACKAGE="$("$PYTHON" -c 'import sys; print("python%d.%d-venv" % sys.version_info[:2])' || echo python3-venv)"
      fail "Python could not create .venv. On Ubuntu or WSL run: sudo apt update && sudo apt install -y $VENV_PACKAGE   Then run this script again."
    fi
  fi
fi
# Update pip (the tool that installs libraries) to its newest version.
"$(venv_python)" -m pip install --quiet --upgrade pip || fail "Could not update pip. Check your internet connection."
# Install the exact library versions from requirements.txt.
"$(venv_python)" -m pip install --quiet -r requirements.txt || fail "Could not install the libraries. Check your internet connection."
echo "Libraries installed."

# ---------------------------------------------------------------------
step 4 "Create the Snowflake login key (only the first time)"
# ---------------------------------------------------------------------
# A key pair has two parts:
#   private key  stays on this computer (.secrets/snowflake_rsa_key.p8)
#   public key   is given to Snowflake, so Snowflake can recognise us
if [ ! -f "$KEY_FILE" ]; then
  # Make the .secrets folder. chmod 700 means only you can open it.
  mkdir -p .secrets
  chmod 700 .secrets
  # Create a new 2048-bit RSA private key in the PKCS#8 format that Snowflake expects.
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$KEY_FILE" 2>/dev/null
  # chmod 600 means only you can read the key.
  chmod 600 "$KEY_FILE"
  echo "Created a new private key: $KEY_FILE"
else
  echo "Private key already exists: $KEY_FILE"
fi
# Make the public key from the private key, as one line without the BEGIN/END lines.
# tr removes the line breaks. On Windows, openssl ends each line with \r\n, so we remove \r too.
PUBLIC_KEY="$(openssl rsa -in "$KEY_FILE" -pubout 2>/dev/null | grep -v -- '-----' | tr -d '\r\n')"

# ---------------------------------------------------------------------
step 5 "Check that Snowflake accepts our key"
# ---------------------------------------------------------------------
if ! helper check-snowflake; then
  # The usual reason: the public key is not registered in Snowflake yet.
  echo
  echo "Snowflake does not accept the key yet. Do this once:"
  echo "  1. Open Snowsight (the Snowflake web page) and create a new SQL worksheet."
  echo "  2. Paste the 2 lines below."
  echo "  3. Click the arrow next to Run and choose 'Run All' (Cmd+Shift+Enter on Mac, Ctrl+Shift+Enter on Windows)."
  echo
  echo "USE ROLE ACCOUNTADMIN;"
  echo "ALTER USER $(env_value SNOWFLAKE_USER) SET RSA_PUBLIC_KEY='$PUBLIC_KEY';"
  echo
  # "-t 0" is true when a person is typing in this terminal. Then we wait for Enter.
  # When the script runs without a person (for example from another program), we stop.
  if [ -t 0 ]; then
    read -r -p "Press Enter after you ran the SQL in Snowsight... " _
    helper check-snowflake || fail "Snowflake still does not accept the key. Check the SQL ran without errors, then run this script again."
  else
    fail "Register the key in Snowsight, then run this script again."
  fi
fi

# ---------------------------------------------------------------------
step 6 "Run the unit tests"
# ---------------------------------------------------------------------
# pytest runs every test in the tests folder. -q means short output.
"$(venv_python)" -m pytest -q || fail "A unit test failed. Read the test output above."

# ---------------------------------------------------------------------
step 7 "Check that the raw file is in S3"
# ---------------------------------------------------------------------
helper check-s3 || fail "Upload the telemetry folder to your S3 bucket (see docs/SETUP_GUIDE.md), then run again."

# ---------------------------------------------------------------------
step 8 "Create the Snowflake objects"
# ---------------------------------------------------------------------
# The 3 files run in order: warehouse, database and schemas first, then tables, then the procedure.
# The "\" at the end of a line means the command goes on in the next line.
helper run-sql \
  snowflake/01_create_database.sql \
  snowflake/02_create_tables.sql \
  snowflake/03_create_merge_procedure.sql || fail "A Snowflake SQL file failed. Read the error above."

# ---------------------------------------------------------------------
step 9 "Start Airflow in Docker"
# ---------------------------------------------------------------------
# -d means "run in the background". The first time, Docker downloads the images (about 2.6 GB).
docker compose up -d || fail "Docker could not start Airflow. Read the error above."
# Wait until the web page answers: 60 tries, 5 seconds apart = up to 5 minutes.
# curl asks the page for its health status. -s = silent, -o /dev/null = throw the answer away,
# -f = count an error page as a failure.
echo "Waiting for Airflow to start..."
for attempt in $(seq 1 60); do
  if curl -s -o /dev/null -f "$AIRFLOW_URL/health"; then
    break
  fi
  sleep 5
done
# Ask one last time. If the page still does not answer, stop.
curl -s -o /dev/null -f "$AIRFLOW_URL/health" || fail "Airflow did not start. Look at: docker compose logs airflow-webserver"
echo "Airflow is running at $AIRFLOW_URL"

# ---------------------------------------------------------------------
step 10 "Run the pipeline once"
# ---------------------------------------------------------------------
# The scheduler needs a few seconds to read our DAG file.
# Ask Airflow for its list of DAGs every 3 seconds (up to 2 minutes) until our DAG is in it.
echo "Waiting for Airflow to load the DAG..."
for attempt in $(seq 1 40); do
  # "|| true" keeps the script going if Airflow is not ready to answer yet.
  dag_list="$(airflow_cli dags list -o plain || true)"
  # *"text"* means "contains text".
  if [[ "$dag_list" == *"$DAG_ID"* ]]; then
    break
  fi
  sleep 3
done
# If the DAG is still not in the list, stop.
[[ "$dag_list" == *"$DAG_ID"* ]] || fail "Airflow did not load the DAG. Look at: docker compose logs airflow-scheduler"

# Give this run its own name, with the date and time, for example run_20260916_211339.
RUN_ID="run_$(date +%Y%m%d_%H%M%S)"
# A new DAG starts paused (switched off). Switch it on.
airflow_cli dags unpause "$DAG_ID" >/dev/null || fail "Airflow could not switch on the DAG. Wait one minute and run this script again."
# Start one run of the DAG.
airflow_cli dags trigger "$DAG_ID" --run-id "$RUN_ID" >/dev/null || fail "Airflow could not start the DAG run. Wait one minute and run this script again."
echo "Started DAG run: $RUN_ID (watch it at $AIRFLOW_URL)"

# Check the run state every 5 seconds, for up to 10 minutes (120 tries).
# We start with "queued", the state a new run has before it begins.
STATE="queued"
for attempt in $(seq 1 120); do
  # list-runs prints one line per run. awk finds the line where column 2 is our run id
  # and prints column 3, the state (queued, running, success or failed).
  STATE="$(airflow_cli dags list-runs -d "$DAG_ID" -o plain | awk -v id="$RUN_ID" '$2 == id {print $3}' || true)"
  echo "  state: ${STATE:-starting}"
  # Stop checking when the run has finished.
  if [ "$STATE" = "success" ] || [ "$STATE" = "failed" ]; then
    break
  fi
  sleep 5
done

# Print the state of each of the 4 tasks.
# awk prints column 3 (task name) and column 4 (state) of the header line (NR == 1) and of our task lines.
echo
airflow_cli tasks states-for-dag-run "$DAG_ID" "$RUN_ID" -o plain | awk 'NR == 1 || /tesla/ {print $3, $4}' || true
# The run must have ended with success.
[ "$STATE" = "success" ] || fail "The DAG run did not succeed (state: ${STATE:-unknown}). Open $AIRFLOW_URL, click the red task and read its log."

# ---------------------------------------------------------------------
step 11 "Show the results in Snowflake"
# ---------------------------------------------------------------------
helper show-results || fail "Could not read the results from Snowflake."

# Final summary.
echo
echo "============================================================"
echo " DONE. The pipeline ran end to end."
echo " Airflow web page : $AIRFLOW_URL  (login: airflow / airflow)"
echo " Snowflake        : database TESLA_TELEMETRY, schema CURATED"
echo " Stop Airflow     : docker compose down"
echo " Remove all       : bash remove_pipeline.sh"
echo "============================================================"
