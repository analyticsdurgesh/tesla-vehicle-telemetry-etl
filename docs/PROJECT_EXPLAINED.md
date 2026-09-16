# The Project Explained, From Start to End

This document explains the whole project in order. It starts with the idea, follows one file from S3 to Snowflake, and then explains every file and both scripts.
You can read it top to bottom while sharing your screen in class.

---

## 1. The idea in five sentences

1. Electric cars send small messages all day. We call each message an **event**.
2. The events are saved as text files in an **S3 bucket** on AWS.
3. An **Airflow** pipeline picks up new files, checks the data, and turns it into 5 report tables.
4. The tables are loaded into **Snowflake**, a data warehouse where analysts run SQL.
5. The pipeline remembers which files it already loaded, so it never loads the same file twice.

This is called an **ETL** pipeline: **E**xtract, **T**ransform, **L**oad.

---

## 2. The big picture

```text
┌────────────────────────────────────────────────────────────┐
│ Amazon S3 (raw zone)                                       │
├────────────────────────────────────────────────────────────┤
│ telemetry/year=2026/.../hour=08/events.jsonl               │
└────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────┐
│ Airflow DAG (in Docker on your laptop)                     │
├────────────────────────────────────────────────────────────┤
│ 1. extract_from_s3    download new files -> build/raw      │
│ 2. validate_data      check every event  -> build/clean    │
│ 3. transform_data     build 5 tables     -> build/curated  │
│ 4. load_to_snowflake  upload, copy, merge                  │
└────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────┐
│ Snowflake (database TESLA_TELEMETRY)                       │
├────────────────────────────────────────────────────────────┤
│ RAW      stage for files + list of loaded files            │
│ STAGING  5 waiting tables (only this run)                  │
│ CURATED  5 final tables for reports                        │
└────────────────────────────────────────────────────────────┘
```

The same flow in one line:

```text
S3 file -> download -> check -> 5 tables (Parquet) -> Snowflake stage -> staging tables -> MERGE -> curated tables
```

---

## 3. Words you need to know

| Word | Simple meaning |
| --- | --- |
| ETL | Extract (get the data), Transform (clean and reshape it), Load (put it in the warehouse). |
| Event | One message from a car: time, speed, battery, position and so on. |
| Batch | All the events the pipeline checks and loads together in one run. |
| JSON / JSONL | JSON is a text format for data. JSONL (JSON Lines) is a file with one JSON object per line. |
| Amazon S3 | AWS file storage. Files are kept in a **bucket**. The path of a file is called its **key**. |
| ETag | A fingerprint of a file in S3. If the file content changes, the ETag changes. |
| IAM user | A user in AWS for a program. It has an **access key** (like a user name and password for code). |
| Apache Airflow | A tool that runs pipelines in the right order, shows them on a web page and keeps logs. |
| DAG | Airflow's word for a pipeline: tasks joined by arrows, with no loops. |
| Task | One step of a DAG. Our DAG has 4 tasks. |
| Docker / container | Docker runs programs in small isolated boxes called containers. We run Airflow this way, so nobody has to install Airflow by hand. |
| Docker image / volume / port | An image is the download a container starts from. A volume is a disk that Docker keeps between restarts. A port is a numbered door on your laptop, for example 8090 for the Airflow page. |
| pandas / DataFrame | A Python library for tables. A DataFrame is one table in memory. |
| pydantic | A Python library that checks data against a description (field names, types, limits). |
| Parquet | A file format for tables. It keeps column types and is fast to load. |
| Snowflake | A cloud data warehouse. You store tables there and query them with SQL. |
| Snowsight | The Snowflake web page. You write and run SQL there, in a worksheet. |
| Role (Snowflake) | A set of rights. `SYSADMIN` may create warehouses, databases and tables. `ACCOUNTADMIN` may also change users. |
| Warehouse (Snowflake) | The compute engine that runs SQL. It costs money only while it runs. Do not mix it up with "data warehouse" (section 1), which means the whole system where tables are stored. |
| Database / schema | In Snowflake, a database is the top folder and a schema is a sub folder with tables. Careful: in `schemas.py` and "schema check", schema means something else: the description of what one event must look like. |
| Stage | A place inside Snowflake where you upload files before loading them into tables. |
| Staging table | A waiting table. It holds only the rows of the current run. |
| Curated table | The final, clean table that reports use. |
| MERGE | One SQL statement that updates rows that already exist and inserts new ones. |
| Stored procedure | A saved block of SQL in Snowflake that you run with `CALL`. |
| Key pair | Two matching keys. The private key stays on your laptop. The public key is given to Snowflake. It replaces a password. |
| Virtual environment | A private Python folder (`.venv`) with the libraries for one project. |

---

## 4. The data

### 4.1 One event

Every line of the input file is one event like this:

```json
{"event_id":"evt_00000003","vin":"5YJ3E1EA7KF000001","event_ts":"2026-06-06T08:25:00Z","ingest_ts":"2026-06-06T08:25:05Z","event_type":"alert","latitude":37.7894,"longitude":-122.4011,"speed_mph":41.7,"battery_soc":80.9,"battery_temp_c":31.1,"odometer_miles":12852.1,"charging_state":"Disconnected","gear":"D","autopilot_engaged":false,"alert_code":"TIRE_PRESSURE_LOW","software_version":"2026.14.3"}
```

| Field | Meaning | Rule we check |
| --- | --- | --- |
| `event_id` | unique id of the event | at least 8 characters, no duplicates in a batch |
| `vin` | Vehicle Identification Number (which car) | 11 to 17 characters, no spaces, made uppercase |
| `event_ts` | when it happened in the car | must have a time zone (the `Z` means UTC) |
| `ingest_ts` | when our system received it | must have a time zone, must not be before `event_ts` |
| `event_type` | `telemetry`, `charge` or `alert` | only these 3 words |
| `latitude`, `longitude` | GPS position | -90 to 90, and -180 to 180 |
| `speed_mph` | speed in miles per hour | 0 to 180 |
| `battery_soc` | battery charge in percent | 0 to 100 |
| `battery_temp_c` | battery temperature in Celsius | -40 to 90 |
| `odometer_miles` | total miles the car has driven | not negative, not 0 when speed is above 0 |
| `charging_state` | for example `Charging` or `Disconnected` | text |
| `gear` | `P` park, `D` drive | text |
| `autopilot_engaged` | was Autopilot on | true or false |
| `alert_code` | which alert, only for alerts | required when `event_type` is `alert` |
| `software_version` | car software version | text |

### 4.2 The sample file

`data/sample/tesla_telemetry_sample.jsonl` has 6 events from 2 cars on 6 June 2026:

| event | car | time (UTC) | type | speed | battery | what is happening |
| --- | --- | --- | --- | --- | --- | --- |
| evt_00000001 | ...000001 | 08:00 | telemetry | 0 | 82.4 | parked |
| evt_00000002 | ...000001 | 08:10 | telemetry | 34.2 | 81.8 | driving, Autopilot on |
| evt_00000003 | ...000001 | 08:25 | alert | 41.7 | 80.9 | driving, tire pressure low |
| evt_00000004 | ...000001 | 09:05 | charge | 0 | 80.5 | parked and charging |
| evt_00000005 | ...000002 | 08:00 | telemetry | 12.4 | 54.2 | driving |
| evt_00000006 | ...000002 | 08:30 | telemetry | 65.1 | 49.9 | driving, Autopilot on |

### 4.3 Where the file lives in S3

```text
s3://<your-bucket>/telemetry/year=2026/month=06/day=06/hour=08/events.jsonl
```

The folders `year=`, `month=`, `day=`, `hour=` are a common way to organise raw data by time. It is easy to find the files of one hour, and new files never overwrite old ones.

---

## 5. The folders and files

```text
tesla-vehicle-telemetry-etl/
├── create_pipeline.sh              build everything and run the pipeline once
├── remove_pipeline.sh              remove everything again
├── docker-compose.yml              start Airflow in Docker
├── requirements.txt                Python libraries for your laptop
├── pytest.ini                      where pytest finds the code and tests
├── .env.example                    template for your private settings
├── .env                            your private settings (you create it, never uploaded)
├── .secrets/snowflake_rsa_key.p8   your Snowflake private key (created by the script, never uploaded)
│
├── data/sample/tesla_telemetry_sample.jsonl   6 sample events
├── dags/tesla_telemetry_dag.py                the Airflow DAG (the order of the 4 tasks)
├── src/telemetry_etl/
│   ├── config.py        settings
│   ├── schemas.py       what one event must look like
│   ├── extract.py       list and download files from S3
│   ├── quality.py       read and write JSONL, check data quality
│   ├── transform.py     build the 5 tables, save as Parquet
│   └── load.py          log in to Snowflake, load and merge
├── snowflake/
│   ├── 01_create_database.sql         warehouse, database, schemas, stage
│   ├── 02_create_tables.sql           file list, curated and staging tables
│   ├── 03_create_merge_procedure.sql  the MERGE procedure
│   └── 99_drop_everything.sql         delete it all
├── scripts/pipeline_helper.py         helper commands for the two scripts
├── tests/test_quality.py, test_transform.py   unit tests
│
└── build/        created while the pipeline runs (never uploaded)
    ├── raw/        files downloaded from S3
    ├── clean/      checked events
    └── curated/    the 5 Parquet files
```

---

## 6. One file from S3 to Snowflake, step by step

This is the most important section. We follow the sample file from S3 to Snowflake.

### Step 0: before the pipeline

- The file is uploaded to S3 under `telemetry/...`.
- `create_pipeline.sh` has created the Snowflake objects and started Airflow.
- Somebody starts the DAG (the script does it, or you click the play button in Airflow).

The DAG is described in `dags/tesla_telemetry_dag.py`. The last 4 lines of the DAG function set the order:

```python
files = extract_from_s3()
clean_file = validate_data(files)
parquet_files = transform_data(clean_file)
load_to_snowflake(parquet_files, files)
```

What one task returns is passed to the next task. Airflow stores these small values for us (this is called XCom).

### Step 1: `extract_from_s3` (Extract)

Code: `extract_from_s3` in the DAG, plus `src/telemetry_etl/extract.py` and `load.get_processed_files`.

1. `load_settings()` reads `.env` (bucket, region, Snowflake account).
2. `extract.list_s3_files()` asks S3 for every file under `telemetry/` and keeps only `.jsonl` files. For each file it keeps the bucket, the key (path) and the ETag (fingerprint).
3. `load.get_processed_files()` reads the table `RAW.PROCESSED_FILES` in Snowflake. That table lists every file we loaded before.
4. The task keeps only files whose (key, ETag) pair is not in that list. If a file was changed in S3, its ETag changes, so it counts as new again.
5. If nothing is new, the task raises `AirflowSkipException`. Airflow marks this task and all later tasks as **skipped** (pink). The run still counts as successful.
6. Otherwise `extract.download_files()` empties `build/raw/` and downloads each new file as `001_events.jsonl`, `002_events.jsonl` and so on. The number stops files with the same name from overwriting each other.
7. The task returns the list of files, now with a `local_path` for each one.

After this step: `build/raw/001_events.jsonl` exists.

### Step 2: `validate_data` (quality gate)

Code: `validate_data` in the DAG, plus `src/telemetry_etl/quality.py` and `schemas.py`.

1. `quality.read_jsonl()` reads every downloaded file, line by line, into a list of Python dictionaries. A broken line stops the task with the line number. If the files have no events at all, the task stops too.
2. `quality.check_records()` checks each event:
   - **Schema check.** Pydantic compares the event with the `TelemetryEvent` class in `schemas.py`: field names, types and limits (for example battery between 0 and 100). Unknown extra fields are refused.
   - **Duplicate check.** The same `event_id` must not appear twice in one batch.
   - **Time check.** `event_ts` must not be later than `ingest_ts`.
   - **Odometer check.** A car with a speed above 0 cannot have an odometer of 0.
   - **Alert check.** An alert must have an `alert_code`.
3. Each problem becomes a readable sentence in a `problems` list. If an event fails the schema check, only its first wrong field is listed, and the other 4 checks are skipped for that event.
4. If the list is not empty, the task **fails** (red) and prints all problems. Bad data never reaches Snowflake. The file is not marked as loaded, so it will be tried again next time.
5. If the data is good, the clean events are written to `build/clean/clean_events.jsonl`. The task returns that path.

After this step: `build/clean/clean_events.jsonl` exists with 6 checked events.

### Step 3: `transform_data` (Transform)

Code: `transform_data` in the DAG, plus `src/telemetry_etl/transform.py`.

1. The clean file is read back into a list of events.
2. `transform.build_tables()` puts them into a pandas DataFrame and adds helpful columns:
   - `event_ts`, `ingest_ts` become real date-time values in UTC
   - `event_date` the day, for example 2026-06-06
   - `event_hour` the hour, for example 08:25 becomes 08:00
   - `is_moving` true when speed is above 1 mph
   - `is_charging` true when the charging state is "Charging"
   - `battery_band` critical (up to 20), low (up to 50), normal (up to 80), high (above 80)
3. Then it builds the 5 tables with `groupby` (put rows with the same values together) and `agg` (calculate one number per group). See section 7 for what each table holds.
4. `transform.save_tables_as_parquet()` writes each table to `build/curated/<table>.parquet`.
5. The task returns the 5 file paths.

After this step: 5 Parquet files exist in `build/curated/`.

### Step 4: `load_to_snowflake` (Load)

Code: `load_to_snowflake` in the DAG, plus `src/telemetry_etl/load.py` and the SQL files.

1. `load.connect()` logs in to Snowflake with the private key file, the role `SYSADMIN`, the warehouse `TELEMETRY_WH` and the database `TESLA_TELEMETRY`. It also sets the time zone to UTC.
2. `load.load_tables()` does 3 moves for each of the 5 files:
   - `TRUNCATE TABLE STAGING.<table>` empties the staging table, so it only holds this run.
   - `PUT 'file://.../<table>.parquet' @RAW.TELEMETRY_STAGE` uploads the file into the Snowflake stage.
   - `COPY INTO STAGING.<table> FROM @RAW.TELEMETRY_STAGE/<table>.parquet MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` copies the rows from the file into the staging table. Each column goes into the table column with the same name.
3. Then it runs `CALL CURATED.MERGE_STAGING_INTO_CURATED()`. This procedure runs 5 MERGE statements (section 8).
4. `load.mark_files_as_processed()` inserts one row per S3 file into `RAW.PROCESSED_FILES`. This happens only after the load worked. The next run will skip these files.

After this step: the curated tables have the data, and the file is on the "already loaded" list.

### Step 5: look at the result

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE TELEMETRY_WH;
SELECT * FROM TESLA_TELEMETRY.CURATED.TELEMETRY_ENRICHED ORDER BY event_ts;
SELECT * FROM TESLA_TELEMETRY.CURATED.VEHICLE_HOURLY_METRICS;
SELECT * FROM TESLA_TELEMETRY.CURATED.TRIP_METRICS;
SELECT * FROM TESLA_TELEMETRY.CURATED.BATTERY_HEALTH;
SELECT * FROM TESLA_TELEMETRY.CURATED.ALERTS;
SELECT * FROM TESLA_TELEMETRY.RAW.PROCESSED_FILES;
```

---

## 7. The 5 output tables

| Table | One row means | Key (what makes a row unique) | Rows from the sample |
| --- | --- | --- | --- |
| `TELEMETRY_ENRICHED` | one event, plus the extra columns | `event_id` | 6 |
| `VEHICLE_HOURLY_METRICS` | one car in one hour | `vin` + `event_hour` | 3 |
| `TRIP_METRICS` | one car on one day, only moving events | `vin` + `event_date` | 2 |
| `BATTERY_HEALTH` | one car on one day, battery numbers | `vin` + `event_date` | 2 |
| `ALERTS` | one alert event | `event_id` | 1 |

Worked examples from the sample file:

- **Hourly:** car ...000001 has 3 events in the 08:00 hour and 1 event in the 09:00 hour. Car ...000002 has 2 events in the 08:00 hour. That makes 3 rows.
- **Trips:** car ...000001 was moving in evt_00000002 and evt_00000003. The odometer went from 12844.9 to 12852.1, so `distance_miles` is 7.2. Car ...000002 went from 44210.0 to 44242.8, which is 32.8 miles.
- **Battery:** car ...000001 has an average battery of 81.4 percent, a minimum of 80.5, and 1 charging event.
- **Alerts:** only evt_00000003 (TIRE_PRESSURE_LOW).

---

## 8. Why we use staging tables and MERGE

We could insert the rows straight into the final tables. But then running the pipeline twice would create every row twice.

So we load in two stages:

1. **Staging** tables get only the rows of this run (we empty them first).
2. **MERGE** compares staging with the curated table using the key:
   - the key already exists, so the row is **updated**
   - the key is new, so the row is **inserted**

Example: an event `evt_00000003` is loaded on Monday. On Tuesday a corrected file arrives with the same event and a new battery value. MERGE finds `evt_00000003` in the curated table and updates it. There is still only one row for that event.

In `03_create_merge_procedure.sql` each MERGE looks like this:

```sql
MERGE INTO TESLA_TELEMETRY.CURATED.TELEMETRY_ENRICHED AS target
USING TESLA_TELEMETRY.STAGING.TELEMETRY_ENRICHED AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE ALL BY NAME
WHEN NOT MATCHED THEN INSERT ALL BY NAME;
```

`ALL BY NAME` means "use every column, matched by name". It works because each staging table is a copy of its curated table (`CREATE TABLE ... LIKE ...` in `02_create_tables.sql`). So both have exactly the same columns.

---

## 9. How the pipeline avoids loading a file twice

- Table `RAW.PROCESSED_FILES` holds one row per loaded S3 file: bucket, key, ETag and time.
- Task 1 reads this table and skips every file whose key and ETag are already there.
- Task 4 adds the new files to the table, but only after the load worked. If any step fails, the file is not added, so the next run tries it again.

So there are two safety nets:

1. The file list stops the same file from being loaded again.
2. MERGE stops duplicate rows even if the same events come in a different file.

---

## 10. What `create_pipeline.sh` does

Run it with `bash create_pipeline.sh`. It prints `=== STEP n of 11 ===` before each step and stops with `STOPPED: ...` and a clear message if something is wrong.

| Step | What happens | Files or tools used |
| --- | --- | --- |
| 1 | Checks that Python, Docker (running) and openssl are there | |
| 2 | Checks `.env`. If it is missing, copies `.env.example` to `.env` and stops so you can fill it in. Every value must be filled in. | `.env` |
| 3 | Creates `.venv` (or rebuilds a broken one) and installs the libraries. On Ubuntu or WSL without the venv package, it downloads pip itself. | `requirements.txt` |
| 4 | Creates the private key the first time, and makes the public key from it | `openssl`, `.secrets/` |
| 5 | Tries to log in to Snowflake. If Snowflake does not know the key yet, prints the 2 lines of SQL to run in Snowsight and waits for Enter | `scripts/pipeline_helper.py check-snowflake` |
| 6 | Runs the unit tests | `tests/`, `pytest.ini` |
| 7 | Lists the JSONL files in S3. Stops if there are none. | `pipeline_helper.py check-s3` |
| 8 | Runs the 3 SQL files in order | `snowflake/01..03`, `pipeline_helper.py run-sql` |
| 9 | Starts Airflow with `docker compose up -d` and waits for the web page | `docker-compose.yml` |
| 10 | Waits until Airflow knows the DAG, switches it on (a new DAG starts paused), starts a run, checks every 5 seconds until it ends, prints each task state | Airflow command line inside the scheduler container |
| 11 | Prints the row counts and the list of loaded files | `pipeline_helper.py show-results` |

You can run it again at any time. The warehouse, database, schemas, stage and tables that already exist are kept (`CREATE ... IF NOT EXISTS`). The merge procedure is replaced with the newest version (`CREATE OR REPLACE`). A second run finds no new files, so the 4 tasks show as **skipped** and the tables do not change.

### How Airflow runs our code (docker-compose.yml)

- The project folder is shared into the containers at `/opt/project`.
- `AIRFLOW__CORE__DAGS_FOLDER=/opt/project/dags` tells Airflow where the DAG is.
- `PYTHONPATH=/opt/project/src` lets Python import `telemetry_etl`.
- `env_file: .env` gives the containers our AWS and Snowflake settings.
- The Airflow image already has pandas, pyarrow, pydantic, boto3 and the Snowflake connector, so nothing extra is installed.
- `user: "${AIRFLOW_UID:-50000}:0"` runs Airflow with your own user id (the scripts set `AIRFLOW_UID`), so the files in `build/` belong to you. Without the scripts, Airflow's normal user 50000 is used.
- 4 containers: `postgres` (Airflow's own database), `airflow-init` (one-time setup and the `airflow` login), `airflow-webserver` (the page on port 8090), `airflow-scheduler` (runs the tasks).

---

## 11. What `remove_pipeline.sh` does

| Step | What happens |
| --- | --- |
| 1 | Runs `snowflake/99_drop_everything.sql`: drops the database `TESLA_TELEMETRY` (with all schemas, tables, the stage and the procedure) and the warehouse `TELEMETRY_WH` |
| 2 | `docker compose down --volumes`: stops and deletes the Airflow containers, Airflow's database and the logs. With `--everything` it also deletes the downloaded Docker images (`--rmi all`). |
| 3 | Deletes `build/`, `.pytest_cache/` and `__pycache__/` |
| 4 | Deletes `.venv/` |
| 5 | Only with `--everything`: deletes `.env` and `.secrets/`, and prints the SQL to remove the public key from your Snowflake user |
| 6 | Prints the AWS console steps to delete the bucket and the IAM user (the pipeline key can only read, so the script cannot delete them) |

It does not stop when one step fails. It moves on and tells you what to do by hand.
Your code, the sample data and the S3 file are never touched.

---

## 12. Security in this project

| Secret | Where it lives | How it is protected |
| --- | --- | --- |
| AWS access key | `.env` | `.env` is in `.gitignore` and only you can read it (`chmod 600`). The IAM user can only list and read `telemetry/` in one bucket. |
| Snowflake private key | `.secrets/snowflake_rsa_key.p8` | In `.gitignore`, only you can read it. Only the public key is given to Snowflake. |
| Airflow login | `airflow` / `airflow` | Only for your own laptop. Do not open port 8090 to the internet. |

We use a key instead of a Snowflake password, because Snowflake is turning off logins that use only a password. A person can add a second factor (a code on the phone), but a pipeline cannot. Key pair login is the normal way for programs.

If a secret was ever shown on screen or pasted in a chat, create a new one and delete the old one.

---

## 13. Problems we met while building this, and the fixes

These are real problems from the first setup. They make good class examples.

| Problem | What we saw | Cause | Fix in the project |
| --- | --- | --- | --- |
| Key not accepted | `JWT token is invalid` | The key was never saved in Snowflake. Most likely the SQL was run with **Run**, which runs only the line under the cursor. | Use **Run All**. The script prints the exact SQL. |
| Wrong dates | Dates in the year 594603171, then a failed MERGE | pandas saves times in nanoseconds. Snowflake's old Parquet reader read them as plain numbers. | `USE_VECTORIZED_SCANNER = TRUE` on the stage (01 SQL file), and the session time zone set to UTC |
| Duplicate rows in MERGE | `Duplicate row detected during DML action` | The staging table still had rows from an earlier run | `TRUNCATE` the staging table before each `COPY` |
| Upload failed | PUT error | The laptop folder name has spaces | Put quotes around the path: `PUT 'file://...'` |
| S3 error only inside Airflow | `IllegalLocationConstraintException` | The bucket was in `ap-south-1`, but `.env` said `ap-southeast-7`. The newer boto3 on the laptop worked around it without an error. The older boto3 in Airflow did not. | `AWS_REGION` in `.env` must be the bucket's real region |
| Resources "missing" | `AccessDenied` from the laptop's AWS CLI | The console and the laptop CLI were two different AWS accounts | The pipeline uses only the key in `.env` |
| Airflow page did not open | `port is already allocated` | Another program used port 8080 | Airflow page moved to port 8090 |
| Could not log in to Airflow | no user | The YAML command was split over lines with different indents, so the "create user" part never ran | All lines of the command at the same indent |
| Airflow libraries changed | pandas upgraded inside Airflow | Installing our project with pip inside the container upgraded libraries Airflow depends on | No install at all: we use `PYTHONPATH` and the libraries already in the image |

---

## 14. How to show it in class (about 30 minutes)

1. **The idea (3 min).** Show section 2 of this document.
2. **The data (3 min).** Open `data/sample/tesla_telemetry_sample.jsonl`. Then open the S3 bucket in the AWS console and show the `telemetry/year=.../hour=08/` folder.
3. **The code (8 min).** Open the files in this order: `schemas.py`, `quality.py`, `extract.py`, `transform.py`, `load.py`, then the DAG. Each line has a comment you can read out.
4. **The SQL (4 min).** Open `snowflake/01`, `02` and `03`.
5. **Run it (5 min).** Run `bash create_pipeline.sh`. While it runs, open <http://localhost:8090>, click the DAG, open **Graph**, and watch the 4 boxes turn green. Click a task and open **Logs**.
6. **The result (4 min).** Run the queries from step 5 of section 6 in Snowsight.
7. **Only new files are loaded (2 min).** Start the DAG again with the play button. All 4 tasks turn pink (skipped), because the file is already on the list. Loading only new files is called an **incremental load**.
8. **MERGE demo (optional).** Upload the same `events.jsonl` to a new folder, for example `hour=09`. Run the DAG. The file is new, so it is loaded. But the curated tables still have 6, 3, 2, 2 and 1 rows. MERGE updated the rows that were already there.
9. **Quality gate demo (optional).** Download the sample file, change one `battery_soc` to `150`, upload it to a new folder (for example `hour=10`) and run the DAG. The task `validate_data` turns red and its log says `field 'battery_soc' is wrong`. Nothing is loaded. Delete that file from S3 afterwards, or every next run will fail on it again.
10. **Clean up.** Run `bash remove_pipeline.sh`.

Notes for demos 8 and 9:

- If two new files with the same events are loaded in one run, the duplicate check fails. Upload one new file per run.
- Before you build the project again after `remove_pipeline.sh`, delete the extra `hour=09` and `hour=10` files from S3. Removing the pipeline deletes the file list in Snowflake, so the next build loads every file in the bucket in one run, and the duplicate check fails.

---

## 15. Questions students often ask

**Why not load the JSON straight into Snowflake?**
We could. But then bad data would reach Snowflake, and we would do the calculations in SQL. This project shows a common pattern: check and shape the data first, then load clean tables.

**Why Parquet and not CSV?**
Parquet keeps the column types (dates stay dates, numbers stay numbers) and is smaller. CSV turns everything into text.

**Why is the Airflow schedule `None`?**
So it only runs when we start it, and the Snowflake warehouse does not wake up every few minutes and cost money. In a real company you would set something like `"@hourly"`.

**What does the warehouse cost?**
It is the smallest size (XSMALL) and turns itself off after 60 seconds without work. One run uses about one minute.

**Where are the logs?**
In the Airflow page: click the DAG, then a task, then **Logs**. They are kept in a Docker volume and are deleted by `remove_pipeline.sh`.

**How do I add more data?**
Upload more `.jsonl` files with new `event_id` values under `telemetry/` in the bucket and run the DAG. Give the new events a new day in `event_ts`, for example 2026-06-07. Section 16 explains why.

---

## 16. Limits of this project

This is a teaching project. These are the known limits, and what a real project would do.

| Limit | What it means | What a real project would do |
| --- | --- | --- |
| Hourly and daily tables are built from one run only | If a later file has more events for a car and an hour or day that is already loaded, MERGE replaces that row with the numbers from the later file only. The numbers are not added together. | Recalculate these tables from all events in `CURATED.TELEMETRY_ENRICHED`, for example with SQL inside the procedure. |
| One bad file blocks every run | A file that fails the quality check is never marked as loaded, so every next run fails on it again. | Move bad files to a separate "quarantine" folder and load the good ones. |
| All new files are handled in one batch | Many large files would use a lot of memory. | Process the files in smaller groups. |
| Airflow 2.9.3 | The Airflow team no longer updates Airflow 2. | Use Airflow 3. |
| Secrets in a `.env` file | Fine on one laptop. | Use a secrets manager, for example AWS Secrets Manager or Airflow connections. |

