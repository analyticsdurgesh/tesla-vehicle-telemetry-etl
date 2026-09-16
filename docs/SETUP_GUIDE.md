# Setup Guide

This guide covers what to install and what to create before you run `bash create_pipeline.sh`.
Do the steps in order. It takes about 30 minutes the first time.

## 1. Accounts you need

| Account | What we use it for | Notes |
| --- | --- | --- |
| AWS | An S3 bucket for the raw files, and one IAM user (an AWS login for a program) for the pipeline | You need to log in to the AWS console as an admin. |
| Snowflake | The data warehouse where the tables are loaded | A free 30-day trial works. See the note below. |
| GitHub | Only to download the code | Optional. |

A **role** is a set of rights in Snowflake. Your Snowflake user must be able to use two roles:

- `SYSADMIN` creates warehouses, databases and tables.
- `ACCOUNTADMIN` can change users.

Trial users can use both.

**Snowsight** is the Snowflake web page (<https://app.snowflake.com>). You write and run SQL there, in a **worksheet**.

## 2. Tools to install on your laptop

| Tool | Why | Check it with |
| --- | --- | --- |
| Git | download the project | `git --version` |
| Python 3.11 | run the tests and the helper commands | Mac: `python3.11 --version`, Windows: `py -3.11 --version` |
| Docker Desktop 4.27 or newer | run Airflow | `docker compose version` (must be 2.24 or newer) |
| openssl | create the Snowflake login key | `openssl version` |
| A web browser | AWS console, Snowsight, Airflow page | |

### macOS

Install Homebrew first if you do not have it (see <https://brew.sh>). Then:

```bash
brew install git python@3.11
# Docker Desktop: download it from https://www.docker.com/products/docker-desktop/
# openssl is already on macOS.
```

### Windows

Open PowerShell and run:

```powershell
winget install Git.Git
winget install Python.Python.3.11
winget install Docker.DockerDesktop
```

Docker Desktop on Windows needs WSL 2 (Windows Subsystem for Linux, a small Linux system inside Windows). If Docker Desktop asks for it, open PowerShell as administrator, run `wsl --install`, and restart the computer.

After that, open **Git Bash** (it comes with Git) and type **every** command of this guide there. This includes the two `.sh` scripts and the `mkdir` and `cp` lines in step 4.3. PowerShell does not understand these commands. Git Bash also has `openssl`.

### Linux, or Ubuntu inside WSL

```bash
sudo apt update
sudo apt install -y git python3 python3-venv openssl curl
```

`python3-venv` lets Python create the `.venv` folder. If it is missing, `create_pipeline.sh` tries to work around it by downloading pip. If that also fails, the script tells you the exact package to install (for example `python3.12-venv`).
In WSL, also turn on **Settings > Resources > WSL integration** for your Ubuntu in Docker Desktop, so the `docker` command works inside Ubuntu.

### Docker Desktop settings

- Open Docker Desktop and wait until it says it is running.
- Mac: in **Settings > Resources**, give it at least 4 GB of memory. On Windows with WSL 2, Windows manages the memory, so you can skip this.
- The Airflow page uses port **8090** on your laptop. A port is a numbered door on your computer. If another program already uses 8090, change `"8090:8080"` in `docker-compose.yml` **and** `AIRFLOW_URL` in `create_pipeline.sh` to the same new number.

You do **not** need to install Airflow, Postgres or any Snowflake tool. Docker downloads Airflow and Postgres for you.

## 3. Get the code

```bash
git clone https://github.com/analyticsdurgesh/tesla-vehicle-telemetry-etl.git
cd tesla-vehicle-telemetry-etl
```

## 4. AWS: create the bucket, upload the file, create the IAM user

Do all of this in the AWS console with your admin login.
Write down your **AWS account ID** (top right, under your name). You will see it in a few places.

### 4.1 Pick a region

A region is the place in the world where AWS keeps your data.
Pick a region that is already switched on for your account, for example **Asia Pacific (Mumbai) ap-south-1**.
Some newer regions (for example Thailand, ap-southeast-7) must be switched on first under **Account > AWS Regions**.
Any region works, as long as `AWS_REGION` in your `.env` file is the same region.

### 4.2 Create the S3 bucket

1. Open **S3** and click **Create bucket**.
2. Check the region at the top right of the page. The bucket is created in that region. Write it down.
3. Fill in:
   - **Bucket type:** General purpose
   - **Bucket name:** a unique name, for example `tesla-telemetry-raw-<your-account-id>`
   - **Object Ownership:** ACLs disabled
   - **Block all public access:** keep it ticked
   - **Bucket Versioning:** Enable
   - **Default encryption:** SSE-S3, Bucket Key: Enable
4. Click **Create bucket**.

### 4.3 Upload the sample file

The pipeline looks for `.jsonl` files inside the folder `telemetry/` in the bucket.

1. In the project folder, make this folder path and copy the sample file into it, with the new name `events.jsonl`:

   ```bash
   mkdir -p upload/telemetry/year=2026/month=06/day=06/hour=08
   cp data/sample/tesla_telemetry_sample.jsonl upload/telemetry/year=2026/month=06/day=06/hour=08/events.jsonl
   ```

2. In the bucket, click **Upload**, then **Add folder**, and pick the `telemetry` folder inside `upload`.
3. Click **Upload**.
4. Check that this file exists in the bucket: `telemetry/year=2026/month=06/day=06/hour=08/events.jsonl`

You can delete the `upload` folder on your laptop afterwards. Git ignores it.

### 4.4 Create the IAM user for the pipeline

1. Open **IAM > Users > Create user**.
2. **User name:** `tesla-telemetry-etl`. Do not tick console access. Click **Next**.
3. Choose **Attach policies directly**, but do not select any policy. Click **Next**, then **Create user**.

### 4.5 Give the user read access to the bucket

A **policy** is a list of rules that says what a user may do.

1. Open the user `tesla-telemetry-etl` > **Permissions** tab > **Add permissions** > **Create inline policy**. "Inline" means the policy belongs to this one user only.
2. Click **JSON**, delete what is there, and paste the policy below.
   Replace `YOUR_BUCKET_NAME` (3 times) with your bucket name.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListTelemetryFolder",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME",
      "Condition": { "StringLike": { "s3:prefix": ["telemetry/", "telemetry/*"] } }
    },
    {
      "Sid": "BucketLocation",
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME"
    },
    {
      "Sid": "ReadTelemetryFiles",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/telemetry/*"
    }
  ]
}
```

3. Click **Next**, name it `TeslaTelemetryS3ReadOnly`, and click **Create policy**.

With this policy the user can only list and read files under `telemetry/`. It cannot delete or change anything.

### 4.6 Create the access key

1. On the same user, open **Security credentials** > **Access keys** > **Create access key**.
2. Use case: **Application running outside AWS**. Click **Next**, then **Create access key**.
3. Copy the **Access key** and the **Secret access key**, or download the `.csv` file.
   The secret is shown only once.

## 5. Snowflake: find your account and user

1. Log in to Snowsight with your normal Snowflake user name and password.
2. Click your name (bottom left) > **Account** > **View account details**.
3. Write down:
   - **Account identifier**, for example `ABCDEFG-XY12345`. If Snowsight shows it with a dot (`ABCDEFG.XY12345`), write it with a dash instead.
   - **Login name**, for example `STUDENT1`

The pipeline itself does not use your Snowflake password. `create_pipeline.sh` creates a login key and shows you two lines of SQL to run in Snowsight (section 7).

## 6. Fill in the .env file

```bash
cp .env.example .env
```

Open `.env` in any text editor and fill in all 6 values:

| Setting | Example | Where it comes from |
| --- | --- | --- |
| `AWS_REGION` | `ap-south-1` | the region of your bucket (step 4.2) |
| `AWS_ACCESS_KEY_ID` | `AKIA...` | step 4.6 |
| `AWS_SECRET_ACCESS_KEY` | `abc...` | step 4.6 |
| `S3_BUCKET` | `tesla-telemetry-raw-123456789012` | step 4.2 |
| `SNOWFLAKE_ACCOUNT` | `ABCDEFG-XY12345` | step 5 |
| `SNOWFLAKE_USER` | `STUDENT1` | step 5 |

Never share or upload `.env`. Git already ignores it.

## 7. Run the pipeline

```bash
bash create_pipeline.sh
```

The first time, step 5 of the script stops and prints two lines of SQL like this:

```sql
USE ROLE ACCOUNTADMIN;
ALTER USER STUDENT1 SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

1. In Snowsight, open a new SQL worksheet and paste both lines.
2. Run them with **Run All** (Cmd+Shift+Enter on Mac, Ctrl+Shift+Enter on Windows).
   The normal **Run** button runs only the line under the cursor, which is not enough.
3. Go back to the terminal and press Enter.

The script then does the rest. When it finishes, you see the row counts and:

- Airflow: <http://localhost:8090> (login `airflow` / `airflow`)
- Snowflake: database `TESLA_TELEMETRY`

## 8. Remove everything

```bash
bash remove_pipeline.sh                # keeps .env and the Snowflake key
bash remove_pipeline.sh --everything   # also deletes them and the Docker images
```

At the end, the script prints the AWS console steps for deleting the bucket and the IAM user. The script cannot delete them itself, because the pipeline's AWS key can only read.

## 9. If something goes wrong

| What you see | Why | What to do |
| --- | --- | --- |
| `Docker is not running` | Docker Desktop is closed | Open Docker Desktop, wait, run again |
| `Python 3.10 or newer was not found` | Python 3.11 is not installed, or the terminal was open before you installed it | Install Python 3.11, open a new terminal, run again. On Windows, tick **Add python.exe to PATH** in the installer (the script also tries the `py` launcher). |
| `ensurepip is not available` or `Python could not create .venv` | Ubuntu or WSL without the venv package | `sudo apt update && sudo apt install -y python3-venv` (or the exact package the script names), then run the script again |
| `... is not filled in inside .env` | a value is empty or still `replace_me` | Fill it in |
| `JWT token is invalid` | Snowflake does not know your public key | Run the SQL from step 7 with **Run All** |
| `IllegalLocationConstraintException` in the Airflow log | `AWS_REGION` is not the region of your bucket | Fix `AWS_REGION` in `.env`, then run `AIRFLOW_UID=$(id -u) docker compose up -d --force-recreate` |
| `InvalidAccessKeyId` right after creating the key | a new key takes a minute or two to work everywhere | Wait and run again |
| `No .jsonl files found` | the file is not under `telemetry/` in the bucket | Check step 4.3 |
| `port is already allocated` | another program uses port 8090 | Change the port in both files (see section 2) |
| A red task in Airflow | the task failed | Click the task, then **Logs**, and read the last lines |

## 10. Versions we tested (2026-09-16)

| Part | Version |
| --- | --- |
| Python on the laptop | 3.11.15 |
| pandas / pyarrow / pydantic | 2.3.3 / 25.0.1 / 2.13.5 |
| boto3 / snowflake-connector-python | 1.43.95 / 4.7.3 |
| Airflow Docker image | apache/airflow:2.9.3-python3.11 (inside: pandas 2.1.4, pyarrow 16.1.0, snowflake-connector-python 3.11.0) |
| Postgres Docker image | postgres:15 |
| Docker | Engine 29.6, Compose 5.3 |

The tests were run on a Mac. The Windows steps follow the same commands in Git Bash, but they were not tested on a Windows computer.

The Airflow team no longer updates Airflow 2 (end of life April 2026). It is fine for a class on your own laptop. Do not put it on the internet.
