# =====================================================================
# config.py
# Job of this file: keep all settings of the project in one place.
#
# There are two kinds of settings:
#   1. Fixed names that never change (folder names, Snowflake object names).
#      They are written directly in this file.
#   2. Private values that are different for every person (keys, account names).
#      They are read from the .env file.
# =====================================================================

# dataclass lets us make a small "box" that holds several values together.
from dataclasses import dataclass

# getenv reads one value from the environment variables.
from os import getenv

# Path helps us build file and folder paths that work on every computer.
from pathlib import Path

# load_dotenv copies every line of the .env file into the environment variables.
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Fixed settings
# ---------------------------------------------------------------------

# __file__ is the path of this file: <project>/src/telemetry_etl/config.py
# .parents[0] is telemetry_etl, .parents[1] is src, .parents[2] is the project folder.
# On a laptop this is the cloned repo. Inside Docker it is /opt/project.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# The pipeline writes its working files (raw, clean, curated) into the build folder.
# The "/" joins folder names into one path.
BUILD_DIR = PROJECT_ROOT / "build"

# The private key we use to log in to Snowflake (created by create_pipeline.sh).
SNOWFLAKE_KEY_FILE = PROJECT_ROOT / ".secrets" / "snowflake_rsa_key.p8"

# The folder inside the S3 bucket where the raw telemetry files are kept.
S3_PREFIX = "telemetry/"

# The Snowflake role we work with. SYSADMIN is the normal role for creating tables.
SNOWFLAKE_ROLE = "SYSADMIN"

# The Snowflake warehouse (the compute engine) created by snowflake/01_create_database.sql.
SNOWFLAKE_WAREHOUSE = "TELEMETRY_WH"

# The Snowflake database created by snowflake/01_create_database.sql.
SNOWFLAKE_DATABASE = "TESLA_TELEMETRY"


# ---------------------------------------------------------------------
# Private settings from the .env file
# ---------------------------------------------------------------------

# A small box that holds the four private values the code needs.
# "@dataclass" writes the boring setup code of the class for us.
@dataclass
class Settings:
    # AWS region of the S3 bucket, for example ap-south-1 (": str" means the value is text)
    aws_region: str
    # Name of the S3 bucket with the raw files
    s3_bucket: str
    # Snowflake account identifier, for example ABCDEFG-XY12345
    snowflake_account: str
    # Snowflake login name, for example STUDENT1
    snowflake_user: str


# Read one setting. "name: str" means the input is text, "-> str" means the result is text.
def read_setting(name: str) -> str:
    # Read the value, and remove spaces at the start and end.
    value = getenv(name, "").strip()
    # If the value is missing or still the template text, stop with a clear message.
    if value == "" or value == "replace_me":
        raise ValueError(f"Please fill in {name} in the .env file")
    # Otherwise give the value back.
    return value


# Read all settings and give back one Settings box.
def load_settings() -> Settings:
    # Copy the values from the .env file into the environment variables.
    # Inside Docker, Airflow already has them, so this line changes nothing there.
    load_dotenv(PROJECT_ROOT / ".env")
    # Build the settings box from the environment variables.
    # Note: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not read here.
    # boto3 (the AWS library) finds them in the environment by itself.
    return Settings(
        aws_region=read_setting("AWS_REGION"),
        s3_bucket=read_setting("S3_BUCKET"),
        snowflake_account=read_setting("SNOWFLAKE_ACCOUNT"),
        snowflake_user=read_setting("SNOWFLAKE_USER"),
    )
