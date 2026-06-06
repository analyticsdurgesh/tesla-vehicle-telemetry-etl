from __future__ import annotations

from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    # This class keeps all runtime configuration in one typed object.
    aws_region: str
    s3_bucket: str
    s3_prefix: str
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_role: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str


def load_settings() -> Settings:
    # Load local .env values first, then fall back to real environment variables.
    load_dotenv()
    return Settings(
        # AWS values tell the extractor where to find raw telemetry files.
        aws_region=getenv("AWS_REGION", "us-east-1"),
        s3_bucket=getenv("S3_BUCKET", ""),
        s3_prefix=getenv("S3_PREFIX", "telemetry/"),
        # Snowflake values tell the loader where curated data should land.
        snowflake_account=getenv("SNOWFLAKE_ACCOUNT", ""),
        snowflake_user=getenv("SNOWFLAKE_USER", ""),
        snowflake_password=getenv("SNOWFLAKE_PASSWORD", ""),
        snowflake_role=getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        snowflake_warehouse=getenv("SNOWFLAKE_WAREHOUSE", "TELEMETRY_WH"),
        snowflake_database=getenv("SNOWFLAKE_DATABASE", "TESLA_TELEMETRY"),
        snowflake_schema=getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )
