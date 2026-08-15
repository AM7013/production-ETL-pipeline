import os
import logging
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

# Google BigQuery Configuration
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "production-etl-pipeline")
BQ_DATASET = os.getenv("BQ_DATASET", "my_etl_data")
BQ_TABLE = os.getenv("BQ_TABLE", "cleaned_data_from_spark")

# Postgres Configuration
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME", "orders")

# Local Storage & Data Paths
CSV_FILENAME = os.getenv("CSV_FILENAME", "cleaned_data_v1.csv")
QUARANTINE_FILE = os.getenv("QUARANTINE_FILE", "quarantine_zone.csv")

# Data Quality Settings
QUALITY_THRESHOLD = float(os.getenv("QUALITY_THRESHOLD", "80.0"))

# Prefect Cloud API URL - must be set via .env, no real workspace ID committed here
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "")


def get_postgres_url() -> str:
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def validate_env(require_postgres: bool = True) -> None:
    required = {"DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD, "DB_HOST": DB_HOST, "DB_NAME": DB_NAME}
    missing = [name for name, val in required.items() if not val]
    if require_postgres and missing:
        logger.error(f"[CONFIG] Missing required environment variables: {', '.join(missing)}")
        logger.error("[CONFIG] Copy .env.example to .env and fill these in.")
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")