import os
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# BigQuery
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "production-etl-pipeline")
BQ_DATASET = os.getenv("BQ_DATASET", "my_etl_data")
BQ_TABLE = os.getenv("BQ_TABLE", "cleaned_data_from_spark")

# Data paths
CSV_FILENAME = os.getenv("CSV_FILENAME", "cleaned_data_v1.csv")
QUARANTINE_FILE = os.getenv("QUARANTINE_FILE", "quarantine_zone.csv")

# Quality
QUALITY_THRESHOLD = float(os.getenv("QUALITY_THRESHOLD", "80.0"))

# Prefect (optional - only if you really need it hardcoded)
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "https://api.prefect.cloud/api/accounts/5e2a7dd3-10e0-49a4-a447-6d6a83552b2c/workspaces/c128ac14-0a9f-4353-b2ba-0e2687775ec1")