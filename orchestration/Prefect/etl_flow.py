import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
import uuid
 
import pandas as pd
from prefect import flow, task
from prefect_shell import shell_run_command  # noqa: F401 (kept for future shell-based dbt/ops tasks)
 
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
 
from dbt_task import run_dbt_models, test_dbt_models
 
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_ETL_PIPELINE_DIR = _REPO_ROOT / "etl-pipeline"
if str(_ETL_PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(_ETL_PIPELINE_DIR))
 
import config
from tasks.ingestion import extract_data, profile_columns, stop_spark
from tasks.quality import run_quality_checks
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
 
_bq_client = None
NATURAL_KEY = "OrderID"
 
 
def _with_run_metadata(pandas_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """Tags every row with the run that loaded it - matches tasks/storage.py's version."""
    tagged = pandas_df.copy()
    tagged["run_id"] = run_id
    tagged["loaded_at"] = datetime.now(timezone.utc)
    return tagged
 
 
@task(name="Log Pipeline Start")
def log_start(run_id: str) -> None:
    logger.info("=" * 50)
    logger.info(f"[START] Pipeline run {run_id} initialized at {datetime.now(timezone.utc)}")
    logger.info("=" * 50)
 
 
def alert_on_crash(task, task_run, state) -> None:
    logger.error(f"[ALERT] Task {task.name} failed! Exception: {state.message}")
 
 
def _get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=config.BQ_PROJECT_ID)
    return _bq_client
 
 
def _bq_table_exists(client: bigquery.Client, table_ref: str) -> bool:
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False
 
 
@task(name="BigQuery Load", retries=3, retry_delay_seconds=10, on_failure=[alert_on_crash])
def load_to_bigquery(pandas_df: pd.DataFrame, run_id: str) -> None:
    if pandas_df is None or pandas_df.empty:
        logger.warning("[SKIP] No data to load into BigQuery.")
        return
 
    tagged_df = _with_run_metadata(pandas_df, run_id)
    destination_table = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET}.{config.BQ_TABLE}"
    client = _get_bq_client()
 
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
 
    if not _bq_table_exists(client, destination_table):
        logger.info(f"[CREATE] '{destination_table}' doesn't exist yet - "
                    f"creating with daily partitioning on loaded_at, clustered on run_id/{NATURAL_KEY}.")
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="loaded_at"
        )
        job_config.clustering_fields = ["run_id", NATURAL_KEY]
 
    logger.info(f"[SEND] Loading {len(tagged_df):,} rows into BigQuery -> {destination_table} (run_id={run_id})...")
    try:
        job = client.load_table_from_dataframe(tagged_df, destination_table, job_config=job_config)
        job.result()
    except Exception as e:
        logger.error(f"[ALERT] BigQuery load failed: {type(e).__name__}: {str(e)[:500]}")
        raise
 
    logger.info(f"[OK] Uploaded {job.output_rows:,} rows to BigQuery -> {destination_table}")
 
 
@flow(name="ETL Pipeline")
def etl_pipeline(target_date: Optional[str] = None, dry_run: bool = False) -> None:
    if not target_date:
        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
 
    run_id = str(uuid.uuid4())
 
    if dry_run:
        logger.info(f"[DRY RUN] Pipeline would process data for date: {target_date}")
        return
 
    logger.info(f"[START] Processing data for date: {target_date}")
    log_start(run_id)
 
    try:
        # extract_data() and run_quality_checks() are the SAME functions
        # tasks/test_quality.py and any future tasks/test_ingestion.py
        # already test - no second implementation to drift out of sync.
        raw_data = extract_data(csv_filename=config.CSV_FILENAME)
        profile_columns(raw_data)
 
        cleaned_data, quality_report = run_quality_checks(raw_data)
 
        if quality_report["passed"]:
            load_to_bigquery(cleaned_data, run_id=run_id)
 
            logger.info("[WAIT] Triggering downstream dbt models...")
            run_dbt_models()
            test_dbt_models()
 
            logger.info(f"[{datetime.now(timezone.utc)}] [FINISH] Pipeline Completed Successfully!")
        else:
            logger.warning("[SKIP] Skipping load because quality check failed")
    finally:
        stop_spark()
 
 
if __name__ == "__main__":
    etl_pipeline(target_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"))