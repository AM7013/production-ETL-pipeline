import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
 
from prefect import flow, task
from prefect_shell import shell_run_command  # noqa: F401 (kept for future shell-based dbt/ops tasks)
 
from dbt_task import run_dbt_models, test_dbt_models
 
# --- Wire up imports from the modular tasks/ library ---------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
<<<<<<< HEAD
_ETL_PIPELINE_DIR = _REPO_ROOT / "etl-pipeline"
=======
_ETL_PIPELINE_DIR = _REPO_ROOT / "Pipeline_building"
>>>>>>> 35da853a6fc13e763165a52434cb7d3967f1bf64
if str(_ETL_PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(_ETL_PIPELINE_DIR))
 
import config
from tasks.ingestion import extract_data, profile_columns, stop_spark
from tasks.quality import run_quality_checks
from tasks.storage import load_to_postgres, load_to_bigquery, new_run_id
from tasks.tracking import time_tracking, track_pipeline_run
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
 
 
@task(name="Log Pipeline Start")
def log_start(run_id: str) -> None:
    logger.info("=" * 50)
    logger.info(f"[START] Pipeline run {run_id} initialized at {datetime.now(timezone.utc)}")
    logger.info("=" * 50)
 
 
@flow(name="ETL Pipeline")
def etl_pipeline(target_date: Optional[str] = None, dry_run: bool = False) -> None:
    if not target_date:
        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
 
    # Generated up front, before the try block, so it exists in scope for
    # the failure-tracking call too - a run that fails still gets a run_id
    # recorded, not just successful ones.
    run_id = new_run_id()
 
    if dry_run:
        logger.info(f"[DRY RUN] Pipeline would process data for date: {target_date}")
        return
 
    logger.info(f"[START] Processing data for date: {target_date}")
    log_start(run_id)
 
    try:
        with time_tracking("Full ETL Pipeline"):
            raw_data = extract_data(csv_filename=config.CSV_FILENAME)
            profile_columns(raw_data)
 
            cleaned_data, quality_report = run_quality_checks(raw_data)
 
            if quality_report["passed"]:
                load_to_postgres(cleaned_data, run_id=run_id)
                load_to_bigquery(cleaned_data, run_id=run_id)
 
                logger.info("[WAIT] Triggering downstream dbt models...")
                run_dbt_models()
                test_dbt_models()
 
                track_pipeline_run(run_id, config.TABLE_NAME, quality_report["clean_rows"], success=True)
                logger.info(f"[{datetime.now(timezone.utc)}] [FINISH] Pipeline Completed Successfully!")
            else:
                track_pipeline_run(run_id, config.TABLE_NAME, quality_report.get("clean_rows", 0), success=False)
                logger.warning("[SKIP] Skipping load because quality check failed")
    finally:
        # Runs whether the try block succeeded, failed the quality gate,
        # or raised an exception - Spark always gets released.
        stop_spark()
 
 
if __name__ == "__main__":
    etl_pipeline(target_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
