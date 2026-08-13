import logging
from datetime import datetime, timezone
from typing import Optional

from prefect import flow, task

import config
from tasks.ingestion import extract_data, profile_columns, stop_spark
from tasks.schema_validation import validate_schema
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


@flow(name="ETL Pipeline Main Flow")
def etl_pipeline(
    target_date: Optional[str] = None,
    dry_run: bool = False,
    csv_filename: Optional[str] = None,
) -> None:
    if not target_date:
        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    csv_filename = csv_filename or config.CSV_FILENAME
    run_id = new_run_id()

    log_start(run_id)

    if dry_run:
        logger.info(f"[DRY RUN] Would process '{csv_filename}' for {target_date}. Skipping loads.")
        return

    # Fail fast on missing env vars instead of failing 20 minutes in on a
    # bad connection string built from a None value.
    config.validate_env(require_postgres=True)

    try:
        with time_tracking("Full ETL Pipeline"):
            try:
                raw_data = extract_data(csv_filename=csv_filename)
            except FileNotFoundError as e:
                logger.error(f"[FAIL] {e}")
                raise

            validate_schema(raw_data)
            profile_columns(raw_data)

            cleaned_data, quality_report = run_quality_checks(raw_data)

            if quality_report["passed"]:
                load_to_postgres(cleaned_data, run_id=run_id, idempotent=True)
                load_to_bigquery(cleaned_data, run_id=run_id, idempotent=True)

                logger.info("[WAIT] Triggering downstream dbt models...")
                track_pipeline_run(run_id, config.TABLE_NAME, quality_report["clean_rows"], success=True)
                logger.info(f"[FINISH] Full ETL & dbt pipeline completed successfully for {target_date}!")
            else:
                track_pipeline_run(run_id, config.TABLE_NAME, quality_report.get("clean_rows", 0), success=False)
                logger.warning(
                    "[SKIP] Pipeline halted: Postgres/BigQuery load & dbt models "
                    "skipped due to quality failure."
                )
                # Raising (instead of sys.exit) lets Prefect mark this run as
                # Failed with a real traceback, rather than Crashed. Running the
                # script directly still exits non-zero for CI, since an uncaught
                # exception at the top level does that automatically.
                raise RuntimeError(
                    f"Data quality {quality_report['quality_score']}% below threshold "
                    f"{config.QUALITY_THRESHOLD}% - pipeline halted."
                )
    finally:
        # Always release the SparkSession, success or failure - it was never
        # being closed before, which leaks in any long-lived process.
        stop_spark()


if __name__ == "__main__":
    etl_pipeline()
