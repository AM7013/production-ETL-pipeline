import logging
import os
from datetime import datetime
import pandas as pd
import pandas_gbq
from prefect import flow, task
from pyspark.sql import SparkSession
from prefect_shell import shell_run_command
from dbt_task import run_dbt_models, test_dbt_models
from pathlib import Path
from config import (
    BQ_PROJECT_ID,
    BQ_DATASET,
    BQ_TABLE,
    CSV_FILENAME,
    QUARANTINE_FILE,
    QUALITY_THRESHOLD,
    PROJECT_ROOT,
    PREFECT_API_URL
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



table_name = "test"


@task
def log_start():
    logger.info(f"[START] Pipeline started at {datetime.now()}")


@task
def extract():
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL
    os.environ["HADOOP_HOME"] = ""
    os.environ["SPARK_LOG4J_DIR"] = "."
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow"

    spark = SparkSession.builder \
        .appName("ETL") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.logConf", "false") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j2.configurationFile=log4j2.properties -Djdk.reflect.useDirectMethodHandle=false") \
        .getOrCreate()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir) 
    csv_path = PROJECT_ROOT / CSV_FILENAME  

    logger.info(f"[WAIT] Reading file: {csv_path}")

    if not csv_path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")

    spark_df = spark.read.csv(str(csv_path), header=True, inferSchema=True)

    logger.info(f'[OK] Read successful! Rows: {spark_df.count()}')
    logger.info('[WAIT] Processing Data...')
    spark_df.show(25, truncate=False)
    spark_df = spark_df.dropDuplicates(["OrderID"])
    spark_df.printSchema() 

    logger.info('Grouped Data:')
    groups = spark_df.groupBy("status").agg({"status": "count"}).filter(spark_df.Status != "Cancelled").show()

    # logger.info('[SAVE] Saving to Parquet...')
    # spark_df.write.mode("overwrite").parquet("clean_parquet") 


    pandas_df = spark_df.toPandas()

    # Drop duplicates
    if "OrderID" in pandas_df.columns:
        pandas_df = pandas_df.drop_duplicates(subset=["OrderID"])

    logger.info(pandas_df.dtypes)
    return pandas_df


@task
def run_quality_checks(pandas_df):
    logger.info("[QUALITY] Starting Data Quality Checks...")

    columns_to_check = [
        'CustomerName', 'Email', 'ProductName',
        'UnitPrice', 'TotalAmount', 'OrderDate',
        'Region', 'Status', 'Discount'
    ]

    report = {
        "total_rows": 0,
        "invalid_rows": 0,
        "clean_rows": 0,
        "quality_score": 0.0,
        "invalid_breakdown": {},
        "passed": False
    }

    try:
        if pandas_df is None or pandas_df.empty:
            logger.warning("[QUALITY] Empty DataFrame received")
            return pandas_df, report

        total_rows = len(pandas_df)
        invalid_rows_mask = pd.Series([False] * total_rows)

        # Check for invalid values like {something}
        for col in columns_to_check:
            if col in pandas_df.columns:
                col_invalid = pandas_df[col].astype(str).str.contains(r'\{.*\}', na=False)
                invalid_count = col_invalid.sum()
                if invalid_count > 0:
                    report["invalid_breakdown"][col] = int(invalid_count)
                invalid_rows_mask = invalid_rows_mask | col_invalid

        # Special check for Status
        if 'Status' in pandas_df.columns:
            unknown_status = (pandas_df['Status'] == '{UNKNOWN}')
            unknown_count = unknown_status.sum()
            if unknown_count > 0:
                report["invalid_breakdown"]["Unknown Status"] = int(unknown_count)
            invalid_rows_mask = invalid_rows_mask | unknown_status

        invalid_rows = int(invalid_rows_mask.sum())
        clean_rows = total_rows - invalid_rows
        quality_score = (clean_rows / total_rows) * 100 if total_rows > 0 else 0.0

        report.update({
            "total_rows": total_rows,
            "invalid_rows": invalid_rows,
            "clean_rows": clean_rows,
            "quality_score": round(quality_score, 1),
            "passed": quality_score >= QUALITY_THRESHOLD
        })

        # Logging
        logger.info(f"  • Total rows checked : {total_rows}")
        logger.info(f"  • Invalid rows       : {invalid_rows}")
        logger.info(f"  • Clean rows         : {clean_rows}")
        logger.info(f"  • Quality Score      : {quality_score:.1f}%")

        if report["invalid_breakdown"]:
            logger.info("\n[SEARCH] Invalid Data Breakdown:")
            for col, count in report["invalid_breakdown"].items():
                logger.info(f"  • {col}: {count}")

        # Quarantine decision
        if not report["passed"]:
            logger.warning(f"[STOP] Quality too low ({quality_score:.1f}%) → Sending to quarantine")
            pandas_df.to_csv(QUARANTINE_FILE, index=False)
            logger.warning(f"[SENT] Data saved to {QUARANTINE_FILE}")
        else:
            logger.info("[OK] Quality check passed")

    except Exception as e:
        logger.error(f"[ERROR] Quality check failed: {e}")
        report["passed"] = False

    return pandas_df, report


def alert_on_crash(task, task_run, state):
    print(f"Alert! Task {task.name} failed with error: {state.message}")

@task(retries=3, retry_delay_seconds=10, on_failure=[alert_on_crash])
def load_to_bigquery(data):
    logger.info(f"[{datetime.now()}] [SEND] Writing cleaned data to BigQuery...")
    try:
        destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
        pandas_gbq.to_gbq(
            data,
            destination_table=destination_table,
            project_id=BQ_PROJECT_ID,
            if_exists="replace"
        )
        logger.info(f"[OK] Successfully uploaded {len(data):,} rows to BigQuery!")
    except Exception as e:
        logger.error(f"[ALERT] Failed to write to BigQuery: {e}")



@flow(name="ETL Pipeline")
def etl_pipeline(target_date: str = None, dry_run: bool = False):
    if not target_date:
        target_date = datetime.utcnow().strftime("%Y-%m-%d")
    if dry_run:
        logger.info(f"[DRY RUN] Pipeline would process data for date: {target_date}")
        return

    logger.info(f"[START] Processing data for date: {target_date}")
    log_start()
    raw_data = extract()
    cleaned_data, quality_report = run_quality_checks(raw_data)
    if quality_report["passed"]:
        load_to_bigquery(cleaned_data)
    else:
        logger.warning("[SKIP] Skipping load because quality check failed")
    run_dbt_models()
    test_dbt_models()

    logger.info(f"[{datetime.now()}] [FINISH] Pipeline Completed Successfully!")


if __name__ == "__main__":
    etl_pipeline(target_date=datetime.utcnow().strftime("%Y-%m-%d"))
