import logging
import os
from datetime import datetime
import pandas as pd
import pandas_gbq
from prefect import flow, task
from pyspark.sql import SparkSession

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
    os.environ["PREFECT_API_URL"] = "Your URL here"
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
    possible_csv_files = ['cleaned_data_test.csv', 'cleaned_data_v1.csv']
    
    csv_path = None
    for filename in possible_csv_files:
        path = os.path.join(os.getcwd(), filename)
        if os.path.exists(path):
            csv_path = path
            break
        
        path = os.path.join(os.getcwd(), 'Sample', filename)
        if os.path.exists(path):
            csv_path = path
            break

    if not csv_path:
        raise FileNotFoundError(f"Could not find any CSV file. Tried: {possible_csv_files}") 

    logger.info(f"[WAIT] Reading file: {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File not found: {csv_path}")


    spark_df = spark.read.csv(csv_path, header=True, inferSchema=True)

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
def transform(pandas_df):
    logger.info("[WAIT] Running Data Quality Checks...")
    logger.info("[OK] Quality checks completed")
    logger.info("\n[CRITICAL] CRITICAL METRICS:")
    columns_to_check = [
        'CustomerName', 'Email', 'ProductName', 
        'UnitPrice', 'TotalAmount', 'OrderDate',
        'Region', 'Status', 'Discount'
    ]

    try:
        if pandas_df is None or pandas_df.empty:
            invalid_rows_mask = pd.Series([])
        else:
            invalid_rows_mask = pd.Series([False] * len(pandas_df))

        for col in columns_to_check:
            if col in pandas_df.columns:
                invalid_rows_mask = invalid_rows_mask | pandas_df[col].astype(str).str.contains('{.*}', na=False)

        if 'Status' in pandas_df.columns:
            invalid_rows_mask = invalid_rows_mask | (pandas_df['Status'] == '{UNKNOWN}')

        invalid_rows = invalid_rows_mask.sum()
        total_rows = len(pandas_df)

        quality_score = ((total_rows - invalid_rows) / total_rows) * 100

        logger.info(f"  • Total rows checked: {total_rows}")
        logger.info(f"  • Rows with ANY invalid data: {invalid_rows}")
        logger.info(f"  • Clean rows: {total_rows - invalid_rows}")

        logger.info("\n[SEARCH] INVALID DATA BREAKDOWN:")
        for col in columns_to_check:
            if col in pandas_df.columns:
                invalid_count = pandas_df[col].astype(str).str.contains('{.*}', na=False).sum()
                if invalid_count > 0:
                    logger.info(f"  • Invalid {col}: {invalid_count}")

        if 'Status' in pandas_df.columns:
            unknown_status = (pandas_df['Status'] == '{UNKNOWN}').sum()
            if unknown_status > 0:
                logger.info(f"  • Unknown Status: {unknown_status}")

        logger.info(f"\n[METRIC] DATA QUALITY SCORE: {quality_score:.1f}% clean")

        if quality_score < 80.0:
            logger.warning(f"\n[STOP] Data Quality is too low ({quality_score:.1f}%)")
            logger.warning("[ALERT] Pipeline is in low clean state")
            logger.warning('[SEND] Sending The Pipeline to "quarantine_zone.csv..."')
            pandas_df.to_csv('quarantine_zone.csv', index=False)
            logger.warning('[SENT] Pipeline sent to "quarantine_zone.csv"')
        else:
            logger.info("\n[OK] QUALITY CHECK PASSED! Proceeding to load...")

    except Exception as e:
        logger.error(f"[ERROR] Error during quality check: {e}")
        logger.warning("[ALERT] But pipeline will still attempt to load data (with potential issues)")

    return pandas_df


def alert_on_crash(task, task_run, state):
    print(f"Alert! Task {task.name} failed with error: {state.message}")

@task(retries=3, retry_delay_seconds=10, on_failure=[alert_on_crash])
def load_to_bigquery(data):
    logger.info(f"[{datetime.now()}] [SEND] Writing cleaned data to BigQuery...")
    try:
        pandas_gbq.to_gbq(
            data,
            destination_table="my_etl_data.cleaned_data_from_spark",
            project_id="production-etl-pipeline",
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
    cleaned_data = transform(raw_data)
    load_to_bigquery(cleaned_data)
    logger.info(f"[{datetime.now()}] [FINISH] Pipeline Completed Successfully!")


if __name__ == "__main__":
    etl_pipeline(target_date=datetime.utcnow().strftime("%Y-%m-%d"))
