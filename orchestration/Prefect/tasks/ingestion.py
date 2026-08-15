import os
import logging
import pandas as pd
from prefect import task
from pyspark.sql import SparkSession

import config

logger = logging.getLogger(__name__)

_spark = None


def _get_spark() -> SparkSession:
    """Lazily creates a single shared SparkSession instead of one at import time."""
    global _spark
    if _spark is None:
        os.environ.setdefault("HADOOP_HOME", "")
        os.environ.setdefault("SPARK_LOG4J_DIR", ".")
        os.environ.setdefault("PYSPARK_PYTHON", "python")
        os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Djava.security.manager=allow")

        _spark = (
            SparkSession.builder
            .appName("ETL")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.logConf", "false")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j2.configurationFile=log4j2.properties "
                "-Djdk.reflect.useDirectMethodHandle=false",
            )
            .getOrCreate()
        )
    return _spark


def stop_spark() -> None:
    global _spark
    if _spark is not None:
        logger.info("[SPARK] Stopping SparkSession...")
        _spark.stop()
        _spark = None


@task(name="Extract & Dedupe", retries=2, retry_delay_seconds=5)
def extract_data(csv_filename: str = None) -> pd.DataFrame:
    csv_filename = csv_filename or config.CSV_FILENAME
    csv_path = str(config.PROJECT_ROOT / csv_filename)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"[EXTRACT] Source CSV not found at '{csv_path}'. "
            f"Check CSV_FILENAME in your .env, or pass a different path explicitly."
        )

    spark = _get_spark()

    logger.info(f"[WAIT] Reading CSV from {csv_path}...")
    spark_df = spark.read.csv(csv_path, header=True, inferSchema=True)
    logger.info(f"[OK] Read successful! Rows: {spark_df.count()}")

    spark_df = spark_df.dropDuplicates(["OrderID"])


    status_col = next((c for c in spark_df.columns if c.lower() == "status"), None)
    if status_col:
        logger.info("[INFO] Order counts by status (excluding Cancelled):")
        spark_df.groupBy(status_col).count().filter(spark_df[status_col] != "Cancelled").show()

    pandas_df = spark_df.toPandas()
    logger.info(f"[OK] Converted to pandas: {len(pandas_df):,} rows")
    return pandas_df


@task(name="Profile Columns")
def profile_columns(pandas_df: pd.DataFrame) -> dict:
    if pandas_df is None or pandas_df.empty:
        logger.warning("[PROFILE] No data to profile.")
        return {}

    profile = {}
    logger.info("[SEARCH] COLUMN ANALYSIS:")
    for col in pandas_df.columns:
        null_pct = (pandas_df[col].isna().sum() / len(pandas_df)) * 100
        unique_vals = pandas_df[col].nunique()
        profile[col] = {"null_pct": round(null_pct, 1), "unique": int(unique_vals)}
        logger.info(f"  {col}: {null_pct:.1f}% null, {unique_vals} unique values")

    return profile
