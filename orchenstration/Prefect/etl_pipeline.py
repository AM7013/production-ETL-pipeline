import pandas as pd
import pandas_gbq
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import sys
import time
import sqlite3
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#Spark:

# Environment
os.environ["HADOOP_HOME"] = ""
os.environ["SPARK_LOG4J_DIR"] = "."
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow"

# Spark session
spark = SparkSession.builder \
    .appName("ETL") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.logConf", "false") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j2.configurationFile=log4j2.properties -Djdk.reflect.useDirectMethodHandle=false") \
    .getOrCreate()


pandas_df = None
spark_df = None
df = None # just in case
table_name = "test"
df_ge = None




logger.info("=" * 50) 
logger.info("[START] Starting pipeline...") # STARTING PIPELINE
logger.info("=" * 50)

logger.info(f"Current working directory: {os.getcwd()}")
logger.info(f"Script location: {__file__}")
logger.info(f"CSV should be at: {os.path.join(os.path.dirname(__file__), 'cleaned_data_v1.csv')}")

try: # connecting PostegresSQL with python files like CSV/JSON



    logger.info('[WAIT] pending...')
    load_dotenv()

    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url)
    logger.info(f"[OK] Connected to PostgreSQL!")
    logger.info(f'[WAIT] reading the file...')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'cleaned_data_v1.csv')

    spark_df = spark.read.csv(csv_path, header=True, inferSchema=True)

    logger.info(f'[OK] Read successful! Rows: {spark_df.count()}')
    logger.info('[WAIT] Processing Data...')
    spark_df.show(25, truncate=False)
    spark_df = spark_df.dropDuplicates(["OrderID"])
    spark_df.printSchema() 

    # GROUP BY
    logger.info('Grouped Data:')
    groups = spark_df.groupBy("status").agg({"status": "count"}).filter(spark_df.Status != "Cancelled").show()

    # logger.info('[SAVE] Saving to Parquet...')
    # spark_df.write.mode("overwrite").parquet("clean_parquet") 


    pandas_df = spark_df.toPandas()






    logger.info(f'[OK] Readed!!')

    pandas_df.to_csv(
        index=False,
        header=False,
        quoting=1,              
        escapechar='\\',         
        encoding='utf-8',
        chunksize=100000         
    )


    logger.info('[WAIT] Reading Data...')
    logger.info(f"[INFO] DATA QUALITY REPORT for table: {table_name}")
    
    logger.info("\n[SEARCH] COLUMN ANALYSIS:")
    for col in pandas_df.columns:
        null_pct = (pandas_df[col].isna().sum() / len(pandas_df)) * 100
        unique_vals = pandas_df[col].nunique()
        logger.info(f"  {col}: {null_pct:.1f}% null, {unique_vals} unique values") 
    logger.info(f"Rows processed: {len(pandas_df):,}")

    logger.info(f'[OK] congrats Pipeline worked')





except Exception as e: # Catch ANY error that happens in the try block / # ERROR HANDLING BLOCK
    logger.error(f'[ERROR] error: {e}') # 'e' contains error message instead of breaking the entire code
    logger.error('[FAIL] looks like the pipeline broke')

logger.info("----------- SEPARATOR -----------")
logger.info("----------- SEPARATOR -----------")


# ===================================================================================================
# =================================== DATA QUALITY CHECKS & METRICS =================================
# ===================================================================================================

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


# ===================================================================================================
# =================================== DATA QUALITY CHECKS & METRICS =================================
# ===================================================================================================

except Exception as e:
    logger.error(f"[ERROR] Error during quality check: {e}")
    logger.warning("[ALERT] But pipeline will still attempt to load data (with potential issues)")

def connect_simple():
    try:
        engine = create_engine('...')
        return engine.connect()
    except:
        logger.warning("Failed once, trying again...")

        engine = create_engine('...')
        return engine.connect()


def track_pipeline_run(table_name, row_count, success=True):  #tracking pipeline with table_name, row_count and insuring success
    try: 
        logger.info('[WAIT] tracking pipeline...')
        logger.info("[OK] Pipeline tracking saved")
    except Exception as e:
        logger.error(f"[ERROR] Tracking failed: {e}")
        logger.info("   But main pipeline still worked! [OK]")

if df is None or df.empty:
    track_pipeline_run = pd.Series([]) # calling the tracking pipeline function with empty series if df is None or empty to avoid errors in tracking
else:
    track_pipeline_run = pd.Series([False] * len(pandas_df))

logger.info("----------- SEPARATOR -----------")
logger.info("----------- SEPARATOR -----------")

try: 
    with engine.connect() as conn:
        check_query = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        result = conn.execute(check_query)
        table_exists = result.scalar()

        if not table_exists:
            logger.info(f"[CREATE] Table '{table_name}' does not exist. Creating it...") 
            logger.info(f"[OK] Table '{table_name}' created successfully!")
        else:
            logger.info(f"[INFO] Table '{table_name}' already exists. Skipping creation.")

    logger.info(f"[LOAD] Loading data into '{table_name}'...")
    pandas_df.to_csv(
        index=False,
        header=False,
        quoting=1,              
        escapechar='\\',         
        encoding='utf-8',
        chunksize=100000         
    )
    logger.info("[OK] Data loaded!")


except Exception as e:
    logger.error(f"[ERROR] Error during table check/creation or data loading: {e}")
    logger.error(f"[FAIL] Pipeline failed here.")

  

# ==================================================================================================================
# ===============================PORTFOLIO OPTIMIZATION EXAMPLES (UNCOMMENT TO USE)===============================
# ==================================================================================================================


# with engine.connect() as conn:
#     logger.info("[CLEAN] Cleaning up duplicates...") 
#     conn.execute(text(f"""
#         DELETE FROM {table_name} t1
#         WHERE t1.ctid != (
#             SELECT MIN(t2.ctid)
#             FROM {table_name} t2
#             WHERE t2."OrderID" = t1."OrderID"
#         )
#     """))
#     conn.commit()
#     logger.info("[SPARKLE] Duplicates removed! Table is now clean.")


# logger.info("[PROCESS] UNIQUE is being added...")
# with engine.connect() as conn:
#     conn.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT unique_order_id UNIQUE (\"OrderID\");"))
#     conn.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT unique_order_date UNIQUE (\"OrderDate\");"))
#     conn.commit()
#     logger.info(f"[PROCESS] UNIQUE CONSTRAINT added to the table")


# logger.info('[ADD] Adding Indexes to improve query performance...')
# with engine.connect() as conn:
#     conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_date ON {table_name} (\"OrderDate\");"))
#     conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_region ON {table_name} (\"Region\");"))
#     conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_customer ON {table_name} (\"CustomerName\");"))
#     conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_status ON {table_name} (\"OrderID\" ,\"Status\");"))
#     conn.commit()
#     logger.info(f"[HAND] Indexes created on columns: OrderDate, Region, CustomerName, OrderID+Status")

# logger.info("[FILES] Indexes added!")

# logger.info("\n[SEARCH] Testing SELECT query from the DB...")

# with engine.connect() as conn:
#     result = conn.execute(text("SELECT * FROM test limit 10;"))
#     logger.info("[INFO] Data Base:")
#     for row in result:
#         logger.info(row)

#     filtered_pandas_df = pd.read_sql(text("SELECT * FROM test WHERE \"OrderDate\" = '2024-01-15';"), conn)
#     logger.info("\nRows where OrderDate = '2024-01-15':")
#     logger.info(filtered_pandas_df)

# logger.info("\n[SEARCH] Analyzing Query Performance with EXPLAIN ANALYZE...")
# with engine.connect() as conn:
#     query = """ 
#     EXPLAIN ANALYZE
#     SELECT * FROM test WHERE "OrderDate" = '2024-01-15';
#      """
#     result = conn.execute(text(query))
#     logger.info("\n[METRIC] Query Execution Plan:")
#     for row in result:
#         logger.info(row[0])

# logger.info("\n[ADD] Adding NOT NULL CONSTRAINT to critical columns...")
# with engine.connect() as conn:
#     conn.execute(text("""
#         CREATE TABLE IF NOT EXISTS test (
#             "OrderID" TEXT NOT NULL,
#             "CustomerName" TEXT,
#             "Email" TEXT,
#             "ProductName" TEXT,
#             "Quantity" INTEGER,
#             "UnitPrice" TEXT,
#             "TotalAmount" TEXT,
#             "OrderDate" DATE NOT NULL,
#             "Region" TEXT,
#             "Status" TEXT,
#             "Discount" TEXT
#         )
#     """))
#     logger.info(f"[SHIELD] NOT NULL CONSTRAINT added to the table")

logger.info("----------- SEPARATOR -----------")
logger.info("----------- SEPARATOR -----------")



logger.info("[SEND] Writing cleaned data to BigQuery...")
try:
    pandas_gbq.to_gbq(
        pandas_df,                                  
        destination_table="my_etl_data.cleaned_data_from_spark",
        project_id="production-etl-pipeline",
        if_exists="replace"
    )
    logger.info(f"[OK] Successfully uploaded {len(pandas_df):,} rows to BigQuery!")
except Exception as e:
    logger.warning(f"[ALERT] Failed to write to BigQuery: {e}")



def time_tracking():
    logger.info('[CALC] Tracking Timeline...')
    start_time = time.perf_counter()
    time.sleep(0.10)
    end_time = time.perf_counter()
    logger.info('[CALC] Calculating...')
    return end_time - start_time    

logger.info(f'[TIME] Time Taken is...{time_tracking():.6f}')

logger.info("=" * 50)
logger.info("[FINISH] Pipeline Done:") 
logger.info("=" * 50)
