from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional
import io
import csv

import pandas as pd
import psycopg2
import pandas_gbq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from prefect import task

from config import get_postgres_url, TABLE_NAME, BQ_PROJECT_ID, BQ_DATASET, BQ_TABLE

logger = logging.getLogger(__name__)

_engine: Engine = None

# Natural key used for idempotent loads
NATURAL_KEY = "OrderID"


def _get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(get_postgres_url(), hide_parameters=True)
    return _engine


def alert_on_crash(task, task_run, state) -> None:
    logger.error(f"[ALERT] Task {task.name} failed! Exception: {state.message}")


def _preflight_postgres_check() -> bool:
    try:
        conn = psycopg2.connect(get_postgres_url())
        conn.close()
        logger.info("[OK] Postgres preflight check passed.")
        return True
    except Exception as e:
        logger.error(f"[ERROR] Postgres preflight check failed: {e}")
        return False


def _connect_with_retry(engine: Engine, retries: int = 1):
    last_error = None
    for attempt in range(retries + 1):
        try:
            return engine.connect()
        except Exception as e:
            last_error = e
            if attempt < retries:
                logger.warning(f"[RETRY] Postgres connect failed ({e}), retrying...")
    raise last_error


def new_run_id() -> str:
    """One run_id generated per flow run, threaded through storage + tracking."""
    return str(uuid.uuid4())


def _with_run_metadata(pandas_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """Tags every row with the run that loaded it."""
    tagged = pandas_df.copy()
    tagged["run_id"] = run_id
    tagged["loaded_at"] = datetime.now(timezone.utc)
    return tagged


def _delete_existing_keys_via_temp(conn, table_name: str, keys: list, key_col: str = "OrderID") -> int:
    if not keys:
        return 0

    # 1. Create temp table
    conn.execute(text("CREATE TEMP TABLE _idempotent_keys (id TEXT PRIMARY KEY) ON COMMIT DROP"))

    # 2. Bulk insert the keys (fast)
    #    Use executemany or a simple loop in reasonable chunks
    chunk_size = 10_000
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i:i + chunk_size]
        conn.execute(
            text("INSERT INTO _idempotent_keys (id) VALUES " + ",".join(f"(:k{j})" for j in range(len(chunk)))),
            {f"k{j}": v for j, v in enumerate(chunk)},
        )

    # 3. Delete using the temp table (this is what scales)
    result = conn.execute(text(f'''
        DELETE FROM {table_name}
        WHERE "{key_col}" IN (SELECT id FROM _idempotent_keys)
    '''))
    return result.rowcount


def _copy_to_postgres(engine, df: pd.DataFrame, table_name: str):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            columns = ",".join(f'"{c}"' for c in df.columns)
            cur.copy_expert(
                f'COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT csv, NULL \'\\N\')',
                buf,
            )
        raw.commit()
    finally:
        raw.close()

@task(name="Postgres Load", retries=2, retry_delay_seconds=5, on_failure=[alert_on_crash])
def load_to_postgres(
    pandas_df: pd.DataFrame,
    run_id: str,
    table_name: Optional[str] = None,
    idempotent: bool = True,
) -> None:
    table_name = table_name or TABLE_NAME

    if pandas_df is None or pandas_df.empty:
        logger.warning("[SKIP] No data to load into Postgres.")
        return

    if NATURAL_KEY not in pandas_df.columns:
        raise ValueError(
            f"[LOAD] Natural key '{NATURAL_KEY}' missing from DataFrame – "
            f"cannot perform idempotent load."
        )

    if not _preflight_postgres_check():
        raise ConnectionError("Postgres preflight check failed - see logs above.")

    engine = _get_engine()
    tagged_df = _with_run_metadata(pandas_df, run_id)

    with _connect_with_retry(engine) as conn:
        exists = conn.execute(
            text(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :table_name)"
            ),
            {"table_name": table_name},
        ).scalar()

        if not exists:
            logger.info(f"[CREATE] Table '{table_name}' does not exist – creating via to_sql...")
            # One-time create (schema inference). Subsequent runs use COPY.
            tagged_df.head(0).to_sql(table_name, engine, if_exists="replace", index=False)
            exists = True

        if idempotent and exists:
            keys = tagged_df[NATURAL_KEY].dropna().unique().tolist()
            deleted = _delete_existing_keys_via_temp(conn, table_name, keys)  # ← correct name
            conn.commit()
            logger.info(
                f"[IDEMPOTENT] Removed {deleted:,} existing rows for "
                f"{len(keys):,} incoming {NATURAL_KEY}s before append."
            )

    logger.info(
        f"[LOAD] Loading {len(tagged_df):,} rows into '{table_name}' "
        f"(run_id={run_id}, idempotent={idempotent}) via COPY..."
    )
    try:
        _copy_to_postgres(engine, tagged_df, table_name)          # ← use COPY
    except Exception as e:
        logger.error(f"[ERROR] Postgres load failed: {type(e).__name__}: {str(e)[:500]}")
        logger.error(
            f"[HINT] If this mentions a missing column, '{table_name}' likely "
            f"exists from an earlier run with an older schema. Either ALTER TABLE "
            f"to add the column(s) or drop/rename the table so it is recreated."
        )
        raise

    logger.info("[OK] Data loaded into Postgres!")


@task(name="BigQuery Load", retries=3, retry_delay_seconds=10, on_failure=[alert_on_crash])
def load_to_bigquery(
    pandas_df: pd.DataFrame,
    run_id: str,
    idempotent: bool = True,
) -> None:
    if pandas_df is None or pandas_df.empty:
        logger.warning("[SKIP] No data to load into BigQuery.")
        return

    if NATURAL_KEY not in pandas_df.columns:
        raise ValueError(
            f"[LOAD] Natural key '{NATURAL_KEY}' missing from DataFrame – "
            f"cannot perform idempotent load."
        )

    tagged_df = _with_run_metadata(pandas_df, run_id)
    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
    project_id = BQ_PROJECT_ID

    if not idempotent:
        logger.info(f"[SEND] Appending to BigQuery -> {destination_table} (run_id={run_id})...")
        pandas_gbq.to_gbq(
            tagged_df,
            destination_table=destination_table,
            project_id=project_id,
            if_exists="append",
        )
        logger.info(f"[OK] Uploaded {len(tagged_df):,} rows to BigQuery")
        return

    # Idempotent path for BigQuery
    temp_table = f"{BQ_DATASET}._tmp_{BQ_TABLE}_{run_id.replace('-', '')[:12]}"

    logger.info(
        f"[SEND] Idempotent load to BigQuery -> {destination_table} "
        f"(via temp {temp_table}, run_id={run_id})..."
    )

    try:
        # 1. Write batch to temp table
        pandas_gbq.to_gbq(
            tagged_df,
            destination_table=temp_table,
            project_id=project_id,
            if_exists="replace",
        )

        # 2. DELETE matching keys + INSERT new rows
        sql = f"""
        BEGIN
          DELETE FROM `{project_id}.{destination_table}`
          WHERE {NATURAL_KEY} IN (
            SELECT {NATURAL_KEY} FROM `{project_id}.{temp_table}`
          );

          INSERT INTO `{project_id}.{destination_table}`
          SELECT * FROM `{project_id}.{temp_table}`;

          DROP TABLE `{project_id}.{temp_table}`;
        END;
        """

        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        job = client.query(sql)
        job.result()

        logger.info(
            f"[OK] Idempotent upload of {len(tagged_df):,} rows to BigQuery -> {destination_table}"
        )

    except Exception as e:
        logger.error(f"[ALERT] BigQuery idempotent load failed: {e}")
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=project_id)
            client.delete_table(f"{project_id}.{temp_table}", not_found_ok=True)
        except Exception:
            pass
        raise

