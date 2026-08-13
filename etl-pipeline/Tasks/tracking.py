"""
Timing and run-tracking utilities.

track_pipeline_run() now actually persists to a `pipeline_runs` table in
Postgres instead of only logging. The monolithic script's version of this
function was also dead code for a different reason - it got shadowed by
`track_pipeline_run = pd.Series([...])` right after being defined, so it
never even ran. Both problems (never runs, and wouldn't have persisted
anything if it did) are fixed here.
"""

import time
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import get_postgres_url

logger = logging.getLogger(__name__)

_engine: Engine = None

_CREATE_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    table_name TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    success BOOLEAN NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);
"""

_INSERT_RUN = """
INSERT INTO pipeline_runs (run_id, table_name, row_count, success, recorded_at)
VALUES (:run_id, :table_name, :row_count, :success, :recorded_at)
ON CONFLICT (run_id) DO NOTHING;
"""


def _get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(get_postgres_url(), hide_parameters=True)
    return _engine


@contextmanager
def time_tracking(label: str = "Pipeline"):
    """Times any block of code. Usage: `with time_tracking("Extract"): ...`"""
    logger.info(f"[CALC] Tracking timeline for: {label}...")
    start_time = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start_time
        logger.info(f"[TIME] {label} took {elapsed:.3f}s")


def track_pipeline_run(run_id: str, table_name: str, row_count: int, success: bool = True) -> None:
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            conn.execute(text(_CREATE_RUNS_TABLE))
            conn.execute(
                text(_INSERT_RUN),
                {
                    "run_id": run_id,
                    "table_name": table_name,
                    "row_count": row_count,
                    "success": success,
                    "recorded_at": datetime.now(timezone.utc),
                },
            )
            conn.commit()
        logger.info(f"[OK] Pipeline run {run_id} tracked ({row_count:,} rows, success={success})")
    except Exception as e:
        logger.error(f"[ERROR] Tracking failed: {e}")
        logger.info("   But main pipeline still worked! [OK]")