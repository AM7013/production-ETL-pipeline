from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "OrderID",
    "CustomerName",
    "Email",
    "ProductName",
    "UnitPrice",
    "TotalAmount",
    "OrderDate",
    "Region",
    "Status",
    "Discount",
]

# Columns we expect to be numeric (or coercible to numeric)
NUMERIC_COLUMNS = ["UnitPrice", "TotalAmount", "Discount"]


@task(name="Validate Schema")
def validate_schema(
    pandas_df: pd.DataFrame,
    required_columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    required_columns = required_columns or REQUIRED_COLUMNS

    if pandas_df is None:
        raise ValueError(
            "[SCHEMA] Received None instead of a DataFrame - extraction likely failed silently."
        )

    missing = [c for c in required_columns if c not in pandas_df.columns]
    if missing:
        logger.error(f"[SCHEMA] Missing required columns: {', '.join(missing)}")
        logger.error(f"[SCHEMA] Columns present: {', '.join(map(str, pandas_df.columns))}")
        raise ValueError(f"Source data is missing required columns: {', '.join(missing)}")

    # Soft dtype check – warn + attempt coercion rather than hard-fail,
    # so a string "9.99" still gets through.
    for col in NUMERIC_COLUMNS:
        if col not in pandas_df.columns:
            continue
        coerced = pd.to_numeric(pandas_df[col], errors="coerce")
        bad = coerced.isna() & pandas_df[col].notna()
        if bad.any():
            logger.warning(
                f"[SCHEMA] Column '{col}' has {int(bad.sum())} values that cannot "
                f"be coerced to numeric. Downstream quality rules will flag them."
            )

    logger.info(f"[SCHEMA] All {len(required_columns)} required columns present.")
    return pandas_df