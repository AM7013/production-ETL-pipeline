import logging
from typing import Tuple
import pandas as pd
from prefect import task

from config import QUALITY_THRESHOLD, QUARANTINE_FILE

logger = logging.getLogger(__name__)


@task(name="Data Quality Auditor")
def run_quality_checks(pandas_df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    logger.info("[QUALITY] Starting Data Quality Audit...")

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

    if pandas_df is None or pandas_df.empty:
        logger.warning("[QUALITY] Empty DataFrame received!")
        return pandas_df, report

    total_rows = len(pandas_df)
    invalid_rows_mask = pd.Series([False] * total_rows)

    # Regex scan for raw placeholders (e.g. {something})
    for col in columns_to_check:
        if col in pandas_df.columns:
            col_invalid = pandas_df[col].astype(str).str.contains(r'\{.*\}', na=False)
            invalid_count = col_invalid.sum()
            if invalid_count > 0:
                report["invalid_breakdown"][col] = int(invalid_count)
            invalid_rows_mask = invalid_rows_mask | col_invalid

    # Check for Status placeholder values
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

    logger.info(f"  \u2022 Total rows : {total_rows:,}")
    logger.info(f"  \u2022 Quality    : {quality_score:.1f}% (Threshold: {QUALITY_THRESHOLD}%)")

    if not report["passed"]:
        logger.warning(f"[STOP] Quality low ({quality_score:.1f}%) \u2192 Quarantine engaged.")
        try:
            pandas_df.to_csv(QUARANTINE_FILE, index=False)
            logger.warning(f"[SENT] Quarantine saved to: {QUARANTINE_FILE}")
        except OSError as e:
            logger.error(f"[ERROR] Could not write quarantine file '{QUARANTINE_FILE}': {e}")
    else:
        logger.info("[OK] Data Quality checks passed successfully.")

    return pandas_df, report