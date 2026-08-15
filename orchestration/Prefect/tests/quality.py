from __future__ import annotations

import logging
from typing import Tuple

import pandas as pd
from prefect import task

from config import QUALITY_THRESHOLD, QUARANTINE_FILE

logger = logging.getLogger(__name__)

# Columns scanned for placeholder / null / empty issues
COLUMNS_TO_CHECK = [
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

ALLOWED_STATUSES = {
    "Pending",
    "Shipped",
    "Delivered",
    "Cancelled",
    "Processing",
    "Returned",
}


def _empty_report() -> dict:
    return {
        "total_rows": 0,
        "invalid_rows": 0,
        "clean_rows": 0,
        "quality_score": 0.0,
        "invalid_breakdown": {},
        "passed": False,
    }


@task(name="Data Quality Auditor")
def run_quality_checks(pandas_df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    logger.info("[QUALITY] Starting Data Quality Audit...")

    report = _empty_report()

    if pandas_df is None or pandas_df.empty:
        logger.warning("[QUALITY] Empty DataFrame received!")
        return pandas_df, report

    total_rows = len(pandas_df)
    invalid_mask = pd.Series(False, index=pandas_df.index)
    reasons: dict[str, pd.Series] = {}

    def _flag(mask: pd.Series, label: str) -> None:
        nonlocal invalid_mask
        if mask.any():
            count = int(mask.sum())
            report["invalid_breakdown"][label] = report["invalid_breakdown"].get(label, 0) + count
            reasons[label] = mask
            invalid_mask = invalid_mask | mask

    # 1. Placeholder patterns {something}
    for col in COLUMNS_TO_CHECK:
        if col not in pandas_df.columns:
            continue
        mask = pandas_df[col].astype(str).str.contains(r"\{.*\}", na=False)
        _flag(mask, f"placeholder:{col}")

    # 2. Explicit {UNKNOWN} status (kept for backwards compatibility with older data)
    if "Status" in pandas_df.columns:
        mask = pandas_df["Status"].astype(str) == "{UNKNOWN}"
        _flag(mask, "Unknown Status")

    # 3. Status not in allowed set (and not already a placeholder)
    if "Status" in pandas_df.columns:
        status_str = pandas_df["Status"].astype(str)
        mask = ~status_str.isin(ALLOWED_STATUSES) & ~status_str.str.contains(r"\{.*\}", na=False)
        # also treat pure empty / nan as invalid
        mask = mask | status_str.isin(["", "nan", "None", "NaN"])
        _flag(mask, "invalid_status")

    # 4. Nulls / empty strings on critical text columns
    for col in ["CustomerName", "Email", "ProductName", "OrderDate", "Region"]:
        if col not in pandas_df.columns:
            continue
        series = pandas_df[col]
        mask = series.isna() | (series.astype(str).str.strip() == "")
        _flag(mask, f"null_or_empty:{col}")

    # 5. Email basic shape
    if "Email" in pandas_df.columns:
        email = pandas_df["Email"].astype(str)
        mask = ~email.str.contains("@", na=False) & email.notna() & (email.str.strip() != "")
        # don't double-count pure empties already caught above
        _flag(mask, "bad_email")

    # 6. Numeric non-negative checks
    for col in ["UnitPrice", "TotalAmount"]:
        if col not in pandas_df.columns:
            continue
        numeric = pd.to_numeric(pandas_df[col], errors="coerce")
        mask = numeric.isna() | (numeric < 0)
        _flag(mask, f"bad_numeric:{col}")

    # 7. Discount in a sensible range (accept 0–1 or 0–100)
    if "Discount" in pandas_df.columns:
        disc = pd.to_numeric(pandas_df["Discount"], errors="coerce")
        mask = disc.isna() | (disc < 0) | (disc > 100)
        _flag(mask, "bad_discount")

    invalid_rows = int(invalid_mask.sum())
    clean_rows = total_rows - invalid_rows
    quality_score = (clean_rows / total_rows) * 100 if total_rows > 0 else 0.0

    report.update(
        {
            "total_rows": total_rows,
            "invalid_rows": invalid_rows,
            "clean_rows": clean_rows,
            "quality_score": round(quality_score, 1),
            "passed": quality_score >= QUALITY_THRESHOLD,
        }
    )

    logger.info(f"  • Total rows : {total_rows:,}")
    logger.info(f"  • Quality    : {quality_score:.1f}% (Threshold: {QUALITY_THRESHOLD}%)")
    if report["invalid_breakdown"]:
        logger.info(f"  • Breakdown  : {report['invalid_breakdown']}")

    # Build a human-readable reason string per invalid row
    if invalid_rows > 0:
        reason_series = pd.Series("", index=pandas_df.index, dtype=object)
        for label, mask in reasons.items():
            reason_series = reason_series.where(~mask, reason_series + label + "; ")
        reason_series = reason_series.str.rstrip("; ")

        bad_df = pandas_df.loc[invalid_mask].copy()
        bad_df["quarantine_reason"] = reason_series.loc[invalid_mask]

        if not report["passed"]:
            logger.warning(
                f"[STOP] Quality low ({quality_score:.1f}%) → Quarantine engaged "
                f"({invalid_rows:,} invalid rows)."
            )
            try:
                bad_df.to_csv(QUARANTINE_FILE, index=False)
                logger.warning(f"[SENT] Quarantine saved to: {QUARANTINE_FILE}")
            except OSError as e:
                logger.error(f"[ERROR] Could not write quarantine file '{QUARANTINE_FILE}': {e}")
        else:
            # Quality still passed overall, but we log that some rows were dirty
            logger.info(
                f"[INFO] {invalid_rows:,} rows had quality issues but overall score "
                f"still met threshold."
            )
    else:
        logger.info("[OK] Data Quality checks passed successfully.")

    # Return only the clean rows when we pass (so downstream never sees dirty data).
    # On failure we still return the original frame so callers can inspect it.
    if report["passed"]:
        clean_df = pandas_df.loc[~invalid_mask].copy()
        return clean_df, report

    return pandas_df, report
