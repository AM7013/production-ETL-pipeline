from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from tasks.quality import run_quality_checks, ALLOWED_STATUSES


def make_row(**overrides):
    row = {
        "OrderID": 1001,
        "CustomerName": "Jane Doe",
        "Email": "jane@example.com",
        "ProductName": "Widget",
        "UnitPrice": 9.99,
        "TotalAmount": 19.98,
        "OrderDate": "2026-01-01",
        "Region": "EU",
        "Status": "Shipped",
        "Discount": 0,
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Core pass / fail behaviour
# ---------------------------------------------------------------------------

def test_all_clean_data_passes():
    df = pd.DataFrame([make_row(OrderID=i) for i in range(10)])
    result_df, report = run_quality_checks.fn(df)

    assert report["passed"] is True
    assert report["quality_score"] == 100.0
    assert report["invalid_rows"] == 0
    assert report["invalid_breakdown"] == {}
    assert len(result_df) == 10


def test_all_invalid_data_fails():
    df = pd.DataFrame([make_row(Status="{UNKNOWN}", OrderID=i) for i in range(10)])
    result_df, report = run_quality_checks.fn(df)

    assert report["passed"] is False
    assert report["quality_score"] == 0.0
    assert report["invalid_rows"] == 10
    assert "Unknown Status" in report["invalid_breakdown"] or "placeholder:Status" in report["invalid_breakdown"]
    # FIX: even on total failure, returned rows must be the clean subset
    # (here, zero), never the raw unvalidated frame.
    assert len(result_df) == 0


def test_placeholder_pattern_flags_row():
    df = pd.DataFrame([
        make_row(OrderID=1),
        make_row(OrderID=2, CustomerName="{MISSING}"),
    ])
    result_df, report = run_quality_checks.fn(df)

    assert report["invalid_rows"] == 1
    assert report["quality_score"] == 50.0
    assert any("CustomerName" in k for k in report["invalid_breakdown"])


def test_exactly_at_threshold_passes():
    rows = [make_row(OrderID=i) for i in range(8)]
    rows += [make_row(OrderID=100 + i, Status="{UNKNOWN}") for i in range(2)]
    df = pd.DataFrame(rows)
    result_df, report = run_quality_checks.fn(df)

    assert report["quality_score"] == 80.0
    assert report["passed"] is True
    assert len(result_df) == 8


def test_just_below_threshold_fails():
    rows = [make_row(OrderID=i) for i in range(7)]
    rows += [make_row(OrderID=100 + i, Status="{UNKNOWN}") for i in range(3)]
    df = pd.DataFrame(rows)
    result_df, report = run_quality_checks.fn(df)

    assert report["quality_score"] == 70.0
    assert report["passed"] is False
    # FIX: clean rows (7 of them) must still come back, not the raw frame.
    assert len(result_df) == 7


def test_empty_dataframe_does_not_crash():
    df = pd.DataFrame()
    result_df, report = run_quality_checks.fn(df)

    assert report["passed"] is False
    assert report["total_rows"] == 0


def test_none_input_does_not_crash():
    result_df, report = run_quality_checks.fn(None)

    assert report["passed"] is False
    assert report["total_rows"] == 0


def test_missing_optional_column_is_skipped_not_errored():
    row = {k: v for k, v in make_row().items() if k != "Discount"}
    df = pd.DataFrame([row])
    result_df, report = run_quality_checks.fn(df)

    assert report["passed"] is True
    assert "Discount" not in report["invalid_breakdown"]


# ---------------------------------------------------------------------------
# Strengthened rules
# ---------------------------------------------------------------------------

def test_null_customer_name_flagged():
    df = pd.DataFrame([
        make_row(OrderID=1),
        make_row(OrderID=2, CustomerName=None),
        make_row(OrderID=3, CustomerName=""),
    ])
    _, report = run_quality_checks.fn(df)

    assert report["invalid_rows"] >= 2
    assert any("CustomerName" in k for k in report["invalid_breakdown"])


def test_bad_email_flagged():
    df = pd.DataFrame([
        make_row(OrderID=1, Email="not-an-email"),
        make_row(OrderID=2, Email="good@example.com"),
    ])
    _, report = run_quality_checks.fn(df)

    assert report["invalid_rows"] == 1
    assert "bad_email" in report["invalid_breakdown"]


def test_negative_price_flagged():
    df = pd.DataFrame([
        make_row(OrderID=1, UnitPrice=-5.0),
        make_row(OrderID=2, UnitPrice=10.0),
    ])
    _, report = run_quality_checks.fn(df)

    assert report["invalid_rows"] == 1
    assert "bad_numeric:UnitPrice" in report["invalid_breakdown"]


def test_invalid_status_flagged():
    df = pd.DataFrame([
        make_row(OrderID=1, Status="Shipped"),
        make_row(OrderID=2, Status="FlyingToMoon"),
    ])
    _, report = run_quality_checks.fn(df)

    assert report["invalid_rows"] == 1
    assert "invalid_status" in report["invalid_breakdown"]


def test_allowed_statuses_pass():
    rows = [make_row(OrderID=i, Status=s) for i, s in enumerate(ALLOWED_STATUSES)]
    df = pd.DataFrame(rows)
    _, report = run_quality_checks.fn(df)

    assert report["invalid_rows"] == 0
    assert report["passed"] is True


def test_bad_discount_flagged():
    df = pd.DataFrame([
        make_row(OrderID=1, Discount=150),
        make_row(OrderID=2, Discount=-1),
        make_row(OrderID=3, Discount=0.15),
    ])
    _, report = run_quality_checks.fn(df)

    assert report["invalid_rows"] == 2
    assert "bad_discount" in report["invalid_breakdown"]


# ---------------------------------------------------------------------------
# Quarantine behaviour (includes the two bug-fix regression tests)
# ---------------------------------------------------------------------------

def test_quarantine_writes_only_invalid_rows(tmp_path, monkeypatch):
    quarantine_path = tmp_path / "quarantine_zone.csv"
    monkeypatch.setattr("tasks.quality.QUARANTINE_FILE", str(quarantine_path))
    monkeypatch.setattr("tasks.quality.QUALITY_THRESHOLD", 99.0)

    df = pd.DataFrame([
        make_row(OrderID=1),
        make_row(OrderID=2, Status="{UNKNOWN}"),
        make_row(OrderID=3, Email="bad-email"),
    ])
    _, report = run_quality_checks.fn(df)

    assert report["passed"] is False
    assert quarantine_path.exists()

    qdf = pd.read_csv(quarantine_path)
    assert len(qdf) == 2
    assert "quarantine_reason" in qdf.columns
    assert qdf["quarantine_reason"].notna().all()


def test_invalid_rows_still_quarantined_when_batch_passes(tmp_path, monkeypatch):
    quarantine_path = tmp_path / "quarantine_zone.csv"
    monkeypatch.setattr("tasks.quality.QUARANTINE_FILE", str(quarantine_path))
    monkeypatch.setattr("tasks.quality.QUALITY_THRESHOLD", 50.0)

    # 1 bad out of 3 -> 66% -> still passes overall
    df = pd.DataFrame([
        make_row(OrderID=1),
        make_row(OrderID=2),
        make_row(OrderID=3, Status="{UNKNOWN}"),
    ])
    result_df, report = run_quality_checks.fn(df)

    assert report["passed"] is True
    # The one bad row must be quarantined even though the batch passed.
    assert quarantine_path.exists()
    qdf = pd.read_csv(quarantine_path)
    assert len(qdf) == 1
    assert "quarantine_reason" in qdf.columns
    # Only clean rows come back for loading - the bad row is excluded.
    assert len(result_df) == 2


def test_clean_rows_still_returned_when_batch_fails(tmp_path, monkeypatch):
    quarantine_path = tmp_path / "quarantine_zone.csv"
    monkeypatch.setattr("tasks.quality.QUARANTINE_FILE", str(quarantine_path))
    monkeypatch.setattr("tasks.quality.QUALITY_THRESHOLD", 99.0)

    df = pd.DataFrame([
        make_row(OrderID=1),                          # clean
        make_row(OrderID=2, Status="{UNKNOWN}"),       # invalid
        make_row(OrderID=3, Email="bad-email"),        # invalid
    ])
    result_df, report = run_quality_checks.fn(df)

    assert report["passed"] is False
    # Even on failure, the returned frame must contain ONLY clean rows -
    # never the raw, unvalidated data.
    assert len(result_df) == 1
    assert result_df.iloc[0]["OrderID"] == 1
    assert quarantine_path.exists()
    qdf = pd.read_csv(quarantine_path)
    assert len(qdf) == 2
