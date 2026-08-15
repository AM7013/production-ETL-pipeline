from __future__ import annotations

import pandas as pd
import pytest

from tasks.schema_validation import validate_schema, REQUIRED_COLUMNS


def _full_row(**overrides):
    row = {
        "OrderID": 1,
        "CustomerName": "Jane",
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


def test_valid_schema_passes():
    df = pd.DataFrame([_full_row()])
    result = validate_schema.fn(df)
    assert list(result.columns) == list(df.columns)


def test_missing_column_raises():
    row = {k: v for k, v in _full_row().items() if k != "Email"}
    df = pd.DataFrame([row])
    with pytest.raises(ValueError, match="missing required columns"):
        validate_schema.fn(df)


def test_none_input_raises():
    with pytest.raises(ValueError, match="Received None"):
        validate_schema.fn(None)


def test_extra_columns_are_fine():
    row = _full_row(ExtraCol="hello")
    df = pd.DataFrame([row])
    result = validate_schema.fn(df)
    assert "ExtraCol" in result.columns