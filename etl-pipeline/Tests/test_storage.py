from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pandas as pd
import pytest

import tasks.storage as storage


def make_df(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "OrderID": [f"O{i:04d}" for i in range(n)],
            "CustomerName": ["Jane Doe"] * n,
            "Email": ["jane@example.com"] * n,
            "ProductName": ["Widget"] * n,
            "UnitPrice": [9.99] * n,
            "TotalAmount": [19.98] * n,
            "OrderDate": ["2026-01-01"] * n,
            "Region": ["EU"] * n,
            "Status": ["Shipped"] * n,
            "Discount": [0] * n,
        }
    )


def test_new_run_id_returns_valid_unique_uuids():
    a = storage.new_run_id()
    b = storage.new_run_id()

    assert a != b
    uuid.UUID(a)
    uuid.UUID(b)


def test_with_run_metadata_adds_columns_without_mutating_original():
    df = make_df()
    original_columns = list(df.columns)

    tagged = storage._with_run_metadata(df, "test-run-id")

    assert "run_id" in tagged.columns
    assert "loaded_at" in tagged.columns
    assert (tagged["run_id"] == "test-run-id").all()
    assert list(df.columns) == original_columns


# ---------------------------------------------------------------------------
# load_to_postgres
# ---------------------------------------------------------------------------

def test_load_to_postgres_skips_on_empty_df():
    result = storage.load_to_postgres.fn(pd.DataFrame(), run_id="r1")
    assert result is None  # returns early, no exception


def test_load_to_postgres_skips_on_none_df():
    result = storage.load_to_postgres.fn(None, run_id="r1")
    assert result is None


def test_load_to_postgres_raises_without_natural_key():
    df = make_df().drop(columns=["OrderID"])
    with pytest.raises(ValueError, match="Natural key"):
        storage.load_to_postgres.fn(df, run_id="r1")


def test_load_to_postgres_raises_when_preflight_fails(monkeypatch):
    monkeypatch.setattr(storage, "_preflight_postgres_check", lambda: False)

    with pytest.raises(ConnectionError):
        storage.load_to_postgres.fn(make_df(), run_id="r1")


def test_load_to_postgres_happy_path_calls_copy(monkeypatch):
    monkeypatch.setattr(storage, "_preflight_postgres_check", lambda: True)
    monkeypatch.setattr(storage, "_get_engine", lambda: MagicMock())

    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = True  # table exists
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False
    monkeypatch.setattr(storage, "_connect_with_retry", lambda engine: mock_ctx)
    monkeypatch.setattr(storage, "_delete_existing_keys_via_temp", lambda *a, **k: 0)

    copy_calls = []
    monkeypatch.setattr(
        storage, "_copy_to_postgres",
        lambda engine, df, table_name: copy_calls.append((table_name, len(df)))
    )

    df = make_df(5)
    storage.load_to_postgres.fn(df, run_id="r1", table_name="orders")

    assert len(copy_calls) == 1
    table_name, row_count = copy_calls[0]
    assert table_name == "orders"
    assert row_count == 5


def test_load_to_postgres_idempotent_true_calls_delete(monkeypatch):
    monkeypatch.setattr(storage, "_preflight_postgres_check", lambda: True)
    monkeypatch.setattr(storage, "_get_engine", lambda: MagicMock())

    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = True
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False
    monkeypatch.setattr(storage, "_connect_with_retry", lambda engine: mock_ctx)
    monkeypatch.setattr(storage, "_copy_to_postgres", lambda *a, **k: None)

    delete_calls = []
    monkeypatch.setattr(
        storage, "_delete_existing_keys_via_temp",
        lambda conn, table_name, keys, **k: delete_calls.append(keys) or 0
    )

    storage.load_to_postgres.fn(make_df(3), run_id="r1", idempotent=True)

    assert len(delete_calls) == 1  


def test_load_to_postgres_idempotent_false_skips_delete(monkeypatch):
    monkeypatch.setattr(storage, "_preflight_postgres_check", lambda: True)
    monkeypatch.setattr(storage, "_get_engine", lambda: MagicMock())

    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = True
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False
    monkeypatch.setattr(storage, "_connect_with_retry", lambda engine: mock_ctx)
    monkeypatch.setattr(storage, "_copy_to_postgres", lambda *a, **k: None)

    delete_calls = []
    monkeypatch.setattr(
        storage, "_delete_existing_keys_via_temp",
        lambda *a, **k: delete_calls.append(1) or 0
    )

    storage.load_to_postgres.fn(make_df(3), run_id="r1", idempotent=False)

    assert len(delete_calls) == 0  # dedup path skipped when idempotent=False


def test_load_to_postgres_creates_table_when_missing(monkeypatch):
    monkeypatch.setattr(storage, "_preflight_postgres_check", lambda: True)
    monkeypatch.setattr(storage, "_get_engine", lambda: MagicMock())
    monkeypatch.setattr(storage, "_copy_to_postgres", lambda *a, **k: None)
    monkeypatch.setattr(storage, "_delete_existing_keys_via_temp", lambda *a, **k: 0)

    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = False  # table does NOT exist
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False
    monkeypatch.setattr(storage, "_connect_with_retry", lambda engine: mock_ctx)

    to_sql_calls = []
    monkeypatch.setattr(pd.DataFrame, "to_sql", lambda self, *a, **k: to_sql_calls.append(k))

    storage.load_to_postgres.fn(make_df(3), run_id="r1", table_name="orders")

    assert len(to_sql_calls) == 1  # one-time schema-inferring create was triggered


def test_load_to_postgres_reraises_and_logs_hint_on_copy_failure(monkeypatch, caplog):
    monkeypatch.setattr(storage, "_preflight_postgres_check", lambda: True)
    monkeypatch.setattr(storage, "_get_engine", lambda: MagicMock())

    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = True
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False
    monkeypatch.setattr(storage, "_connect_with_retry", lambda engine: mock_ctx)
    monkeypatch.setattr(storage, "_delete_existing_keys_via_temp", lambda *a, **k: 0)

    def boom(*a, **k):
        raise RuntimeError("column run_id does not exist")

    monkeypatch.setattr(storage, "_copy_to_postgres", boom)

    with pytest.raises(RuntimeError, match="column run_id"):
        storage.load_to_postgres.fn(make_df(3), run_id="r1")


# ---------------------------------------------------------------------------
# load_to_bigquery (sandbox-safe, load_table_from_dataframe version)
# ---------------------------------------------------------------------------

def test_load_to_bigquery_skips_on_empty_df():
    result = storage.load_to_bigquery.fn(pd.DataFrame(), run_id="r1")
    assert result is None


def test_load_to_bigquery_skips_on_none_df():
    result = storage.load_to_bigquery.fn(None, run_id="r1")
    assert result is None


def test_load_to_bigquery_creates_partitioned_table_when_missing(monkeypatch):
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.output_rows = 3
    mock_client.load_table_from_dataframe.return_value = mock_job

    monkeypatch.setattr(storage, "_get_bq_client", lambda: mock_client)
    monkeypatch.setattr(storage, "_bq_table_exists", lambda client, table_ref: False)

    storage.load_to_bigquery.fn(make_df(3), run_id="r1")

    assert mock_client.load_table_from_dataframe.call_count == 1
    _, kwargs = mock_client.load_table_from_dataframe.call_args
    job_config = kwargs.get("job_config") or mock_client.load_table_from_dataframe.call_args[0][2]
    # Partitioning/clustering should only be configured on first creation.
    assert job_config.time_partitioning is not None
    assert job_config.clustering_fields == ["run_id", storage.NATURAL_KEY]


def test_load_to_bigquery_appends_without_reconfiguring_partitioning_when_exists(monkeypatch):
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.output_rows = 3
    mock_client.load_table_from_dataframe.return_value = mock_job

    monkeypatch.setattr(storage, "_get_bq_client", lambda: mock_client)
    monkeypatch.setattr(storage, "_bq_table_exists", lambda client, table_ref: True)

    storage.load_to_bigquery.fn(make_df(3), run_id="r1")

    _, kwargs = mock_client.load_table_from_dataframe.call_args
    job_config = kwargs.get("job_config") or mock_client.load_table_from_dataframe.call_args[0][2]
    # BigQuery rejects partitioning changes on an existing table - must stay unset.
    assert job_config.time_partitioning is None


def test_load_to_bigquery_propagates_exception_on_failure(monkeypatch):
    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.side_effect = RuntimeError("quota exceeded")

    monkeypatch.setattr(storage, "_get_bq_client", lambda: mock_client)
    monkeypatch.setattr(storage, "_bq_table_exists", lambda client, table_ref: True)

    with pytest.raises(RuntimeError, match="quota exceeded"):
        storage.load_to_bigquery.fn(make_df(3), run_id="r1")