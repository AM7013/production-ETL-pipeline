import sys
from pathlib import Path
from unittest.mock import MagicMock
 
import pandas as pd
import pytest
 
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
 
import etl_flow
 
 
def make_df(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "OrderID": [f"O{i:04d}" for i in range(n)],
        "Status": ["Shipped"] * n,
    })
 
 
@pytest.fixture(autouse=True)
def mock_everything(monkeypatch):
    """Mocks every external dependency so tests exercise only the flow's own branching logic."""
    monkeypatch.setattr(etl_flow, "log_start", MagicMock())
    monkeypatch.setattr(etl_flow, "extract_data", MagicMock(return_value=make_df()))
    monkeypatch.setattr(etl_flow, "profile_columns", MagicMock())
    monkeypatch.setattr(etl_flow, "load_to_postgres", MagicMock())
    monkeypatch.setattr(etl_flow, "load_to_bigquery", MagicMock())
    monkeypatch.setattr(etl_flow, "run_dbt_models", MagicMock())
    monkeypatch.setattr(etl_flow, "test_dbt_models", MagicMock())
    monkeypatch.setattr(etl_flow, "track_pipeline_run", MagicMock())
    monkeypatch.setattr(etl_flow, "stop_spark", MagicMock())
    # time_tracking is a context manager; a plain MagicMock supports the
    # `with ...:` protocol automatically, so this is safe to swap in as-is.
    monkeypatch.setattr(etl_flow, "time_tracking", MagicMock())
 
 
def _passing_report():
    return {"passed": True, "quality_score": 95.0, "clean_rows": 3, "total_rows": 3}
 
 
def _failing_report():
    return {"passed": False, "quality_score": 40.0, "clean_rows": 1, "total_rows": 3}
 
 
def test_dry_run_skips_all_real_work():
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=True)
 
    etl_flow.extract_data.assert_not_called()
    etl_flow.load_to_postgres.assert_not_called()
    etl_flow.load_to_bigquery.assert_not_called()
    etl_flow.run_dbt_models.assert_not_called()
    etl_flow.track_pipeline_run.assert_not_called()
    etl_flow.stop_spark.assert_not_called()
 
 
def test_quality_pass_triggers_both_loads_dbt_and_success_tracking(monkeypatch):
    monkeypatch.setattr(etl_flow, "run_quality_checks", MagicMock(return_value=(make_df(), _passing_report())))
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.load_to_postgres.assert_called_once()
    etl_flow.load_to_bigquery.assert_called_once()
    etl_flow.run_dbt_models.assert_called_once()
    etl_flow.test_dbt_models.assert_called_once()
 
    etl_flow.track_pipeline_run.assert_called_once()
    args, kwargs = etl_flow.track_pipeline_run.call_args
    assert args[-1] is True or kwargs.get("success") is True
 
 
def test_quality_failure_skips_loads_and_dbt_but_still_tracks(monkeypatch):
    monkeypatch.setattr(etl_flow, "run_quality_checks", MagicMock(return_value=(make_df(), _failing_report())))
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.load_to_postgres.assert_not_called()
    etl_flow.load_to_bigquery.assert_not_called()
    etl_flow.run_dbt_models.assert_not_called()
    etl_flow.test_dbt_models.assert_not_called()
 
    # A failed run should still be recorded - failures matter for the
    # audit trail just as much as successes do.
    etl_flow.track_pipeline_run.assert_called_once()
    args, kwargs = etl_flow.track_pipeline_run.call_args
    assert args[-1] is False or kwargs.get("success") is False
 
 
def test_both_loads_receive_the_same_run_id(monkeypatch):
    monkeypatch.setattr(etl_flow, "run_quality_checks", MagicMock(return_value=(make_df(), _passing_report())))
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    pg_kwargs = etl_flow.load_to_postgres.call_args.kwargs
    bq_kwargs = etl_flow.load_to_bigquery.call_args.kwargs
 
    assert pg_kwargs["run_id"] == bq_kwargs["run_id"]
    assert isinstance(pg_kwargs["run_id"], str) and len(pg_kwargs["run_id"]) > 0
 
 
def test_stop_spark_called_even_when_extract_fails(monkeypatch):
    monkeypatch.setattr(etl_flow, "extract_data", MagicMock(side_effect=RuntimeError("simulated failure")))
 
    with pytest.raises(RuntimeError, match="simulated failure"):
        etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.stop_spark.assert_called_once()
 
 
def test_stop_spark_called_on_success_too(monkeypatch):
    monkeypatch.setattr(etl_flow, "run_quality_checks", MagicMock(return_value=(make_df(), _passing_report())))
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.stop_spark.assert_called_once()