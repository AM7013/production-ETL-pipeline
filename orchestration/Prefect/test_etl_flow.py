import sys
from pathlib import Path
from unittest.mock import MagicMock
 
import pandas as pd
import pytest
 
# Same path-wiring etl_flow.py itself does - added explicitly here rather
# than relying on pytest's implicit rootdir insertion, so this test file
# works the same way regardless of which directory pytest is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
 
import etl_flow
 
 
def make_df(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "OrderID": [f"O{i:04d}" for i in range(n)],
        "Status": ["Shipped"] * n,
    })
 
 
@pytest.fixture(autouse=True)
def mock_everything(monkeypatch):
    """
    Mocks every external dependency the flow touches, so these tests
    exercise only the flow's own branching logic - never Spark, never a
    real DB, never a real dbt subprocess call.
    """
    monkeypatch.setattr(etl_flow, "log_start", MagicMock())
    monkeypatch.setattr(etl_flow, "extract_data", MagicMock(return_value=make_df()))
    monkeypatch.setattr(etl_flow, "profile_columns", MagicMock())
    monkeypatch.setattr(etl_flow, "load_to_bigquery", MagicMock())
    monkeypatch.setattr(etl_flow, "run_dbt_models", MagicMock())
    monkeypatch.setattr(etl_flow, "test_dbt_models", MagicMock())
    monkeypatch.setattr(etl_flow, "stop_spark", MagicMock())
 
 
def test_dry_run_skips_all_real_work(monkeypatch):
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=True)
 
    etl_flow.extract_data.assert_not_called()
    etl_flow.load_to_bigquery.assert_not_called()
    etl_flow.run_dbt_models.assert_not_called()
    # dry_run returns before the try/finally block even starts, so
    # stop_spark() correctly never runs either - nothing was ever started.
    etl_flow.stop_spark.assert_not_called()
 
 
def test_quality_pass_triggers_load_and_dbt(monkeypatch):
    passing_report = {"passed": True, "quality_score": 95.0, "clean_rows": 3, "total_rows": 3}
    monkeypatch.setattr(
        etl_flow, "run_quality_checks",
        MagicMock(return_value=(make_df(), passing_report))
    )
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.load_to_bigquery.assert_called_once()
    etl_flow.run_dbt_models.assert_called_once()
    etl_flow.test_dbt_models.assert_called_once()
 
 
def test_quality_failure_skips_load_and_dbt(monkeypatch):
    failing_report = {"passed": False, "quality_score": 40.0, "clean_rows": 1, "total_rows": 3}
    monkeypatch.setattr(
        etl_flow, "run_quality_checks",
        MagicMock(return_value=(make_df(), failing_report))
    )
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.load_to_bigquery.assert_not_called()
    etl_flow.run_dbt_models.assert_not_called()
    etl_flow.test_dbt_models.assert_not_called()
 
 
def test_load_to_bigquery_receives_a_run_id(monkeypatch):
    passing_report = {"passed": True, "quality_score": 95.0, "clean_rows": 3, "total_rows": 3}
    monkeypatch.setattr(
        etl_flow, "run_quality_checks",
        MagicMock(return_value=(make_df(), passing_report))
    )
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    _, kwargs = etl_flow.load_to_bigquery.call_args
    assert "run_id" in kwargs
    assert isinstance(kwargs["run_id"], str)
    assert len(kwargs["run_id"]) > 0
 
 
def test_stop_spark_called_even_when_extract_fails(monkeypatch):
    """
    The finally block must release the SparkSession on failure, not just
    on success - this was a real gap in earlier versions of this file.
    """
    monkeypatch.setattr(
        etl_flow, "extract_data",
        MagicMock(side_effect=RuntimeError("simulated extraction failure"))
    )
 
    with pytest.raises(RuntimeError, match="simulated extraction failure"):
        etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.stop_spark.assert_called_once()
 
 
def test_stop_spark_called_on_success_too(monkeypatch):
    passing_report = {"passed": True, "quality_score": 95.0, "clean_rows": 3, "total_rows": 3}
    monkeypatch.setattr(
        etl_flow, "run_quality_checks",
        MagicMock(return_value=(make_df(), passing_report))
    )
 
    etl_flow.etl_pipeline.fn(target_date="2027-01-01", dry_run=False)
 
    etl_flow.stop_spark.assert_called_once()