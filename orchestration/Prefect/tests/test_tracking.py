
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

import tasks.tracking as tracking


# ---------------------------------------------------------------------------
# track_pipeline_run
# ---------------------------------------------------------------------------

def test_track_pipeline_run_success_creates_inserts_and_commits(monkeypatch):
    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_ctx
    monkeypatch.setattr(tracking, "_get_engine", lambda: mock_engine)

    tracking.track_pipeline_run("run-123", "orders", 500, success=True)


    assert mock_conn.execute.call_count == 2
    insert_call = mock_conn.execute.call_args_list[1]
    params = insert_call[0][1]
    assert params["run_id"] == "run-123"
    assert params["table_name"] == "orders"
    assert params["row_count"] == 500
    assert params["success"] is True
    mock_conn.commit.assert_called_once()


def test_track_pipeline_run_records_failure_correctly(monkeypatch):
    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_ctx
    monkeypatch.setattr(tracking, "_get_engine", lambda: mock_engine)

    tracking.track_pipeline_run("run-456", "orders", 0, success=False)

    insert_call = mock_conn.execute.call_args_list[1]
    params = insert_call[0][1]
    assert params["success"] is False
    assert params["row_count"] == 0


def test_track_pipeline_run_swallows_connection_failure(monkeypatch, caplog):
    def boom():
        raise ConnectionError("could not connect to server")

    monkeypatch.setattr(tracking, "_get_engine", boom)


    tracking.track_pipeline_run("run-789", "orders", 10, success=True)

    assert "Tracking failed" in caplog.text


def test_track_pipeline_run_swallows_insert_failure(monkeypatch, caplog):
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("duplicate key value")
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = False

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_ctx
    monkeypatch.setattr(tracking, "_get_engine", lambda: mock_engine)

    tracking.track_pipeline_run("run-dup", "orders", 10, success=True)

    assert "Tracking failed" in caplog.text


# ---------------------------------------------------------------------------
# time_tracking
# ---------------------------------------------------------------------------

def test_time_tracking_logs_start_and_elapsed(caplog):
    with caplog.at_level("INFO"):
        with tracking.time_tracking("Unit Test Block"):
            pass

    assert "Tracking timeline for: Unit Test Block" in caplog.text
    assert "Unit Test Block took" in caplog.text


def test_time_tracking_measures_a_real_positive_duration(caplog):
    with caplog.at_level("INFO"):
        with tracking.time_tracking("Sleep Block"):
            time.sleep(0.05)


    match_lines = [line for line in caplog.text.splitlines() if "Sleep Block took" in line]
    assert len(match_lines) == 1
    seconds = float(match_lines[0].split("took")[1].strip().rstrip("s"))
    assert 0.03 < seconds < 2.0


def test_time_tracking_still_logs_elapsed_when_block_raises(caplog):
    with caplog.at_level("INFO"):
        with pytest.raises(ValueError):
            with tracking.time_tracking("Failing Block"):
                raise ValueError("something broke inside the timed block")

    assert "Failing Block took" in caplog.text