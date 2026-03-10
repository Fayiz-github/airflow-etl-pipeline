"""
tests/test_etl.py — Priority 1: Proper pytest test suite

Coverage:
  extract.py  — 4 tests (happy path + 3 edge cases)
  transform.py — 3 tests (happy path + 2 edge cases)
  load.py     — 3 tests (happy path + 2 edge cases)
  Total: 10 tests (minimum required: 9)

All external dependencies (filesystem I/O, psycopg2, DB) are mocked.
Run with:  pytest dags/tests/test_etl.py -v
"""

import os
import sys
import pytest
import pandas as pd
from io import StringIO
from unittest.mock import patch, MagicMock, mock_open

# ------------------------------------------------------------------
# Make sure the scripts package is importable when running from root
# ------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.extract   import extract
from scripts.transform import transform
from scripts.load      import load


# ===================================================================
# FIXTURES — reusable sample data
# ===================================================================

VALID_CSV = "Time,Amount,Class\n10.0,200.0,0\n20.0,1500.0,1\n30.0,50.0,0\n"
EMPTY_CSV = "Time,Amount,Class\n"


@pytest.fixture
def valid_raw_csv(tmp_path):
    """Creates a valid source CSV and returns its path."""
    p = tmp_path / "task_1.csv"
    p.write_text(VALID_CSV)
    return str(p)


@pytest.fixture
def valid_processed_csv(tmp_path):
    """Creates a valid processed CSV (output of transform) for load tests."""
    p = tmp_path / "processed_2026-04-20.csv"
    p.write_text(
        "Time,Amount,Class,is_fraud,is_high_value,validation_flag\n"
        "10.0,200.0,0,False,False,valid\n"
        "20.0,1500.0,1,True,True,valid\n"
        "30.0,50.0,0,False,False,valid\n"
    )
    return str(p)


# ===================================================================
# EXTRACT TESTS
# ===================================================================

class TestExtract:

    # ----- Happy path -----------------------------------------------
    def test_extract_success(self, tmp_path, valid_raw_csv):
        """Should return status=success and correct row count for a valid date/file."""
        source_dir = tmp_path / "data" / "source"
        source_dir.mkdir(parents=True)
        raw_dir    = tmp_path / "data" / "raw" / "2026" / "04"
        raw_dir.mkdir(parents=True)

        (source_dir / "task_1.csv").write_text(VALID_CSV)

        with patch("scripts.extract.BASE_PATH", str(tmp_path)):
            result = extract("2026-04-01")

        assert result["status"]  == "success"
        assert result["rows"]    == 3
        assert "file_path" in result
        assert os.path.exists(result["file_path"])

    # ----- Edge case 1: invalid run_date format ---------------------
    def test_extract_invalid_date_format(self):
        """Should return status=failed for a bad date string."""
        result = extract("not-a-date")
        assert result["status"] == "failed"
        assert "Invalid run_date format" in result["error"]

    # ----- Edge case 2: source file missing -------------------------
    def test_extract_missing_source_file(self, tmp_path):
        """Should return status=failed when the mapped file doesn't exist."""
        source_dir = tmp_path / "data" / "source"
        source_dir.mkdir(parents=True)

        with patch("scripts.extract.BASE_PATH", str(tmp_path)):
            result = extract("2026-04-01")

        assert result["status"] == "failed"
        assert "not found" in result["error"].lower()

    # ----- Edge case 3: empty source file ---------------------------
    def test_extract_empty_source_file(self, tmp_path):
        """Should return status=failed when the CSV has no data rows."""
        source_dir = tmp_path / "data" / "source"
        source_dir.mkdir(parents=True)
        (source_dir / "task_1.csv").write_text(EMPTY_CSV)

        with patch("scripts.extract.BASE_PATH", str(tmp_path)):
            result = extract("2026-04-01")

        assert result["status"] == "failed"
        assert "0 rows" in result["error"]


# ===================================================================
# TRANSFORM TESTS
# ===================================================================

class TestTransform:

    # ----- Happy path -----------------------------------------------
    def test_transform_success(self, tmp_path, valid_raw_csv):
        """Should clean data, write processed CSV, and return correct metadata."""
        processed_dir = tmp_path / "data" / "processed"
        rejected_dir  = tmp_path / "data" / "rejected"
        processed_dir.mkdir(parents=True)
        rejected_dir.mkdir(parents=True)

        with patch("scripts.transform.BASE_PATH", str(tmp_path)):
            result = transform(valid_raw_csv, "2026-04-20")

        assert result["status"]  == "success"
        assert result["rows"]    == 3
        assert result["rows_rejected"] == 0
        assert os.path.exists(result["file_path"])
        assert os.path.exists(result["quality_report_path"])

        df = pd.read_csv(result["file_path"])
        assert "is_fraud"       in df.columns
        assert "is_high_value"  in df.columns
        assert "validation_flag" in df.columns

    # ----- Edge case 1: file with bad types -------------------------
    def test_transform_rejects_bad_types(self, tmp_path):
        """Rows with non-numeric Amount should be rejected, not crash the pipeline."""
        bad_csv = "Time,Amount,Class\n10.0,abc,0\n20.0,300.0,1\n"
        raw_path = tmp_path / "raw_bad.csv"
        raw_path.write_text(bad_csv)

        processed_dir = tmp_path / "data" / "processed"
        rejected_dir  = tmp_path / "data" / "rejected"
        processed_dir.mkdir(parents=True)
        rejected_dir.mkdir(parents=True)

        with patch("scripts.transform.BASE_PATH", str(tmp_path)):
            result = transform(str(raw_path), "2026-04-20")

        assert result["status"]        == "success"
        assert result["rows"]          == 1
        assert result["rows_rejected"] == 1

        rejected_path = tmp_path / "data" / "rejected" / "rejected_2026-04-20.csv"
        assert rejected_path.exists()

    # ----- Edge case 2: all-duplicate data --------------------------
    def test_transform_deduplicates(self, tmp_path):
        """All-duplicate rows should result in 1 clean row after deduplication."""
        dup_csv = "Time,Amount,Class\n10.0,200.0,0\n10.0,200.0,0\n10.0,200.0,0\n"
        raw_path = tmp_path / "raw_dup.csv"
        raw_path.write_text(dup_csv)

        processed_dir = tmp_path / "data" / "processed"
        rejected_dir  = tmp_path / "data" / "rejected"
        processed_dir.mkdir(parents=True)
        rejected_dir.mkdir(parents=True)

        with patch("scripts.transform.BASE_PATH", str(tmp_path)):
            result = transform(str(raw_path), "2026-04-20")

        assert result["status"] == "success"
        assert result["rows"]   == 1


# ===================================================================
# LOAD TESTS
# ===================================================================

class TestLoad:

    def _mock_conn(self):
        """Build a mock psycopg2 connection with cursor context managers."""
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (0,)

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_conn.__enter__ = lambda s: s
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        return mock_conn, mock_cursor

    # ----- Happy path -----------------------------------------------
    def test_load_success(self, valid_processed_csv):
        """Should return status=success and rows_loaded=3 for valid processed CSV."""
        mock_conn, _ = self._mock_conn()

        with patch("scripts.load._get_conn", return_value=mock_conn), \
             patch("scripts.load.execute_values") as mock_ev:

            result = load(valid_processed_csv, "2026-04-20")

        assert result["status"]      == "success"
        assert result["rows_loaded"] == 3
        assert mock_ev.called, "execute_values() must be called (not iterrows)"

    # ----- Edge case 1: DB connection failure -----------------------
    def test_load_db_connection_failure(self, valid_processed_csv):
        """Should return status=failed gracefully when DB is unavailable."""
        with patch("scripts.load._get_conn", side_effect=Exception("connection refused")):
            result = load(valid_processed_csv, "2026-04-20")

        assert result["status"] == "failed"
        assert "connection refused" in result["error"]

    # ----- Edge case 2: missing required columns --------------------
    def test_load_missing_columns(self, tmp_path):
        """Should return status=failed when processed CSV is missing columns."""
        bad_path = tmp_path / "bad.csv"
        bad_path.write_text("OnlyOneColumn\n1\n2\n")

        mock_conn, _ = self._mock_conn()

        with patch("scripts.load._get_conn", return_value=mock_conn):
            result = load(str(bad_path), "2026-04-20")

        assert result["status"] == "failed"
        assert "Missing" in result["error"] or "missing" in result["error"].lower()
