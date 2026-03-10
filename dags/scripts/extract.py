import glob
import logging
import os
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

BASE_PATH = "/opt/airflow"
if not os.path.exists(BASE_PATH):
    BASE_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )

# Required env vars — checked at module import so the pipeline fails fast
REQUIRED_ENV_VARS = ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]


def _check_env_vars():
    """Warn if expected env vars are missing (non-fatal inside Docker)."""
    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        logger.warning(f"[Extract] Missing env vars: {missing}. OK if running inside Docker.")


def extract(run_date, run_id=None):
    """
    Extract function.

    Steps:
    1. Validate run_date format
    2. Check required env vars
    3. Select correct source file (mapping or fallback, rotates by run_id)
    4. Validate the file exists and is non-empty
    5. Save raw copy to data/raw/{date-partition}/ with run_id in filename
    6. Return metadata dict for XCom

    Parameters
    ----------
    run_date : str
        Airflow execution date string (e.g. '2026-04-20') — used for DB records
    run_id : str, optional
        Airflow ts_nodash (e.g. '20260420T121500') — used for unique file names.
        Falls back to run_date if not provided.

    Returns
    -------
    dict
        status, file_path, rows, and optional warning/error
    """
    _check_env_vars()

    file_key = run_id if run_id else run_date   # unique key used in file names
    logger.info(f"[Extract] Starting extraction — run_date={run_date}, run_id={file_key}")

    # ----------------------------------------------------------------
    # 1. Validate run_date format
    # ----------------------------------------------------------------
    try:
        run_date_obj = date.fromisoformat(run_date)
    except ValueError:
        logger.error(f"[Extract] Invalid run_date format: {run_date}")
        return {
            "status": "failed",
            "error": f"Invalid run_date format: {run_date}. Expected YYYY-MM-DD.",
        }

    # ----------------------------------------------------------------
    # 2. Map run_date → source file
    # ----------------------------------------------------------------
    mapping = {
        "2026-04-01": "task_1.csv",
        "2026-04-02": "task_2.csv",
        "2026-04-03": "task_3.csv",
        "2026-04-04": "task_4.csv",
        "2026-04-05": "task_5.csv",
        "2026-04-06": "task_6.csv",
    }

    file_name = mapping.get(run_date)
    fallback_warning = None

    if not file_name:
        source_files = sorted(
            glob.glob(f"{BASE_PATH}/data/source/task_*.csv")
        )
        if not source_files:
            msg = f"No source files found in {BASE_PATH}/data/source/"
            logger.error(f"[Extract] {msg}")
            return {"status": "failed", "error": msg}

        # Rotate source files using run_id (minute digits) so each
        # 10-minute run picks a different file instead of reusing the same one
        if run_id and len(run_id) >= 13:
            # ts_nodash format: 20260420T121500 — use HHMM as rotation index
            hhmm = int(run_id[9:13])          # e.g. 1215
            index = (hhmm // 10) % len(source_files)
        else:
            index = (run_date_obj.day - 1) % len(source_files)

        file_name = os.path.basename(source_files[index])
        fallback_warning = (
            f"No exact mapping for {run_date}. "
            f"Using fallback file (index={index}): {file_name}"
        )
        logger.warning(f"[Extract] {fallback_warning}")

    source_path = f"{BASE_PATH}/data/source/{file_name}"

    # ----------------------------------------------------------------
    # 3. Validate source file exists
    # ----------------------------------------------------------------
    if not os.path.exists(source_path):
        msg = f"Source file not found: {source_path}"
        logger.error(f"[Extract] {msg}")
        return {"status": "failed", "error": msg}

    if os.path.getsize(source_path) == 0:
        msg = f"Source file is empty: {source_path}"
        logger.error(f"[Extract] {msg}")
        return {"status": "failed", "error": msg}

    # ----------------------------------------------------------------
    # 4. Read and validate structure
    # ----------------------------------------------------------------
    df = pd.read_csv(source_path)

    if df.empty:
        msg = f"Source file loaded 0 rows: {source_path}"
        logger.error(f"[Extract] {msg}")
        return {"status": "failed", "error": msg}

    required_columns = {"Time", "Amount", "Class"}
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        msg = f"Source file missing required columns: {missing_cols}"
        logger.error(f"[Extract] {msg}")
        return {"status": "failed", "error": msg}

    logger.info(
        f"[Extract] Loaded {len(df)} rows from {source_path}. "
        f"Columns: {list(df.columns)}"
    )

    # ----------------------------------------------------------------
    # 5. Save raw copy — date-partitioned subdirectory
    # ----------------------------------------------------------------
    year  = run_date_obj.strftime("%Y")
    month = run_date_obj.strftime("%m")
    raw_dir = f"{BASE_PATH}/data/raw/{year}/{month}"
    os.makedirs(raw_dir, exist_ok=True)

    # Use run_id in filename so each 10-min run saves its own raw file
    raw_path = f"{raw_dir}/raw_{file_key}.csv"
    df.to_csv(raw_path, index=False)
    logger.info(f"[Extract] Raw data saved → {raw_path}")

    # ----------------------------------------------------------------
    # 6. Build result
    # ----------------------------------------------------------------
    result = {
        "status":    "success",
        "file_path": raw_path,
        "rows":      len(df),
        "source_file": file_name,
    }
    if fallback_warning:
        result["warning"] = fallback_warning

    return result
