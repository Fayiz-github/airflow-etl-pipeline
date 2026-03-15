import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

# Base path inside Docker container
BASE_PATH = "/opt/airflow"
if not os.path.exists(BASE_PATH):
    BASE_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )

# ------------------------------------------------------------------
# Business rules / expected ranges for anomaly detection
# ------------------------------------------------------------------
AMOUNT_MIN = 0.0
AMOUNT_MAX = 25000.0     # transactions above this are flagged as anomalies
AMOUNT_HIGH_VALUE = 1000.0


def transform(file_path, run_date, run_id=None):
    """
    Transform function.

    Steps:
    1. Read raw CSV
    2. Type validation and casting
    3. Deduplication
    4. Null removal
    5. Range / anomaly checks (flags rows, does NOT drop them)
    6. Write rejected rows  data/rejected/
    7. Write clean rows     data/processed/
    8. Write quality report data/processed/

    Parameters
    ----------
    file_path : str
        Path to the raw CSV produced by extract.py
    run_date : str
        Airflow execution date string (e.g. '2026-04-20')
    run_id : str, optional
        Airflow ts_nodash (e.g. '20260420T121500') — unique file names.
        Falls back to run_date if not provided.

    Returns
    -------
    dict
        status, file_path, rows, rows_rejected, quality_report_path
    """
    file_key = run_id if run_id else run_date
    logger.info(f"[Transform] Starting transform — run_date={run_date}, run_id={file_key}")

    # 1. Read raw data
    df = pd.read_csv(file_path)
    total_rows = len(df)
    logger.info(f"[Transform] Rows received from extract: {total_rows}")

    # 2. Type validation
    rejection_log = []

    def try_cast_float(val):
        try:
            return float(val), None
        except (ValueError, TypeError):
            return None, "invalid_float"

    def try_cast_int(val):
        try:
            return int(float(val)), None
        except (ValueError, TypeError):
            return None, "invalid_int"

    df["Amount_cast"], df["Amount_err"] = zip(*df["Amount"].map(lambda v: try_cast_float(v)))
    for idx in df[df["Amount_err"].notna()].index:
        rejection_log.append({"index": idx, "reason": "invalid_amount_type"})

    df["Time_cast"], df["Time_err"] = zip(*df["Time"].map(lambda v: try_cast_float(v)))
    for idx in df[df["Time_err"].notna()].index:
        rejection_log.append({"index": idx, "reason": "invalid_time_type"})

    df["Class_cast"], df["Class_err"] = zip(*df["Class"].map(lambda v: try_cast_int(v)))
    for idx in df[df["Class_err"].notna()].index:
        rejection_log.append({"index": idx, "reason": "invalid_class_type"})

    class_value_bad = df["Class_cast"].notna() & ~df["Class_cast"].isin([0, 1])
    for idx in df[class_value_bad].index:
        rejection_log.append({"index": idx, "reason": "invalid_class_value"})

    # 3. Separate bad and good rows
    bad_indices = set(r["index"] for r in rejection_log)
    df_bad  = df.loc[list(bad_indices)].copy()
    df_good = df.drop(index=list(bad_indices)).copy()

    # 4. Apply casts to good rows
    df_good["Amount"] = df_good["Amount_cast"].astype(float)
    df_good["Time"]   = df_good["Time_cast"].astype(float)
    df_good["Class"]  = df_good["Class_cast"].astype(int)
    df_good.drop(columns=["Amount_cast", "Amount_err",
                           "Time_cast",   "Time_err",
                           "Class_cast",  "Class_err"], inplace=True)

    # 5. Remove duplicates
    before_dedup = len(df_good)
    df_good = df_good.drop_duplicates()
    dupes_removed = before_dedup - len(df_good)
    if dupes_removed:
        logger.warning(f"[Transform] Removed {dupes_removed} duplicate rows")

    # 6. Remove nulls
    before_null = len(df_good)
    df_good = df_good.dropna(subset=["Time", "Amount", "Class"])
    nulls_removed = before_null - len(df_good)
    if nulls_removed:
        logger.warning(f"[Transform] Removed {nulls_removed} rows with null values")

    # 7. Null percentage per column
    null_pct = (df_good.isnull().sum() / len(df_good) * 100).round(2).to_dict() if len(df_good) > 0 else {}

    # 8. Derived columns
    df_good["is_fraud"]      = df_good["Class"] == 1
    df_good["is_high_value"] = df_good["Amount"] > AMOUNT_HIGH_VALUE

    # 9. Anomaly detection — flag but keep
    anomaly_mask = (df_good["Amount"] < AMOUNT_MIN) | (df_good["Amount"] > AMOUNT_MAX)
    df_good["validation_flag"] = "valid"
    df_good.loc[anomaly_mask, "validation_flag"] = "anomaly"
    anomaly_count = int(anomaly_mask.sum())
    if anomaly_count:
        logger.warning(f"[Transform] {anomaly_count} rows flagged as anomalies")

    # 10. Save rejected rows
    rejected_dir = f"{BASE_PATH}/data/rejected"
    os.makedirs(rejected_dir, exist_ok=True)
    rejected_path = f"{rejected_dir}/rejected_{file_key}.csv"

    if df_bad.empty and not nulls_removed and not dupes_removed:
        df_bad.to_csv(rejected_path, index=True)
    else:
        reason_map = {r["index"]: r["reason"] for r in rejection_log}
        df_bad["rejection_reason"] = df_bad.index.map(lambda i: reason_map.get(i, "unknown"))
        df_bad.to_csv(rejected_path, index=True)
    logger.info(f"[Transform] Rejected rows saved -> {rejected_path}")

    # 11. Save processed data
    processed_dir = f"{BASE_PATH}/data/processed"
    os.makedirs(processed_dir, exist_ok=True)
    processed_path = f"{processed_dir}/processed_{file_key}.csv"
    df_good.to_csv(processed_path, index=False)
    logger.info(f"[Transform] Processed rows saved -> {processed_path}")

    # 12. Write quality report
    rows_passed   = len(df_good)
    rows_rejected = total_rows - rows_passed

    reason_counts = {}
    for r in rejection_log:
        reason_counts[r["reason"]] = reason_counts.get(r["reason"], 0) + 1
    if dupes_removed:
        reason_counts["duplicate_rows"] = dupes_removed
    if nulls_removed:
        reason_counts["null_values"] = nulls_removed

    null_pct_lines = "\n".join(f"    {col}: {pct}%" for col, pct in null_pct.items()) or "    N/A"
    reason_lines   = "\n".join(f"    {r}: {c}" for r, c in reason_counts.items()) or "    None"

    quality_report = f"""=======================================================
  DATA QUALITY REPORT - {run_date}
=======================================================
Total rows received from extract  : {total_rows}
Rows that passed all checks       : {rows_passed}
Rows rejected (total)             : {rows_rejected}

Rejection reasons:
{reason_lines}

Null value % per column (after clean):
{null_pct_lines}

Anomalies detected (amount out of range [{AMOUNT_MIN}, {AMOUNT_MAX}]):
  Count: {anomaly_count}

High-value transactions (amount > {AMOUNT_HIGH_VALUE}):
  Count: {int(df_good["is_high_value"].sum()) if len(df_good) > 0 else 0}

Fraud transactions (class == 1):
  Count: {int(df_good["is_fraud"].sum()) if len(df_good) > 0 else 0}
=======================================================
"""
    report_path = f"{processed_dir}/quality_report_{file_key}.txt"
    with open(report_path, "w") as f:
        f.write(quality_report)
    logger.info(f"[Transform] Quality report saved -> {report_path}")

    return {
        "status":              "success",
        "file_path":           processed_path,
        "rows":                rows_passed,
        "rows_rejected":       rows_rejected,
        "anomaly_count":       anomaly_count,
        "quality_report_path": report_path,
    }
