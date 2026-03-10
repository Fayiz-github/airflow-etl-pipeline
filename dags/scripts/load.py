import logging
import os
import time
from datetime import date

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ModuleNotFoundError:
    psycopg2 = None
    execute_values = None

import pandas as pd

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "dbname":   os.getenv("POSTGRES_DB",       "airflow"),
    "user":     os.getenv("POSTGRES_USER",     "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
}


def _get_conn():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed. Run inside the Airflow Docker container.")
    return psycopg2.connect(**DB_CONFIG)


def _ensure_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            id              SERIAL PRIMARY KEY,
            run_date        DATE        NOT NULL,
            source_file     TEXT        NOT NULL,
            raw_time        FLOAT,
            raw_amount      TEXT,
            raw_class       TEXT,
            row_hash        TEXT,
            ingestion_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
            created_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fraud_transactions (
            id              SERIAL PRIMARY KEY,
            run_date        DATE        NOT NULL,
            time_val        FLOAT       NOT NULL,
            amount          FLOAT       NOT NULL CHECK (amount >= 0),
            class           INTEGER     NOT NULL CHECK (class IN (0, 1)),
            is_fraud        BOOLEAN     NOT NULL,
            is_high_value   BOOLEAN     NOT NULL DEFAULT FALSE,
            validation_flag TEXT        NOT NULL DEFAULT 'valid',
            ingestion_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
            created_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_fraud_tx UNIQUE (run_date, time_val, amount, class)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id               SERIAL PRIMARY KEY,
            run_date         DATE        NOT NULL,
            run_id           TEXT,
            status           VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed', 'partial')),
            rows_extracted   INTEGER     NOT NULL DEFAULT 0,
            rows_transformed INTEGER     NOT NULL DEFAULT 0,
            rows_loaded      INTEGER     NOT NULL DEFAULT 0,
            rows_rejected    INTEGER     NOT NULL DEFAULT 0,
            duration_secs    FLOAT,
            error_message    TEXT,
            ingestion_date   DATE        NOT NULL DEFAULT CURRENT_DATE,
            created_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_fraud_tx_run_date      ON fraud_transactions (run_date)",
        "CREATE INDEX IF NOT EXISTS idx_fraud_tx_class         ON fraud_transactions (class)",
        "CREATE INDEX IF NOT EXISTS idx_fraud_tx_amount        ON fraud_transactions (amount)",
        "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_date ON pipeline_runs (run_date)",
        "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_id   ON pipeline_runs (run_id)",
    ]:
        cur.execute(stmt)


def load(file_path, run_date, run_id=None, rows_extracted=0, rows_rejected=0):
    """
    Load function.

    Features:
    - execute_values() batch inserts (NOT row-by-row iterrows)
    - UPSERT logic with ON CONFLICT DO UPDATE — no duplicates
    - Row count verification (before/after delta logged)
    - pipeline_runs updated with duration, row counts, and error message
    - Full transaction: all-or-nothing COMMIT / ROLLBACK
    """
    start_time = time.time()
    logger.info(f"[Load] Starting load — run_date={run_date}, run_id={run_id}, file={file_path}")

    try:
        conn = _get_conn()
    except Exception as conn_exc:
        logger.error(f"[Load] DB connection failed: {conn_exc}")
        return {"status": "failed", "error": str(conn_exc)}

    df = pd.read_csv(file_path)
    rows_in_raw = len(df)

    required_cols = {"Time", "Amount", "Class"}
    missing = required_cols - set(df.columns)
    if missing:
        duration = time.time() - start_time
        _log_pipeline_run(conn, run_date, run_id=run_id, status="failed",
                          rows_extracted=rows_extracted, rows_transformed=0,
                          rows_loaded=0, rows_rejected=rows_rejected,
                          duration_secs=duration,
                          error_message=f"Missing required columns: {missing}")
        conn.close()
        return {"status": "failed", "error": f"Missing required columns: {missing}"}

    df_raw = df.copy()
    df = df.drop_duplicates(subset=["Time", "Amount", "Class"])
    rows_in = len(df)
    dupes_dropped = rows_in_raw - rows_in

    if dupes_dropped:
        logger.warning(f"[Load] Dropped {dupes_dropped} duplicate rows before insert.")
        counts = df_raw.groupby(["Time", "Amount", "Class"]).size().reset_index(name="occurrences")
        dup_keys = counts[counts["occurrences"] > 1].copy()
        dup_keys["dropped_count"] = dup_keys["occurrences"] - 1
        dup_rows = [
            (run_id, run_date, row["Time"], row["Amount"], int(row["Class"]),
             int(row["occurrences"]), int(row["dropped_count"]))
            for _, row in dup_keys.iterrows()
        ]
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS duplicate_log (
                        id             SERIAL PRIMARY KEY,
                        run_id         TEXT      NOT NULL,
                        run_date       DATE      NOT NULL,
                        time_val       FLOAT     NOT NULL,
                        amount         FLOAT     NOT NULL,
                        class          INTEGER   NOT NULL,
                        occurrences    INTEGER   NOT NULL,
                        dropped_count  INTEGER   NOT NULL,
                        logged_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                execute_values(cur,
                    "INSERT INTO duplicate_log (run_id, run_date, time_val, amount, class, occurrences, dropped_count) VALUES %s",
                    dup_rows)
            conn.commit()
            logger.info(f"[Load] Logged {len(dup_rows)} duplicate keys to duplicate_log.")
        except Exception as dup_exc:
            logger.warning(f"[Load] Could not write duplicate_log: {dup_exc}")
            conn.rollback()

    logger.info(f"[Load] Rows to insert after dedup: {rows_in}")

    try:
        with conn:
            with conn.cursor() as cur:
                _ensure_tables(cur)
                cur.execute("SELECT COUNT(*) FROM fraud_transactions WHERE run_date = %s", (run_date,))
                count_before = cur.fetchone()[0]

                today = date.today()
                is_high_value_col = "is_high_value" in df.columns
                flag_col = "validation_flag" in df.columns

                rows_to_insert = [
                    (
                        run_date,
                        float(row["Time"]),
                        float(row["Amount"]),
                        int(row["Class"]),
                        bool(int(row["Class"]) == 1),
                        bool(row["is_high_value"]) if is_high_value_col else float(row["Amount"]) > 1000,
                        str(row["validation_flag"]) if flag_col else "valid",
                        today,
                    )
                    for _, row in df.iterrows()
                ]

                upsert_sql = """
                    INSERT INTO fraud_transactions
                        (run_date, time_val, amount, class, is_fraud,
                         is_high_value, validation_flag, ingestion_date)
                    VALUES %s
                    ON CONFLICT (run_date, time_val, amount, class)
                    DO UPDATE SET
                        is_fraud        = EXCLUDED.is_fraud,
                        is_high_value   = EXCLUDED.is_high_value,
                        validation_flag = EXCLUDED.validation_flag,
                        ingestion_date  = EXCLUDED.ingestion_date
                """
                execute_values(cur, upsert_sql, rows_to_insert, page_size=1000)

                cur.execute("SELECT COUNT(*) FROM fraud_transactions WHERE run_date = %s", (run_date,))
                count_after = cur.fetchone()[0]
                delta = count_after - count_before
                logger.info(f"[Load] Row count before={count_before}, after={count_after}, delta={delta}")

        duration = time.time() - start_time
        logger.info(f"[Load] Completed in {duration:.2f}s — {rows_in} rows processed")

        _log_pipeline_run(conn, run_date, run_id=run_id, status="success",
                          rows_extracted=rows_extracted, rows_transformed=rows_in,
                          rows_loaded=rows_in, rows_rejected=rows_rejected,
                          duration_secs=duration, error_message=None)
        conn.close()
        return {"status": "success", "rows_loaded": rows_in, "duration_secs": round(duration, 2)}

    except Exception as exc:
        duration = time.time() - start_time
        logger.error(f"[Load] FAILED after {duration:.2f}s — {exc}", exc_info=True)
        _log_pipeline_run(conn, run_date, run_id=run_id, status="failed",
                          rows_extracted=rows_extracted, rows_transformed=rows_in,
                          rows_loaded=0, rows_rejected=rows_rejected,
                          duration_secs=duration, error_message=str(exc))
        conn.close()
        return {"status": "failed", "error": str(exc)}


def _log_pipeline_run(conn, run_date, status, rows_extracted, rows_transformed,
                      rows_loaded, rows_rejected, duration_secs, error_message, run_id=None):
    try:
        if conn.closed:
            conn = _get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        id               SERIAL PRIMARY KEY,
                        run_date         DATE        NOT NULL,
                        run_id           TEXT,
                        status           VARCHAR(20) NOT NULL,
                        rows_extracted   INTEGER     NOT NULL DEFAULT 0,
                        rows_transformed INTEGER     NOT NULL DEFAULT 0,
                        rows_loaded      INTEGER     NOT NULL DEFAULT 0,
                        rows_rejected    INTEGER     NOT NULL DEFAULT 0,
                        duration_secs    FLOAT,
                        error_message    TEXT,
                        ingestion_date   DATE        NOT NULL DEFAULT CURRENT_DATE,
                        created_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    INSERT INTO pipeline_runs
                        (run_date, run_id, status, rows_extracted, rows_transformed,
                         rows_loaded, rows_rejected, duration_secs, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (run_date, run_id, status, rows_extracted, rows_transformed,
                      rows_loaded, rows_rejected,
                      round(duration_secs, 2) if duration_secs else None, error_message))
        logger.info(f"[Load] pipeline_runs updated — run_id={run_id}, status={status}")
    except Exception as log_exc:
        logger.error(f"[Load] Failed to write pipeline_runs: {log_exc}")
