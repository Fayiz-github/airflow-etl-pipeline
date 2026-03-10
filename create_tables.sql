-- ============================================================
-- ETL Pipeline Database Schema
-- Priority 5: Fixed schema per guide requirements
--   - created_at on all tables
--   - 3-table pattern: raw_data → staging → fraud_transactions
--   - Indexes on query columns
--   - NOT NULL, UNIQUE, CHECK constraints
-- ============================================================

-- ----------------------------------------------------------
-- Table 1: raw_data
-- Stores exact copy of what came from source (zero transformation)
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_data (
    id              SERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL,
    source_file     TEXT        NOT NULL,
    raw_time        FLOAT,
    raw_amount      TEXT,           -- stored as text (no transformation yet)
    raw_class       TEXT,           -- stored as text (no transformation yet)
    row_hash        TEXT,           -- MD5 hash for deduplication
    ingestion_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
    created_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookup by run date
CREATE INDEX IF NOT EXISTS idx_raw_data_run_date      ON raw_data (run_date);
CREATE INDEX IF NOT EXISTS idx_raw_data_ingestion_date ON raw_data (ingestion_date);


-- ----------------------------------------------------------
-- Table 2: staging
-- Cleaned and type-cast data before loading to final table
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging (
    id              SERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL,
    time_val        FLOAT       NOT NULL,
    amount          FLOAT       NOT NULL CHECK (amount >= 0),
    class           INTEGER     NOT NULL CHECK (class IN (0, 1)),
    is_fraud        BOOLEAN     NOT NULL,
    is_high_value   BOOLEAN     NOT NULL DEFAULT FALSE,
    validation_flag TEXT        NOT NULL DEFAULT 'valid',  -- 'valid' | 'anomaly'
    ingestion_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
    created_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookup and joins
CREATE INDEX IF NOT EXISTS idx_staging_run_date ON staging (run_date);
CREATE INDEX IF NOT EXISTS idx_staging_class    ON staging (class);


-- ----------------------------------------------------------
-- Table 3: fraud_transactions (fact / analytics-ready table)
-- Final clean table used for queries and reporting
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_transactions (
    id              SERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL,
    time_val        FLOAT       NOT NULL,
    amount          FLOAT       NOT NULL CHECK (amount >= 0),
    class           INTEGER     NOT NULL CHECK (class IN (0, 1)),
    is_fraud        BOOLEAN     NOT NULL,
    is_high_value   BOOLEAN     NOT NULL DEFAULT FALSE,
    validation_flag VARCHAR(20) NOT NULL DEFAULT 'valid',  -- 'valid' | 'anomaly'
    ingestion_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
    created_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate loads for the same run_date + transaction fingerprint
    CONSTRAINT uq_fraud_tx UNIQUE (run_date, time_val, amount, class)
);

-- Indexes on columns used in WHERE / ORDER BY / analytical queries
CREATE INDEX IF NOT EXISTS idx_fraud_tx_run_date      ON fraud_transactions (run_date);
CREATE INDEX IF NOT EXISTS idx_fraud_tx_ingestion_date ON fraud_transactions (ingestion_date);
CREATE INDEX IF NOT EXISTS idx_fraud_tx_class          ON fraud_transactions (class);
CREATE INDEX IF NOT EXISTS idx_fraud_tx_amount         ON fraud_transactions (amount);
CREATE INDEX IF NOT EXISTS idx_fraud_tx_is_fraud       ON fraud_transactions (is_fraud);


-- ----------------------------------------------------------
-- Table 4: pipeline_runs
-- One record per DAG run — used for monitoring and alerts
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL,
    run_id          TEXT,                   -- ts_nodash (e.g. 20260420T121500) — unique per run
    status          VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed', 'partial')),
    rows_extracted  INTEGER     NOT NULL DEFAULT 0,
    rows_transformed INTEGER    NOT NULL DEFAULT 0,
    rows_loaded     INTEGER     NOT NULL DEFAULT 0,
    rows_rejected   INTEGER     NOT NULL DEFAULT 0,
    duration_secs   FLOAT,
    error_message   TEXT,
    ingestion_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
    created_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for monitoring queries
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_date ON pipeline_runs (run_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status   ON pipeline_runs (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_id   ON pipeline_runs (run_id);


-- ----------------------------------------------------------
-- Table 5: duplicate_log
-- Lightweight audit trail for rows dropped during deduplication.
-- Stores only the identifying key columns — NOT full row data.
-- One row per unique (time_val, amount, class) combo per run.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS duplicate_log (
    id             SERIAL PRIMARY KEY,
    run_id         TEXT        NOT NULL,          -- e.g. 20260423T063907
    run_date       DATE        NOT NULL,
    time_val       FLOAT       NOT NULL,          -- constraint key column 1
    amount         FLOAT       NOT NULL,          -- constraint key column 2
    class          INTEGER     NOT NULL,          -- constraint key column 3
    occurrences    INTEGER     NOT NULL,          -- total copies found in batch
    dropped_count  INTEGER     NOT NULL,          -- occurrences - 1 (kept 1)
    logged_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dup_log_run_id   ON duplicate_log (run_id);
CREATE INDEX IF NOT EXISTS idx_dup_log_run_date ON duplicate_log (run_date);
