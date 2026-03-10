# Fraud Detection ETL Pipeline — Apache Airflow

## Problem Statement

Credit card fraud costs billions annually. This pipeline ingests transaction data on a schedule, applies data quality checks and feature engineering, loads clean records into PostgreSQL, and answers key business questions automatically on every run.

**Domain:** Financial fraud detection  
**Dataset:** Credit card transaction records (Time, Amount, Class columns)  
**Schedule:** Every 10 minutes (`*/10 * * * *`)

> **Design note:** Each run uses two identifiers:
> - `run_date` (`YYYY-MM-DD`) — DB upsert key, ensures no duplicate DB rows per day
> - `run_id` (`ts_nodash`, e.g. `20260420T121500`) — used in **all file names** so every 10-min run saves its own raw, processed, rejected, quality report, and daily report files without overwriting previous runs

---

## Architecture

```
data/source/task_N.csv
        │
        ▼
  [ extract.py ]  ──→  data/raw/YYYY/MM/raw_{date}.csv
        │
        ▼
  [ transform.py ] ──→  data/processed/processed_{date}.csv
                    ──→  data/rejected/rejected_{date}.csv
                    ──→  data/processed/quality_report_{date}.txt
        │
        ▼
  [ load.py ]  ──→  PostgreSQL
                      ├── raw_data
                      ├── fraud_transactions  (fact table)
                      └── pipeline_runs
        │
        ▼
  [ report ] ──→  reports/report_{date}.txt
```

All steps are orchestrated by **Apache Airflow** running in Docker.

---

## Setup Instructions

### Prerequisites
- Docker Desktop installed and running
- Git

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd airflow-etl
```

### 2. Set environment variables
```bash
cp .env.example .env
# Edit .env and fill in your AIRFLOW_ALERT_EMAIL and AIRFLOW_SMTP_PASSWORD
```

### 3. Add source data
```bash
# Place creditcard.csv inside data/source/ then run:
python split_data.py
```

### 4. Start all services
```bash
docker compose up -d
```

### 5. Wait for Airflow to initialize (~60 seconds), then open:
```
http://localhost:8080
Username: admin
Password: admin
```

### 6. Run the database schema
```bash
docker exec -i airflow-etl-postgres-1 psql -U airflow -d airflow < create_tables.sql
```

---

## Triggering the DAG

### Via Airflow UI
1. Open http://localhost:8080
2. Find `etl_pipeline` DAG
3. Click **Trigger DAG ▶**

### Via Airflow CLI
```bash
docker exec airflow-etl-airflow-scheduler-1 \
  airflow dags trigger etl_pipeline --exec-date 2026-04-20
```

### Backfill last 7 days
```bash
docker exec airflow-etl-airflow-scheduler-1 \
  airflow dags backfill etl_pipeline \
  --start-date 2026-04-14 \
  --end-date 2026-04-20
```

---

## Running Tests

```bash
# Install test dependencies (outside Docker, for local dev)
pip install pytest pytest-mock pandas

# Run all tests
pytest dags/tests/test_etl.py -v

# Expected output: 10 tests, all PASSED
```

---

## Business Questions

**Q1: What are the top 5 highest-amount transactions today?**  
→ Answered in `reports/report_{date}.txt` under "Top 5 transactions by amount"

**Q2: What is the fraud rate in today's data?**  
→ Answered in `reports/report_{date}.txt` under "Fraud rate"  
Example: `0.17%` of transactions are fraudulent

**Q3: How many high-value transactions (> $1,000) were processed?**  
→ Answered in `reports/report_{date}.txt` under "High-value transactions"

---

## Known Limitations

- **Static file mapping:** The `extract.py` date→file mapping only covers 6 specific dates. Other dates use a day-of-month fallback. A real pipeline would connect to an API or S3 bucket.
- **No real email sending:** Failure/success callbacks log notifications but don't send actual emails unless SMTP is configured in `.env`.
- **Single-node setup:** Runs on `LocalExecutor`. For production, switch to `CeleryExecutor` with Redis for horizontal scaling.
- **No data encryption:** Sensitive fields (amounts, timestamps) are stored in plaintext. Production systems should encrypt PII at rest.

### What I would improve with more time
- Connect extract.py to a real API (e.g., Kaggle API, Open Banking API)
- Add dbt for SQL-based transformations
- Set up Grafana dashboards for `pipeline_runs` monitoring
- Add GitHub Actions CI/CD to run pytest on every push
- Implement column-level data lineage tracking
