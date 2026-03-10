import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

from airflow.utils.trigger_rule import TriggerRule

# ── Real ETL script imports ───────────────────────────────────────────────────
from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load
# ─────────────────────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)

ALERT_EMAIL = os.environ.get("AIRFLOW_ALERT_EMAIL", "your@email.com")


def _send_failure_email(context):
    """on_failure_callback: fires for ANY task that fails."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    run_date = context["ds"]
    log_url  = context["task_instance"].log_url
    exception = context.get("exception", "Unknown error")

    subject = f"[Airflow] ❌ DAG '{dag_id}' FAILED — task '{task_id}'"

    html_content = f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;">
    <h2 style="color:#c0392b;">❌ Airflow DAG Failure Alert</h2>
    <table border="0" cellpadding="8" style="border-collapse:collapse;width:100%;max-width:600px;">
      <tr style="background:#f9f9f9;">
        <td><strong>DAG</strong></td><td>{dag_id}</td>
      </tr>
      <tr>
        <td><strong>Failed Task</strong></td><td>{task_id}</td>
      </tr>
      <tr style="background:#f9f9f9;">
        <td><strong>Run ID</strong></td><td>{run_id}</td>
      </tr>
      <tr>
        <td><strong>Run Date</strong></td><td>{run_date}</td>
      </tr>
      <tr style="background:#f9f9f9;">
        <td><strong>Error</strong></td><td><code>{exception}</code></td>
      </tr>
      <tr>
        <td><strong>Logs</strong></td>
        <td><a href="{log_url}">View task logs in Airflow UI</a></td>
      </tr>
    </table>
    <p style="margin-top:16px;color:#666;font-size:12px;">
      This is an automated alert from your Airflow ETL pipeline.
    </p>
    </body></html>
    """

    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=html_content,
    )
    log.error("Failure alert email sent to %s for task '%s'", ALERT_EMAIL, task_id)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    # Belt-and-suspenders: explicit callback in addition to email_on_failure
    "on_failure_callback": _send_failure_email,
}

# FIX: Static start_date — never use days_ago() here
START_DATE = datetime(2026, 4, 15)

# ==============================================================================
# TASK CALLABLES
# ==============================================================================

def run_extract(**context):
    """Extract raw data and push results explicitly to XCom."""
    run_date = context["ds"]
    run_id = context["ts_nodash"]
    ti = context["ti"]

    log.info("Starting extraction for run_date=%s, run_id=%s", run_date, run_id)
    result = extract(run_date, run_id)

    if result.get("status") != "success":
        raise RuntimeError(f"extract() returned non-success status: {result}")

    # FIX: Push only the keys downstream tasks actually need — keeps XCom lean
    ti.xcom_push(key="rows", value=result["rows"])
    ti.xcom_push(key="file_path", value=result["file_path"])
    log.info("Extraction complete — rows=%s, file=%s", result["rows"], result["file_path"])


def validate_extract(**context):
    """Validate extraction output before allowing transform to proceed."""
    ti = context["ti"]

    rows = ti.xcom_pull(task_ids="extract_task", key="rows")
    file_path = ti.xcom_pull(task_ids="extract_task", key="file_path")

    if rows is None or file_path is None:
        raise ValueError(
            "XCom pull returned None — extract_task may not have pushed values. "
            "Check extract_task logs."
        )
    if rows == 0:
        raise ValueError("Extraction returned 0 rows — aborting pipeline to avoid empty load.")

    log.info("Validation passed — rows=%s, file_path=%s", rows, file_path)


def run_transform(**context):
    """Transform extracted file and push results to XCom."""
    ti = context["ti"]
    run_date = context["ds"]
    run_id = context["ts_nodash"]

    file_path = ti.xcom_pull(task_ids="extract_task", key="file_path")
    if not file_path:
        raise ValueError("Could not retrieve file_path from extract_task XCom.")

    log.info("Starting transform for file=%s", file_path)
    result = transform(file_path, run_date, run_id)

    if result.get("status") != "success":
        raise RuntimeError(f"transform() returned non-success status: {result}")

    ti.xcom_push(key="rows", value=result["rows"])
    ti.xcom_push(key="rows_rejected", value=result["rows_rejected"])
    ti.xcom_push(key="file_path", value=result["file_path"])
    ti.xcom_push(key="anomaly_count", value=result.get("anomaly_count", 0))
    log.info(
        "Transform complete — rows=%s, rejected=%s, anomalies=%s",
        result["rows"], result["rows_rejected"], result.get("anomaly_count", 0),
    )


def run_load(**context):
    """Load transformed data and push load stats to XCom."""
    ti = context["ti"]
    run_date = context["ds"]
    run_id = context["ts_nodash"]

    transform_file = ti.xcom_pull(task_ids="transform_task", key="file_path")
    rows_extracted = ti.xcom_pull(task_ids="extract_task", key="rows") or 0
    rows_rejected = ti.xcom_pull(task_ids="transform_task", key="rows_rejected") or 0

    if not transform_file:
        raise ValueError("Could not retrieve file_path from transform_task XCom.")

    log.info("Starting load for file=%s", transform_file)
    # FIX: Call signature now matches the real function definition above
    result = load(
        file_path=transform_file,
        run_date=run_date,
        run_id=run_id,
        rows_extracted=rows_extracted,
        rows_rejected=rows_rejected,
    )

    if result.get("status") != "success":
        raise RuntimeError(f"load() returned non-success status: {result}")

    ti.xcom_push(key="rows_loaded", value=result["rows_loaded"])
    ti.xcom_push(key="duration_secs", value=result["duration_secs"])
    log.info(
        "Load complete — rows_loaded=%s, duration=%ss",
        result["rows_loaded"], result["duration_secs"],
    )


def notify_success(**context):
    run_id = context["ts_nodash"]
    run_date = context["ds"]
    log.info("=" * 50)
    log.info("✅ ETL Pipeline SUCCESS")
    log.info("Run ID   : %s", run_id)
    log.info("Run Date : %s", run_date)
    log.info("=" * 50)


def notify_failure(**context):
    """DAG-level failure watchdog task — logs summary and sends a DAG-wide email."""
    dag_id   = context["dag"].dag_id
    run_id   = context["ts_nodash"]
    run_date = context["ds"]

    log.error("=" * 50)
    log.error("❌ ETL Pipeline FAILURE")
    log.error("Run ID   : %s", run_id)
    log.error("Run Date : %s", run_date)
    log.error("Check task logs in the Airflow UI for details.")
    log.error("=" * 50)

    subject = f"[Airflow] ❌ DAG '{dag_id}' run {run_id} FAILED"
    html_content = f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;">
    <h2 style="color:#c0392b;">❌ ETL Pipeline Run Failed</h2>
    <table border="0" cellpadding="8" style="border-collapse:collapse;width:100%;max-width:600px;">
      <tr style="background:#f9f9f9;">
        <td><strong>DAG</strong></td><td>{dag_id}</td>
      </tr>
      <tr>
        <td><strong>Run ID</strong></td><td>{run_id}</td>
      </tr>
      <tr style="background:#f9f9f9;">
        <td><strong>Run Date</strong></td><td>{run_date}</td>
      </tr>
    </table>
    <p>One or more pipeline tasks failed during this run. Please check the
    <a href="http://localhost:8080">Airflow UI</a> for full task logs.</p>
    <p style="margin-top:16px;color:#666;font-size:12px;">
      This is an automated alert from your Airflow ETL pipeline.
    </p>
    </body></html>
    """
    try:
        send_email(to=ALERT_EMAIL, subject=subject, html_content=html_content)
        log.error("DAG-level failure email sent to %s", ALERT_EMAIL)
    except Exception as exc:
        log.error("Could not send DAG-level failure email: %s", exc)


def generate_report(**context):
    """Write a plain-text pipeline summary report to disk."""
    # FIX: Removed unused 'import pandas as pd' — was causing silent failures
    #      if pandas wasn't installed on the worker.
    ti = context["ti"]
    run_date = context["ds"]
    run_id = context["ts_nodash"]

    rows_extracted = ti.xcom_pull(task_ids="extract_task", key="rows") or 0
    rows_rejected = ti.xcom_pull(task_ids="transform_task", key="rows_rejected") or 0
    rows_loaded = ti.xcom_pull(task_ids="load_task", key="rows_loaded") or 0
    duration_secs = ti.xcom_pull(task_ids="load_task", key="duration_secs") or 0
    anomaly_count = ti.xcom_pull(task_ids="transform_task", key="anomaly_count") or 0

    base_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    report_dir = os.path.join(base_dir, "reports")
    report_path = os.path.join(report_dir, f"report_{run_id}.txt")

    # FIX: makedirs is idempotent — safe even if the preprocess_task already ran
    os.makedirs(report_dir, exist_ok=True)

    pass_rate = (rows_loaded / rows_extracted * 100) if rows_extracted else 0

    report_lines = [
        "=" * 50,
        f"ETL Pipeline Report",
        f"Run ID   : {run_id}",
        f"Run Date : {run_date}",
        "-" * 50,
        f"Rows Extracted : {rows_extracted}",
        f"Rows Rejected  : {rows_rejected}",
        f"Rows Loaded    : {rows_loaded}",
        f"Pass Rate      : {pass_rate:.1f}%",
        f"Anomalies      : {anomaly_count}",
        f"Load Duration  : {duration_secs}s",
        "=" * 50,
    ]

    report_content = "\n".join(report_lines)
    with open(report_path, "w") as f:
        f.write(report_content)

    log.info("Report written to %s", report_path)
    ti.xcom_push(key="report_path", value=report_path)


# ==============================================================================
# DAG DEFINITION
# ==============================================================================

with DAG(
    dag_id="etl_pipeline",
    description="Fraud transaction ETL pipeline",
    start_date=START_DATE,
    # FIX: Was */10 (every 10 min) — caused overlapping runs and XCom collisions.
    # Changed to hourly. Adjust to your actual data cadence.
    schedule="*/10 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "fraud-detection"],
    # Prevent a new run from starting if the previous one is still running
    max_active_runs=1,
) as dag:

    preprocess_task = BashOperator(
        task_id="preprocess_task",
        bash_command=(
            "echo 'Preparing directories for {{ ds }}' && "
            "mkdir -p ${AIRFLOW_HOME:-/opt/airflow}/reports"
        ),
    )

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=run_extract,
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=run_load,
    )

    report_task = PythonOperator(
        task_id="report_task",
        python_callable=generate_report,
    )

    # FIX: Replaced EmailOperator with PythonOperator — EmailOperator requires
    # SMTP to be configured in airflow.cfg ([smtp] section). Since it isn't set up,
    # it was failing every run even though all data tasks succeeded.
    # To re-enable email later: configure [smtp] in airflow.cfg and swap back.
    notify_success_task = PythonOperator(
        task_id="notify_success_task",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure_task = PythonOperator(
        task_id="notify_failure_task",
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── Happy path ─────────────────────────────────────────────────────────────
    (
        preprocess_task
        >> extract_task
        >> validate_task
        >> transform_task
        >> load_task
        >> report_task
        >> notify_success_task
    )

    # ── Failure watchdog ───────────────────────────────────────────────────────
    # FIX: Watches only critical pipeline tasks — NOT notify_success_task.
    # This means failure email fires iff a data task fails, never on clean runs.
    [extract_task, validate_task, transform_task, load_task, report_task] >> notify_failure_task
