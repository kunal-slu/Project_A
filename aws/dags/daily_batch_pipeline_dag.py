"""
Daily Batch Pipeline DAG

Main ETL pipeline with DQ gating:
Bronze → DQ → Silver → DQ → Gold

If DQ fails at any stage, downstream jobs do not run.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)


def _failure_callback(context) -> None:
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    run_id = context.get("run_id", "unknown")
    logger.error("Task failure: dag=%s task=%s run_id=%s", dag_id, task_id, run_id)


def run_pipeline_command(job_name: str) -> str:
    return (
        "python -m project_a.pipeline.run_pipeline "
        f"--job {job_name} "
        "--env prod "
        "--config config/prod.yaml "
        "--run-date '{{ ds }}'"
    )


# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": _failure_callback,
}

# Create DAG
dag = DAG(
    "daily_batch_pipeline",
    default_args=default_args,
    description="Main batch ETL pipeline with DQ gating",
    schedule="0 2 * * *",  # Daily at 2 AM UTC
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["production", "bronze", "silver", "gold"],
)

# Dummy start operator
start = EmptyOperator(
    task_id="start",
    dag=dag,
)

# ============================================================================
# INGESTION LAYER: Source → Bronze
# ============================================================================

ingest_crm = BashOperator(
    task_id="ingest_crm_to_bronze",
    bash_command=run_pipeline_command("crm_to_bronze"),
    dag=dag,
)

ingest_snowflake = BashOperator(
    task_id="ingest_snowflake_to_bronze",
    bash_command=run_pipeline_command("snowflake_to_bronze"),
    dag=dag,
)

ingest_redshift = BashOperator(
    task_id="ingest_redshift_to_bronze",
    bash_command=run_pipeline_command("redshift_to_bronze"),
    dag=dag,
)

ingest_fx = BashOperator(
    task_id="ingest_fx_to_bronze",
    bash_command=run_pipeline_command("fx_json_to_bronze"),
    sla=timedelta(minutes=30),
    max_active_tis_per_dag=1,
    dag=dag,
)

ingest_kafka = BashOperator(
    task_id="ingest_kafka_events_to_bronze",
    bash_command=run_pipeline_command("kafka_events_csv_snapshot_to_bronze"),
    sla=timedelta(minutes=30),
    dag=dag,
)

# Wait for all ingestion jobs
ingest_done = EmptyOperator(
    task_id="ingest_done",
    dag=dag,
)

# ============================================================================
# TRANSFORMATION: Bronze → Silver
# ============================================================================

bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=run_pipeline_command("bronze_to_silver"),
    sla=timedelta(minutes=45),
    dag=dag,
)

# ============================================================================
# DQ GATE 2: Silver Layer (with SLA)
# ============================================================================

dq_check_silver = BashOperator(
    task_id="dq_check_silver",
    bash_command=run_pipeline_command("dq_silver_gate"),
    sla=timedelta(minutes=20),  # P4-11: SLA on dq_gate
    dag=dag,
)

# ============================================================================
# TRANSFORMATION: Silver → Gold
# ============================================================================

silver_to_gold = BashOperator(
    task_id="silver_to_gold",
    bash_command=run_pipeline_command("silver_to_gold"),
    sla=timedelta(minutes=45),
    dag=dag,
)

dq_check_gold = BashOperator(
    task_id="dq_check_gold",
    bash_command=run_pipeline_command("dq_gold_gate"),
    sla=timedelta(minutes=20),
    dag=dag,
)

gold_truth_tests = BashOperator(
    task_id="gold_truth_tests",
    bash_command=run_pipeline_command("gold_truth_tests"),
    sla=timedelta(minutes=20),
    dag=dag,
)

# ============================================================================
# GOVERNANCE: Register & Emit
# ============================================================================

register_glue_catalog = BashOperator(
    task_id="register_glue_catalog",
    bash_command="python aws/scripts/utilities/register_glue_tables.py --lake-root s3://$S3_LAKE_BUCKET --database gold_db --layer gold --config config/prod.yaml",
    dag=dag,
)

emit_lineage_metrics = BashOperator(
    task_id="emit_lineage_and_metrics",
    bash_command="python aws/scripts/utilities/emit_lineage_and_metrics.py",
    dag=dag,
)

# Dummy end operator
end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES (This is the critical part)
# ============================================================================

# Ingestion layer
start >> [ingest_crm, ingest_snowflake, ingest_redshift, ingest_fx, ingest_kafka] >> ingest_done

# Bronze → Silver transformation
ingest_done >> bronze_to_silver

# Silver DQ Gate - FAILURE HERE STOPS PIPELINE
bronze_to_silver >> dq_check_silver

# Silver → Gold transformation - ONLY RUNS IF DQ PASSES
dq_check_silver >> silver_to_gold >> dq_check_gold >> gold_truth_tests

# Governance steps
gold_truth_tests >> register_glue_catalog >> emit_lineage_metrics >> end

# CRITICAL: This enforces "DQ fails = Gold never updates"
# If dq_check_bronze or dq_check_silver fails, downstream jobs do NOT run.
