"""
Project A Local ETL DAG

Runs the full local pipeline using the project job runner.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

# Ensure DAG imports do not depend on Airflow SDK-only XCom backends
os.environ.setdefault("AIRFLOW__CORE__XCOM_BACKEND", "airflow.models.xcom.BaseXCom")

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


DEFAULT_CONFIG = "/opt/airflow/config/local_airflow.yaml"
ENV_NAME = "local"
PROJECT_SRC = "/opt/project/src"
PYTHON_BIN = "python"


def job_cmd(job_name: str) -> str:
    return (
        f"PYTHONPATH={PROJECT_SRC} {PYTHON_BIN} -m project_a.pipeline.run_pipeline "
        f"--job {job_name} --env {ENV_NAME} --config {DEFAULT_CONFIG}"
    )


def gold_truth_cmd() -> str:
    return (
        f"PYTHONPATH={PROJECT_SRC} {PYTHON_BIN} -m project_a.pipeline.run_pipeline "
        f"--job gold_truth_tests --env {ENV_NAME} --config {DEFAULT_CONFIG}"
    )


def dbt_build_cmd() -> str:
    return (
        "DBT_PROFILES_DIR=/opt/project/dbt "
        "DBT_PROJECT_DIR=/opt/project/dbt "
        "DBT_TARGET=local_iceberg "
        "DBT_FILE_FORMAT=iceberg "
        "DBT_LOCATION_ROOT=file:///opt/project/data/iceberg "
        "DBT_ICEBERG_WAREHOUSE=file:///opt/project/data/iceberg "
        "dbt deps --project-dir /opt/project/dbt --profiles-dir /opt/project/dbt && "
        "dbt build --project-dir /opt/project/dbt --profiles-dir /opt/project/dbt"
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "project_a_local_etl",
    default_args=DEFAULT_ARGS,
    description="Local Project A ETL (Bronze -> Silver -> Gold) with DQ and truth tests",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["local", "bronze", "silver", "gold"],
)

start = EmptyOperator(task_id="start", dag=dag)

ingest_fx = BashOperator(
    task_id="fx_json_to_bronze",
    bash_command=job_cmd("fx_json_to_bronze"),
    dag=dag,
)

ingest_snowflake = BashOperator(
    task_id="snowflake_to_bronze",
    bash_command=job_cmd("snowflake_to_bronze"),
    dag=dag,
)

ingest_crm = BashOperator(
    task_id="crm_to_bronze",
    bash_command=job_cmd("crm_to_bronze"),
    dag=dag,
)

ingest_redshift = BashOperator(
    task_id="redshift_to_bronze",
    bash_command=job_cmd("redshift_to_bronze"),
    dag=dag,
)

ingest_kafka = BashOperator(
    task_id="kafka_csv_to_bronze",
    bash_command=job_cmd("kafka_csv_to_bronze"),
    dag=dag,
)

ingest_done = EmptyOperator(task_id="ingest_done", dag=dag)

bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=job_cmd("bronze_to_silver"),
    dag=dag,
)

dq_silver_gate = BashOperator(
    task_id="dq_silver_gate",
    bash_command=job_cmd("dq_silver_gate"),
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id="silver_to_gold",
    bash_command=job_cmd("silver_to_gold"),
    dag=dag,
)

dq_gold_gate = BashOperator(
    task_id="dq_gold_gate",
    bash_command=job_cmd("dq_gold_gate"),
    dag=dag,
)

gold_truth_tests = BashOperator(
    task_id="gold_truth_tests",
    bash_command=gold_truth_cmd(),
    dag=dag,
)

dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command=dbt_build_cmd(),
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> [
    ingest_fx,
    ingest_snowflake,
    ingest_crm,
    ingest_redshift,
    ingest_kafka,
] >> ingest_done

ingest_done >> bronze_to_silver >> dq_silver_gate >> silver_to_gold >> dq_gold_gate >> gold_truth_tests >> dbt_build >> end
