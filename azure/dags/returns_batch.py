from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import timedelta
import pendulum

with DAG(
    "returns_batch_adb",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
) as dag:
    run = DatabricksRunNowOperator(
        task_id="run_job",
        job_id="{{ var.value.DATABRICKS_JOB_ID }}",
        notebook_params={"proc_date": "{{ ds }}", "env": "azure"},
    )












