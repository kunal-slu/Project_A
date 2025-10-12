from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from datetime import datetime, timedelta
import pendulum

default_args = dict(owner="data-eng", retries=3, retry_delay=timedelta(minutes=5))

with DAG(
    "returns_batch",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["etl", "bronze-silver-gold"],
) as dag:
    ready = S3KeySensor(
        task_id="wait_raw",
        bucket_key="raw/returns/{{ ds }}/_SUCCESS",
        bucket_name="my-prod-raw",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    with TaskGroup("etl") as etl:
        run = EmrServerlessStartJobOperator(
            task_id="run_pipeline",
            application_id="{{ var.value.EMR_SERVERLESS_APP }}",
            execution_role_arn="{{ var.value.EMR_JOB_ROLE }}",
            job_driver={
                "sparkSubmit": {
                    "entryPoint": "pdi",  # console_scripts from pyproject
                    "entryPointArguments": ["--proc_date", "{{ ds }}", "--env", "aws"]
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": "s3://my-logs/emr/"}
                }
            },
        )

    ready >> etl












