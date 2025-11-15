"""
Project A Daily Pipeline DAG - Using Unified Entrypoint

This DAG orchestrates the complete Bronze → Silver → Gold pipeline
using the unified entrypoint (run_pipeline.py) with --job argument.
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
import boto3
import json

def notify_failure(context):
    """Failure callback - sends notification on task failure."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    
    print(f"❌ Pipeline Failure: {dag_id}.{task_id} at {execution_date}")
    # In production, send to SNS/Slack here


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


with DAG(
    "project_a_daily_pipeline",
    default_args=default_args,
    description="Daily ETL: Bronze → Silver → Gold using unified entrypoint",
    schedule_interval="0 2 * * *",  # 2 AM UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "production", "unified-entrypoint"],
) as dag:
    
    # Get variables (with fallbacks)
    try:
        emr_app_id = Variable.get("PROJECT_A_EMR_APP_ID")
    except KeyError:
        emr_app_id = "00g0tm6kccmdcf09"
    
    try:
        emr_exec_role_arn = Variable.get("PROJECT_A_EXEC_ROLE_ARN")
    except KeyError:
        emr_exec_role_arn = "arn:aws:iam::424570854632:role/project-a-dev-emr-exec"
    
    try:
        artifacts_bucket = Variable.get("PROJECT_A_ARTIFACTS_BUCKET")
    except KeyError:
        artifacts_bucket = "my-etl-artifacts-demo-424570854632"
    
    try:
        env = Variable.get("PROJECT_A_ENV")
    except KeyError:
        env = "dev"
    
    try:
        config_uri = Variable.get("PROJECT_A_CONFIG_URI")
    except KeyError:
        config_uri = f"s3://{artifacts_bucket}/config/dev.yaml"
    
    # Wheel path
    wheel_path = f"s3://{artifacts_bucket}/packages/project_a-0.1.0-py3-none-any.whl"
    
    # Common job driver configuration
    def get_job_driver(job_name: str, run_date: str = None) -> dict:
        """Create job driver configuration for unified entrypoint."""
        entry_point_args = [
            "--job", job_name,
            "--env", env,
            "--config", config_uri,
        ]
        if run_date:
            entry_point_args.extend(["--run-date", run_date])
        
        return {
            "sparkSubmit": {
                "entryPoint": wheel_path,
                "entryPointArguments": entry_point_args,
                "sparkSubmitParameters": " ".join([
                    "--py-files",
                    f"{wheel_path},s3://{artifacts_bucket}/packages/emr_deps_pyyaml.zip",
                    "--packages",
                    "io.delta:delta-core_2.12:2.4.0",
                    "--conf",
                    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                    "--conf",
                    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                ])
            }
        }
    
    # ============================================================================
    # INGESTION: Source → Bronze
    # ============================================================================
    with TaskGroup("ingest_to_bronze") as ingest_group:
        extract_fx_json = EmrServerlessStartJobRunOperator(
            task_id="extract_fx_json_to_bronze",
            application_id=emr_app_id,
            execution_role_arn=emr_exec_role_arn,
            job_driver=get_job_driver("fx_json_to_bronze"),
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                    }
                }
            },
            execution_timeout=timedelta(hours=2),
            sla=timedelta(minutes=30),
        )
    
    # ============================================================================
    # TRANSFORMATION: Bronze → Silver
    # ============================================================================
    bronze_to_silver = EmrServerlessStartJobRunOperator(
        task_id="bronze_to_silver",
        application_id=emr_app_id,
        execution_role_arn=emr_exec_role_arn,
        job_driver=get_job_driver("bronze_to_silver", run_date="{{ ds }}"),
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                }
            }
        },
        execution_timeout=timedelta(hours=3),
        sla=timedelta(minutes=45),
    )
    
    # ============================================================================
    # TRANSFORMATION: Silver → Gold
    # ============================================================================
    silver_to_gold = EmrServerlessStartJobRunOperator(
        task_id="silver_to_gold",
        application_id=emr_app_id,
        execution_role_arn=emr_exec_role_arn,
        job_driver=get_job_driver("silver_to_gold", run_date="{{ ds }}"),
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                }
            }
        },
        execution_timeout=timedelta(hours=2),
        sla=timedelta(minutes=30),
    )
    
    # ============================================================================
    # PUBLISH: Gold → Snowflake
    # ============================================================================
    publish_gold = EmrServerlessStartJobRunOperator(
        task_id="publish_gold_to_snowflake",
        application_id=emr_app_id,
        execution_role_arn=emr_exec_role_arn,
        job_driver=get_job_driver("publish_gold_to_snowflake", run_date="{{ ds }}"),
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                }
            }
        },
        execution_timeout=timedelta(hours=1),
        sla=timedelta(minutes=15),
    )
    
    # ============================================================================
    # DAG DEPENDENCIES
    # ============================================================================
    ingest_group >> bronze_to_silver >> silver_to_gold >> publish_gold

