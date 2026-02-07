"""
Project A Daily Pipeline DAG - Complete ETL Orchestration

This DAG orchestrates the complete data pipeline:
- Phase 1: Ingestion (5 sources → Bronze)
  - Snowflake → Bronze
  - CRM → Bronze
  - Redshift → Bronze
  - FX JSON → Bronze
  - Kafka → Bronze
- Phase 2: Transformation (Bronze → Silver)
  - Data cleaning, conforming, deduplication
- Phase 3: DQ Gate (Silver validation)
- Phase 4: Transformation (Silver → Gold)
  - Dimension and fact table builds
- Phase 5: DQ Gate (Gold validation)
- Phase 6: Publish (Gold → Redshift & Snowflake)

Dependencies:
- Airflow Variables (required in production):
  - PROJECT_A_EMR_APP_ID: EMR Serverless application ID
  - PROJECT_A_EXEC_ROLE_ARN: EMR execution role ARN
  - PROJECT_A_ARTIFACTS_BUCKET: S3 bucket for artifacts
  - PROJECT_A_ENV: Environment name (dev/staging/prod)
  - PROJECT_A_CONFIG_URI: S3 URI to config YAML
- Config file at config_uri must exist in S3
- Wheel package must be uploaded to s3://{artifacts_bucket}/packages/project_a-0.1.0-py3-none-any.whl
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
    
    # Get variables from Airflow Variables (required in production)
    # Safe defaults for local dev/testing only
    import os
    
    # Check if running in production (MWAA) vs local dev
    is_production = os.getenv("AIRFLOW_ENV") == "production" or Variable.get("PROJECT_A_ENV", default_var="dev") == "prod"
    
    if is_production:
        # In production, all variables must be set - fail fast if missing
        try:
            emr_app_id = Variable.get("PROJECT_A_EMR_APP_ID")
        except KeyError:
            raise ValueError("PROJECT_A_EMR_APP_ID Airflow Variable is required in production")
        
        try:
            emr_exec_role_arn = Variable.get("PROJECT_A_EXEC_ROLE_ARN")
        except KeyError:
            raise ValueError("PROJECT_A_EXEC_ROLE_ARN Airflow Variable is required in production")
        
        try:
            artifacts_bucket = Variable.get("PROJECT_A_ARTIFACTS_BUCKET")
        except KeyError:
            raise ValueError("PROJECT_A_ARTIFACTS_BUCKET Airflow Variable is required in production")
    else:
        # Local dev: use safe defaults
        emr_app_id = Variable.get("PROJECT_A_EMR_APP_ID", default_var="00g0tm6kccmdcf09")
        emr_exec_role_arn = Variable.get("PROJECT_A_EXEC_ROLE_ARN", default_var="arn:aws:iam::424570854632:role/project-a-dev-emr-exec")
        artifacts_bucket = Variable.get("PROJECT_A_ARTIFACTS_BUCKET", default_var="my-etl-artifacts-demo-424570854632")
    
    # Environment and config (can have defaults)
    env = Variable.get("PROJECT_A_ENV", default_var="dev")
    config_uri = Variable.get("PROJECT_A_CONFIG_URI", default_var=f"s3://{artifacts_bucket}/config/dev.yaml")
    
    # Wheel path
    wheel_path = f"s3://{artifacts_bucket}/packages/project_a-0.1.0-py3-none-any.whl"
    
    # Common job driver configuration
    def get_job_driver(job_name: str, run_date: str = None) -> dict:
        """Create job driver configuration for EMR jobs.
        
        For bronze_to_silver and silver_to_gold, use the actual job scripts from jobs/transform/
        These are uploaded to S3 and executed directly to ensure AWS and local use identical code.
        """
        # Map job names to actual S3 entry points
        job_entry_points = {
            "bronze_to_silver": f"s3://{artifacts_bucket}/jobs/transform/bronze_to_silver.py",
            "silver_to_gold": f"s3://{artifacts_bucket}/jobs/transform/silver_to_gold.py",
        }
        
        # Use actual job script if available, otherwise use unified entrypoint
        entry_point = job_entry_points.get(job_name, wheel_path)
        
        if job_name in ["bronze_to_silver", "silver_to_gold"]:
            # Direct job script execution
            entry_point_args = [
                "--env", env,
                "--config", config_uri,
            ]
        else:
            # Unified entrypoint (for other jobs)
            entry_point_args = [
                "--job", job_name,
                "--env", env,
                "--config", config_uri,
            ]
            if run_date:
                entry_point_args.extend(["--run-date", run_date])
        
        return {
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": entry_point_args,
                "sparkSubmitParameters": " ".join([
                    "--py-files",
                    f"{wheel_path},s3://{artifacts_bucket}/packages/emr_deps_pyyaml.zip",
                    "--jars",
                    f"s3://{artifacts_bucket}/packages/delta-spark_2.12-3.2.0.jar,s3://{artifacts_bucket}/packages/delta-storage-3.2.0.jar",
                    "--conf",
                    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                    "--conf",
                    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    "--conf",
                    "spark.executor.instances=1",
                    "--conf",
                    "spark.executor.cores=1",
                    "--conf",
                    "spark.executor.memory=3G",
                    "--conf",
                    "spark.driver.memory=2G"
                ])
            }
        }
    
    # ============================================================================
    # INGESTION: Source → Bronze (5 sources)
    # ============================================================================
    with TaskGroup("ingest_to_bronze") as ingest_group:
        snowflake_bronze = EmrServerlessStartJobRunOperator(
            task_id="snowflake_to_bronze",
            application_id=emr_app_id,
            execution_role_arn=emr_exec_role_arn,
            job_driver=get_job_driver("snowflake_to_bronze", run_date="{{ ds }}"),
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                    }
                }
            },
            execution_timeout=timedelta(hours=2),
        )
        
        crm_bronze = EmrServerlessStartJobRunOperator(
            task_id="crm_to_bronze",
            application_id=emr_app_id,
            execution_role_arn=emr_exec_role_arn,
            job_driver=get_job_driver("crm_to_bronze", run_date="{{ ds }}"),
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                    }
                }
            },
            execution_timeout=timedelta(hours=2),
        )
        
        redshift_bronze = EmrServerlessStartJobRunOperator(
            task_id="redshift_to_bronze",
            application_id=emr_app_id,
            execution_role_arn=emr_exec_role_arn,
            job_driver=get_job_driver("redshift_to_bronze", run_date="{{ ds }}"),
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                    }
                }
            },
            execution_timeout=timedelta(hours=2),
        )
        
        fx_bronze = EmrServerlessStartJobRunOperator(
            task_id="fx_json_to_bronze",
            application_id=emr_app_id,
            execution_role_arn=emr_exec_role_arn,
            job_driver=get_job_driver("fx_json_to_bronze", run_date="{{ ds }}"),
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                    }
                }
            },
            execution_timeout=timedelta(hours=2),
        )
        
        kafka_bronze = EmrServerlessStartJobRunOperator(
            task_id="kafka_csv_to_bronze",
            application_id=emr_app_id,
            execution_role_arn=emr_exec_role_arn,
            job_driver=get_job_driver("kafka_csv_to_bronze", run_date="{{ ds }}"),
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                    }
                }
            },
            execution_timeout=timedelta(hours=2),
        )
    
    # Bronze complete marker
    bronze_complete = PythonOperator(
        task_id="bronze_complete",
        python_callable=lambda: print("✅ All bronze ingestion jobs completed"),
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
    # DQ GATE: Silver
    # ============================================================================
    dq_silver = EmrServerlessStartJobRunOperator(
        task_id="dq_silver_gate",
        application_id=emr_app_id,
        execution_role_arn=emr_exec_role_arn,
        job_driver=get_job_driver("dq_silver_gate", run_date="{{ ds }}"),
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                }
            }
        },
        execution_timeout=timedelta(hours=1),
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
    # DQ GATE: Gold
    # ============================================================================
    dq_gold = EmrServerlessStartJobRunOperator(
        task_id="dq_gold_gate",
        application_id=emr_app_id,
        execution_role_arn=emr_exec_role_arn,
        job_driver=get_job_driver("dq_gold_gate", run_date="{{ ds }}"),
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{artifacts_bucket}/emr-logs/"
                }
            }
        },
        execution_timeout=timedelta(hours=1),
    )
    
    # ============================================================================
    # PUBLISH: Gold → Redshift & Snowflake (parallel)
    # ============================================================================
    with TaskGroup("publish_gold") as publish_group:
        publish_redshift = EmrServerlessStartJobRunOperator(
            task_id="publish_gold_to_redshift",
            application_id=emr_app_id,
            execution_role_arn=emr_exec_role_arn,
            job_driver=get_job_driver("publish_gold_to_redshift", run_date="{{ ds }}"),
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
        
        publish_snowflake = EmrServerlessStartJobRunOperator(
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
    [
        snowflake_bronze,
        crm_bronze,
        redshift_bronze,
        fx_bronze,
        kafka_bronze
    ] >> bronze_complete >> bronze_to_silver >> dq_silver >> silver_to_gold >> dq_gold >> publish_group

