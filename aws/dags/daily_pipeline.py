"""
AWS Daily data pipeline DAG with EMR Serverless integration.
Processes data through bronze, silver, and gold layers using EMR Serverless.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_serverless import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr_serverless import EmrServerlessJobSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import os
import boto3

# OpenLineage configuration
OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")
OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "pyspark-de-project")

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# DAG definition
dag = DAG(
    'aws_daily_pipeline',
    default_args=default_args,
    description='AWS Daily data pipeline with EMR Serverless processing',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['aws', 'data-pipeline', 'daily', 'emr-serverless'],
)

# Get configuration from Airflow variables
LAKE_ROOT = Variable.get("LAKE_ROOT", default_var="s3a://pyspark-de-project-dev-data-lake")
CODE_BUCKET = Variable.get("CODE_BUCKET", default_var="pyspark-de-project-dev-artifacts")
EMR_APP_ID = Variable.get("EMR_APP_ID", default_var="")
EMR_JOB_ROLE_ARN = Variable.get("EMR_JOB_ROLE_ARN", default_var="")
REGION = Variable.get("AWS_REGION", default_var="us-east-1")

def get_emr_client():
    """Get EMR Serverless client."""
    return boto3.client('emr-serverless', region_name=REGION)

def submit_emr_job(job_name: str, entry_point: str, extra_args: str = ""):
    """Submit EMR Serverless job with Delta configuration."""
    client = get_emr_client()
    
    # Build Spark submit parameters with Delta configuration
    spark_params = (
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore "
        "--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"
    )
    
    if extra_args:
        spark_params += f" {extra_args}"
    
    # Build entry point arguments
    entry_args = f"--wheel s3://{CODE_BUCKET}/dist/pyspark_interview_project-0.1.0-py3-none-any.whl"
    if extra_args:
        entry_args += f" {extra_args}"
    
    response = client.start_job_run(
        applicationId=EMR_APP_ID,
        executionRoleArn=EMR_JOB_ROLE_ARN,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": [entry_args],
                "sparkSubmitParameters": spark_params
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{CODE_BUCKET}/logs/spark-events/"
                }
            }
        },
        name=job_name
    )
    
    return response['jobRunId']

# Task 1: FX to Bronze
fx_to_bronze = PythonOperator(
    task_id='fx_to_bronze',
    python_callable=lambda: submit_emr_job(
        "fx-to-bronze",
        f"s3://{CODE_BUCKET}/jobs/fx_to_bronze.py",
        "--config s3://{CODE_BUCKET}/config/application-aws.yaml"
    ),
    retries=2,
    retry_delay=timedelta(minutes=5),
    sla=timedelta(minutes=30),
)

# Task 2: Wait for FX to Bronze completion
wait_fx_to_bronze = EmrServerlessJobSensor(
    task_id='wait_fx_to_bronze',
    application_id=EMR_APP_ID,
    job_run_id="{{ ti.xcom_pull(task_ids='fx_to_bronze') }}",
    target_states=['SUCCESS'],
    failed_states=['FAILED', 'CANCELLED'],
    poke_interval=60,
    timeout=3600,
)

# Task 3: FX Bronze to Silver
fx_bronze_to_silver = PythonOperator(
    task_id='fx_bronze_to_silver',
    python_callable=lambda: submit_emr_job(
        "fx-bronze-to-silver",
        f"s3://{CODE_BUCKET}/jobs/fx_bronze_to_silver.py",
        "--config s3://{CODE_BUCKET}/config/application-aws.yaml"
    ),
    retries=2,
    retry_delay=timedelta(minutes=5),
    sla=timedelta(hours=2),
)

# Task 4: Wait for FX Bronze to Silver completion
wait_fx_bronze_to_silver = EmrServerlessJobSensor(
    task_id='wait_fx_bronze_to_silver',
    application_id=EMR_APP_ID,
    job_run_id="{{ ti.xcom_pull(task_ids='fx_bronze_to_silver') }}",
    target_states=['SUCCESS'],
    failed_states=['FAILED', 'CANCELLED'],
    poke_interval=60,
    timeout=3600,
)

# Task 5: Salesforce to Bronze
salesforce_to_bronze = PythonOperator(
    task_id='salesforce_to_bronze',
    python_callable=lambda: submit_emr_job(
        "salesforce-to-bronze",
        f"s3://{CODE_BUCKET}/jobs/salesforce_to_bronze.py",
        "--config s3://{CODE_BUCKET}/config/application-aws.yaml --env SF_SECRET_NAME=pyspark-de-project-dev-salesforce-credentials"
    ),
    retries=2,
    retry_delay=timedelta(minutes=5),
    sla=timedelta(hours=1),
)

# Task 6: Wait for Salesforce to Bronze completion
wait_salesforce_to_bronze = EmrServerlessJobSensor(
    task_id='wait_salesforce_to_bronze',
    application_id=EMR_APP_ID,
    job_run_id="{{ ti.xcom_pull(task_ids='salesforce_to_bronze') }}",
    target_states=['SUCCESS'],
    failed_states=['FAILED', 'CANCELLED'],
    poke_interval=60,
    timeout=3600,
)

# Task 7: Register tables in Glue Catalog
register_catalog = BashOperator(
    task_id='register_catalog',
    bash_command=f"""
    python aws/scripts/register_glue_tables.py \
        --lake-root {LAKE_ROOT} \
        --database pyspark_de_project_silver \
        --layer silver
    python aws/scripts/register_glue_tables.py \
        --lake-root {LAKE_ROOT} \
        --database pyspark_de_project_gold \
        --layer gold
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=30),
)

# Task 8: Run data quality checks
run_dq_checks = BashOperator(
    task_id='run_dq_checks',
    bash_command=f"""
    python aws/scripts/run_ge_checks.py \
        --lake-root {LAKE_ROOT} \
        --lake-bucket {CODE_BUCKET} \
        --suite dq/suites/silver_fx_rates.yml \
        --table fx_rates \
        --layer silver
    python aws/scripts/run_ge_checks.py \
        --lake-root {LAKE_ROOT} \
        --lake-bucket {CODE_BUCKET} \
        --suite dq/suites/silver_orders.yml \
        --table orders \
        --layer silver
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=15),
)

# Task 9: Send completion notification
send_notification = BashOperator(
    task_id='send_notification',
    bash_command="""
    echo "AWS Daily pipeline completed successfully!"
    # This would typically send a notification via SNS, Slack, etc.
    """,
    trigger_rule='all_success',
)

# Task 10: Send failure notification
send_failure_notification = BashOperator(
    task_id='send_failure_notification',
    bash_command="""
    echo "AWS Daily pipeline failed!"
    # This would typically send a failure notification
    """,
    trigger_rule='one_failed',
)

# Define task dependencies
fx_to_bronze >> wait_fx_to_bronze >> fx_bronze_to_silver >> wait_fx_bronze_to_silver >> register_catalog >> run_dq_checks >> send_notification
salesforce_to_bronze >> wait_salesforce_to_bronze >> register_catalog

# Failure path
[fx_to_bronze, fx_bronze_to_silver, salesforce_to_bronze, register_catalog, run_dq_checks] >> send_failure_notification
