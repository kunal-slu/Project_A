"""
Main ETL DAG for PySpark Data Engineer Project.
Orchestrates the complete data pipeline from ingestion to gold layer.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from plugins.helpers.emr import start_emr_job_run, wait_for_emr_job_completion

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'main_etl_dag',
    default_args=default_args,
    description='Main ETL pipeline for PySpark Data Engineer Project',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['etl', 'pyspark', 'data-lake']
)

# Configuration
PROJECT = Variable.get("project", default_var="pyspark-de-project")
ENVIRONMENT = Variable.get("environment", default_var="dev")
REGION = Variable.get("region", default_var="us-east-1")

# S3 paths
S3_DATA_LAKE_BUCKET = f"{PROJECT}-{ENVIRONMENT}-data-lake"
S3_ARTIFACTS_BUCKET = f"{PROJECT}-{ENVIRONMENT}-artifacts"
S3_LOGS_BUCKET = f"{PROJECT}-{ENVIRONMENT}-logs"

# EMR Serverless configuration
EMR_APPLICATION_ID = Variable.get("emr_application_id", default_var="")
EMR_JOB_ROLE_ARN = Variable.get("emr_job_role_arn", default_var="")

# S3 paths for data layers
BRONZE_PATH = f"s3://{S3_DATA_LAKE_BUCKET}/lake/bronze"
SILVER_PATH = f"s3://{S3_DATA_LAKE_BUCKET}/lake/silver"
GOLD_PATH = f"s3://{S3_DATA_LAKE_BUCKET}/lake/gold"

# Artifacts path
ARTIFACTS_PATH = f"s3://{S3_ARTIFACTS_BUCKET}/artifacts"


def check_data_availability(**context):
    """Check if source data is available."""
    import boto3
    
    s3_client = boto3.client('s3')
    
    # Check for bronze data
    bronze_objects = s3_client.list_objects_v2(
        Bucket=S3_DATA_LAKE_BUCKET,
        Prefix="lake/bronze/"
    )
    
    if not bronze_objects.get('Contents'):
        raise ValueError("No bronze data found. Please ensure data ingestion is complete.")
    
    print(f"Found {len(bronze_objects['Contents'])} bronze data objects")
    return True


def run_bronze_to_silver(**context):
    """Run bronze to silver transformation."""
    job_run_id = start_emr_job_run(
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_JOB_ROLE_ARN,
        entry_point=f"{ARTIFACTS_PATH}/src/jobs/bronze_to_silver.py",
        entry_args=[f"{ARTIFACTS_PATH}/config/aws/config-simple.yaml"],
        spark_params="--conf spark.sql.shuffle.partitions=1",
        name="bronze-to-silver-etl"
    )
    
    # Wait for completion
    result = wait_for_emr_job_completion(
        application_id=EMR_APPLICATION_ID,
        job_run_id=job_run_id,
        max_wait_time=3600
    )
    
    if result['state'] != 'SUCCESS':
        raise ValueError(f"Bronze to Silver job failed: {result['state']}")
    
    print(f"Bronze to Silver job completed successfully: {job_run_id}")
    return job_run_id


def run_silver_to_gold(**context):
    """Run silver to gold transformation."""
    job_run_id = start_emr_job_run(
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_JOB_ROLE_ARN,
        entry_point=f"{ARTIFACTS_PATH}/src/jobs/silver_to_gold.py",
        entry_args=[f"{ARTIFACTS_PATH}/config/aws/config-simple.yaml"],
        spark_params="--conf spark.sql.shuffle.partitions=1",
        name="silver-to-gold-etl"
    )
    
    # Wait for completion
    result = wait_for_emr_job_completion(
        application_id=EMR_APPLICATION_ID,
        job_run_id=job_run_id,
        max_wait_time=3600
    )
    
    if result['state'] != 'SUCCESS':
        raise ValueError(f"Silver to Gold job failed: {result['state']}")
    
    print(f"Silver to Gold job completed successfully: {job_run_id}")
    return job_run_id


def run_data_quality_checks(**context):
    """Run data quality checks using Great Expectations."""
    job_run_id = start_emr_job_run(
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_JOB_ROLE_ARN,
        entry_point=f"{ARTIFACTS_PATH}/src/dq/ge_checkpoint.py",
        entry_args=["users_checkpoint"],
        spark_params="--conf spark.sql.shuffle.partitions=1",
        name="data-quality-checks"
    )
    
    # Wait for completion
    result = wait_for_emr_job_completion(
        application_id=EMR_APPLICATION_ID,
        job_run_id=job_run_id,
        max_wait_time=1800
    )
    
    if result['state'] != 'SUCCESS':
        raise ValueError(f"Data Quality checks failed: {result['state']}")
    
    print(f"Data Quality checks completed successfully: {job_run_id}")
    return job_run_id


def generate_data_docs(**context):
    """Generate Great Expectations data docs."""
    job_run_id = start_emr_job_run(
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_JOB_ROLE_ARN,
        entry_point=f"{ARTIFACTS_PATH}/src/dq/ge_checkpoint.py",
        entry_args=["users_checkpoint", "--generate-docs"],
        spark_params="--conf spark.sql.shuffle.partitions=1",
        name="generate-data-docs"
    )
    
    # Wait for completion
    result = wait_for_emr_job_completion(
        application_id=EMR_APPLICATION_ID,
        job_run_id=job_run_id,
        max_wait_time=1800
    )
    
    if result['state'] != 'SUCCESS':
        raise ValueError(f"Data Docs generation failed: {result['state']}")
    
    print(f"Data Docs generated successfully: {job_run_id}")
    return job_run_id


# Task definitions
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag
)

bronze_to_silver_task = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=run_bronze_to_silver,
    dag=dag
)

silver_to_gold_task = PythonOperator(
    task_id='silver_to_gold',
    python_callable=run_silver_to_gold,
    dag=dag
)

data_quality_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

generate_docs_task = PythonOperator(
    task_id='generate_data_docs',
    python_callable=generate_data_docs,
    dag=dag
)

# Task dependencies
check_data_task >> bronze_to_silver_task >> silver_to_gold_task >> data_quality_task >> generate_docs_task