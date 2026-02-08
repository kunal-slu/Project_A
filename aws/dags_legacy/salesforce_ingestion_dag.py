"""
Salesforce Ingestion DAG

This DAG orchestrates the extraction of Salesforce data (Accounts, Contacts, Leads, Solutions)
and lands it in the Bronze layer with data quality checks.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_serverless import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr_serverless import EmrServerlessJobSensor

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# DAG definition
dag = DAG(
    "salesforce_ingestion_dag",
    default_args=default_args,
    description="Extract Salesforce data and land in Bronze layer",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    max_active_runs=1,
    tags=["salesforce", "crm", "bronze", "ingestion"],
)

# Configuration
AWS_REGION = Variable.get("aws_region", default_var="us-east-1")
EMR_APPLICATION_ID = Variable.get("emr_application_id")
EMR_EXECUTION_ROLE_ARN = Variable.get("emr_execution_role_arn")
S3_BUCKET = Variable.get("data_lake_bucket")
INGEST_DATE = "{{ ds }}"  # Airflow template variable


def check_salesforce_data_quality(**context):
    """
    Check data quality for Salesforce Bronze data.
    """
    import boto3
    from botocore.exceptions import ClientError

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Define Salesforce objects to check
    objects_to_check = ["accounts", "contacts", "leads", "solutions"]

    quality_results = {}

    for obj in objects_to_check:
        try:
            # Check if parquet file exists
            key = f"bronze/crm/salesforce/{obj}/ingest_date={INGEST_DATE}/{obj}.parquet"

            response = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
            file_size = response["ContentLength"]

            if file_size > 0:
                quality_results[obj] = {
                    "status": "PASS",
                    "file_size": file_size,
                    "message": f"{obj} data ingested successfully",
                }
                logging.info(f"✅ {obj}: {file_size} bytes")
            else:
                quality_results[obj] = {
                    "status": "FAIL",
                    "file_size": 0,
                    "message": f"{obj} file is empty",
                }
                logging.error(f"❌ {obj}: Empty file")

        except ClientError as e:
            quality_results[obj] = {
                "status": "FAIL",
                "file_size": 0,
                "message": f"{obj} file not found: {str(e)}",
            }
            logging.error(f"❌ {obj}: {str(e)}")

    # Check overall quality
    failed_objects = [obj for obj, result in quality_results.items() if result["status"] == "FAIL"]

    if failed_objects:
        error_msg = f"Data quality check failed for: {', '.join(failed_objects)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    else:
        logging.info("✅ All Salesforce data quality checks passed")

    return quality_results


# Task 1: Extract Salesforce data to Bronze
salesforce_extraction_task = EmrServerlessStartJobOperator(
    task_id="extract_salesforce_to_bronze",
    application_id=EMR_APPLICATION_ID,
    execution_role_arn=EMR_EXECUTION_ROLE_ARN,
    job_driver={
        "sparkSubmit": {
            "entryPoint": f"s3://{S3_BUCKET}/jobs/salesforce_to_bronze.py",
            "entryPointArguments": [
                "--config-path",
                "config/prod.yaml",
                "--ingest-date",
                INGEST_DATE,
            ],
            "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    },
    configuration_overrides={
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": f"s3://{S3_BUCKET}/logs/emr-serverless/"}
        }
    },
    dag=dag,
)

# Task 2: Wait for EMR job completion
wait_for_extraction = EmrServerlessJobSensor(
    task_id="wait_for_salesforce_extraction",
    application_id=EMR_APPLICATION_ID,
    job_run_id="{{ ti.xcom_pull(task_ids='extract_salesforce_to_bronze') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Task 3: Data Quality Check
data_quality_check = PythonOperator(
    task_id="salesforce_data_quality_check", python_callable=check_salesforce_data_quality, dag=dag
)

# Task 4: Register Glue Tables
register_glue_tables = BashOperator(
    task_id="register_salesforce_glue_tables",
    bash_command=f"""
    python3 /opt/airflow/dags/scripts/register_glue_tables.py \
        --bucket {S3_BUCKET} \
        --prefix bronze/crm/salesforce/ \
        --ingest-date {INGEST_DATE} \
        --region {AWS_REGION}
    """,
    dag=dag,
)

# Task 5: Emit Lineage and Metrics
emit_lineage_metrics = BashOperator(
    task_id="emit_salesforce_lineage_metrics",
    bash_command=f"""
    python3 /opt/airflow/dags/scripts/emit_lineage_and_metrics.py \
        --source salesforce \
        --target bronze \
        --ingest-date {INGEST_DATE} \
        --region {AWS_REGION}
    """,
    dag=dag,
)

# Task dependencies
(
    salesforce_extraction_task
    >> wait_for_extraction
    >> data_quality_check
    >> register_glue_tables
    >> emit_lineage_metrics
)
