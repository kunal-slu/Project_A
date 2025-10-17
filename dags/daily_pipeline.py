"""
Daily data pipeline DAG with OpenLineage support.
Processes data through bronze, silver, and gold layers.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_serverless import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr_serverless import EmrServerlessJobSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import os

# Standardized Airflow Variables
EMR_APP_ID = Variable.get("EMR_APP_ID")
EMR_JOB_ROLE_ARN = Variable.get("EMR_JOB_ROLE_ARN")
GLUE_DB_SILVER = Variable.get("GLUE_DB_SILVER", "silver_db")
GLUE_DB_GOLD = Variable.get("GLUE_DB_GOLD", "gold_db")
S3_LAKE_BUCKET = Variable.get("S3_LAKE_BUCKET")
S3_CHECKPOINT_PATH = Variable.get("S3_CHECKPOINT_PREFIX", "checkpoints")

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
    'daily_pipeline',
    default_args=default_args,
    description='Daily data pipeline with bronze, silver, and gold processing',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['data-pipeline', 'daily', 'bronze', 'silver', 'gold'],
)

# Get configuration from Airflow variables
LAKE_ROOT = Variable.get("LAKE_ROOT", default_var="s3://pyspark-de-project-dev-data-lake")
CODE_BUCKET = Variable.get("CODE_BUCKET", default_var="pyspark-de-project-dev-artifacts")
EMR_APP_ID = Variable.get("EMR_APP_ID", default_var="")
EMR_JOB_ROLE_ARN = Variable.get("EMR_JOB_ROLE_ARN", default_var="")
REGION = Variable.get("AWS_REGION", default_var="us-east-1")

# Task 1: Extract data from sources
extract_sources = BashOperator(
    task_id='extract_sources',
    bash_command="""
    echo "Extracting data from various sources..."
    # This would typically trigger Lambda functions or other extractors
    # For now, we'll simulate the extraction
    echo "Data extraction completed"
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=30),
)

# Task 2: Process bronze to silver
bronze_to_silver = EmrServerlessStartJobOperator(
    task_id='bronze_to_silver',
    application_id=EMR_APP_ID,
    execution_role_arn=EMR_JOB_ROLE_ARN,
    job_driver={
        "sparkSubmit": {
            "entryPoint": f"s3://{CODE_BUCKET}/jobs/bronze_to_silver.py",
            "entryPointArguments": [
                "--config", f"s3://{CODE_BUCKET}/config/application-aws.yaml"
            ],
            "sparkSubmitParameters": (
                "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
                "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
                "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore "
                "--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"
            )
        }
    },
    configuration_overrides={
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": f"{LAKE_ROOT}/logs/spark-events/"
            }
        }
    },
    retries=2,
    retry_delay=timedelta(minutes=5),
    sla=timedelta(hours=2),
)

# Task 3: Wait for bronze to silver completion
wait_bronze_to_silver = EmrServerlessJobSensor(
    task_id='wait_bronze_to_silver',
    application_id=EMR_APP_ID,
    job_run_id="{{ ti.xcom_pull(task_ids='bronze_to_silver') }}",
    target_states=['SUCCESS'],
    failed_states=['FAILED', 'CANCELLED'],
    poke_interval=60,
    timeout=3600,
)

# Task 4: Process silver to gold
silver_to_gold = EmrServerlessStartJobOperator(
    task_id='silver_to_gold',
    application_id=EMR_APP_ID,
    execution_role_arn=EMR_JOB_ROLE_ARN,
    job_driver={
        "sparkSubmit": {
            "entryPoint": f"s3://{CODE_BUCKET}/jobs/silver_to_gold.py",
            "entryPointArguments": [
                "--config", f"s3://{CODE_BUCKET}/config/application-aws.yaml"
            ],
            "sparkSubmitParameters": (
                "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
                "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
                "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore "
                "--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"
            )
        }
    },
    configuration_overrides={
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": f"{LAKE_ROOT}/logs/spark-events/"
            }
        }
    },
    retries=2,
    retry_delay=timedelta(minutes=5),
    sla=timedelta(hours=2),
)

# Task 5: Wait for silver to gold completion
wait_silver_to_gold = EmrServerlessJobSensor(
    task_id='wait_silver_to_gold',
    application_id=EMR_APP_ID,
    job_run_id="{{ ti.xcom_pull(task_ids='silver_to_gold') }}",
    target_states=['SUCCESS'],
    failed_states=['FAILED', 'CANCELLED'],
    poke_interval=60,
    timeout=3600,
)

# Task 6: Register tables in Glue Catalog
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

# Task 7: Run data quality checks
run_dq_checks = BashOperator(
    task_id='run_dq_checks',
    bash_command=f"""
    python aws/scripts/run_ge_checks.py \
        --lake-root {LAKE_ROOT} \
        --lake-bucket {LAKE_ROOT.split('/')[-1]} \
        --suite dq/suites/silver_orders.yml \
        --table orders \
        --layer silver
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=15),
)

# Task 8: Send completion notification
send_notification = BashOperator(
    task_id='send_notification',
    bash_command="""
    echo "Daily pipeline completed successfully!"
    # This would typically send a notification via SNS, Slack, etc.
    """,
    trigger_rule='all_success',
)

# Task 9: Send failure notification
send_failure_notification = BashOperator(
    task_id='send_failure_notification',
    bash_command="""
    echo "Daily pipeline failed!"
    # This would typically send a failure notification
    """,
    trigger_rule='one_failed',
)

# Define task dependencies
extract_sources >> bronze_to_silver >> wait_bronze_to_silver >> silver_to_gold >> wait_silver_to_gold >> register_catalog >> run_dq_checks >> send_notification

# Failure path
[bronze_to_silver, silver_to_gold, register_catalog, run_dq_checks] >> send_failure_notification
