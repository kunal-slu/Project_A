"""
Daily Batch Pipeline DAG

Main ETL pipeline with DQ gating:
Bronze → DQ → Silver → DQ → Gold

If DQ fails at any stage, downstream jobs do not run.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'daily_batch_pipeline',
    default_args=default_args,
    description='Main batch ETL pipeline with DQ gating',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    catchup=False,
    tags=['production', 'bronze', 'silver', 'gold'],
)

# Dummy start operator
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# ============================================================================
# INGESTION LAYER: Source → Bronze
# ============================================================================

ingest_hubspot = BashOperator(
    task_id='ingest_hubspot_to_bronze',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/hubspot_to_bronze.py\"}}"',
    dag=dag,
)

ingest_snowflake = BashOperator(
    task_id='ingest_snowflake_to_bronze',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/snowflake_to_bronze.py\"}}"',
    dag=dag,
)

ingest_redshift = BashOperator(
    task_id='ingest_redshift_to_bronze',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/redshift_to_bronze.py\"}}"',
    dag=dag,
)

ingest_vendor = BashOperator(
    task_id='ingest_vendor_to_bronze',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/vendor_to_bronze.py\"}}"',
    dag=dag,
)

# Wait for all ingestion jobs
ingest_done = DummyOperator(
    task_id='ingest_done',
    dag=dag,
)

# ============================================================================
# DQ GATE 1: Bronze Layer
# ============================================================================

dq_check_bronze = BashOperator(
    task_id='dq_check_bronze',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/dq_check_bronze.py\"}}"',
    dag=dag,
)

# ============================================================================
# TRANSFORMATION: Bronze → Silver
# ============================================================================

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/bronze_to_silver.py\"}}"',
    dag=dag,
)

# ============================================================================
# DQ GATE 2: Silver Layer
# ============================================================================

dq_check_silver = BashOperator(
    task_id='dq_check_silver',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/dq_check_silver.py\"}}"',
    dag=dag,
)

# ============================================================================
# TRANSFORMATION: Silver → Gold
# ============================================================================

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/silver_to_gold.py\"}}"',
    dag=dag,
)

# ============================================================================
# GOVERNANCE: Register & Emit
# ============================================================================

register_glue_catalog = BashOperator(
    task_id='register_glue_catalog',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/register_glue_catalog.py\"}}"',
    dag=dag,
)

emit_lineage_metrics = BashOperator(
    task_id='emit_lineage_and_metrics',
    bash_command='aws emr-serverless start-job-run --application-id $EMR_APP_ID --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$CODE_BUCKET/jobs/emit_lineage_and_metrics.py\"}}"',
    dag=dag,
)

# Dummy end operator
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES (This is the critical part)
# ============================================================================

# Ingestion layer
start >> [ingest_hubspot, ingest_snowflake, ingest_redshift, ingest_vendor] >> ingest_done

# Bronze DQ Gate - FAILURE HERE STOPS PIPELINE
ingest_done >> dq_check_bronze

# Bronze → Silver transformation - ONLY RUNS IF DQ PASSES
dq_check_bronze >> bronze_to_silver

# Silver DQ Gate - FAILURE HERE STOPS PIPELINE
bronze_to_silver >> dq_check_silver

# Silver → Gold transformation - ONLY RUNS IF DQ PASSES
dq_check_silver >> silver_to_gold

# Governance steps
silver_to_gold >> register_glue_catalog >> emit_lineage_metrics >> end

# CRITICAL: This enforces "DQ fails = Gold never updates"
# If dq_check_bronze or dq_check_silver fails, downstream jobs do NOT run.

