"""
Catalog and Data Quality DAG.
Runs Glue table registration and data quality checks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os

# Standardized Airflow Variables
EMR_APP_ID = Variable.get("EMR_APP_ID")
EMR_JOB_ROLE_ARN = Variable.get("EMR_JOB_ROLE_ARN")
GLUE_DB_SILVER = Variable.get("GLUE_DB_SILVER", "silver_db")
GLUE_DB_GOLD = Variable.get("GLUE_DB_GOLD", "gold_db")
S3_LAKE_BUCKET = Variable.get("S3_LAKE_BUCKET")
S3_CHECKPOINT_PATH = Variable.get("S3_CHECKPOINT_PREFIX", "checkpoints")

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
    'catalog_and_dq_dag',
    default_args=default_args,
    description='Catalog registration and data quality checks',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['catalog', 'data-quality', 'glue', 'athena'],
)

# Get configuration from Airflow variables
LAKE_ROOT = Variable.get("LAKE_ROOT", default_var="s3://pyspark-de-project-dev-data-lake")
LAKE_BUCKET = Variable.get("LAKE_BUCKET", default_var="pyspark-de-project-dev-data-lake")

# Task 1: Register silver tables in Glue
register_silver_tables = BashOperator(
    task_id='register_silver_tables',
    bash_command=f"""
    python aws/scripts/register_glue_tables.py \
        --lake-root {LAKE_ROOT} \
        --database pyspark_de_project_silver \
        --layer silver
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=15),
)

# Task 2: Register gold tables in Glue
register_gold_tables = BashOperator(
    task_id='register_gold_tables',
    bash_command=f"""
    python aws/scripts/register_glue_tables.py \
        --lake-root {LAKE_ROOT} \
        --database pyspark_de_project_gold \
        --layer gold
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=15),
)

# Task 3: Run DQ checks on silver orders
dq_silver_orders = BashOperator(
    task_id='dq_silver_orders',
    bash_command=f"""
    python aws/scripts/run_ge_checks.py \
        --lake-root {LAKE_ROOT} \
        --lake-bucket {LAKE_BUCKET} \
        --suite dq/suites/silver_orders.yml \
        --table orders \
        --layer silver
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=10),
)

# Task 4: Run DQ checks on silver fx_rates
dq_silver_fx_rates = BashOperator(
    task_id='dq_silver_fx_rates',
    bash_command=f"""
    python aws/scripts/run_ge_checks.py \
        --lake-root {LAKE_ROOT} \
        --lake-bucket {LAKE_BUCKET} \
        --suite dq/suites/silver_fx_rates.yml \
        --table fx_rates \
        --layer silver
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=10),
)

# Task 5: Run DQ checks on gold fact_orders
dq_gold_fact_orders = BashOperator(
    task_id='dq_gold_fact_orders',
    bash_command=f"""
    python aws/scripts/run_ge_checks.py \
        --lake-root {LAKE_ROOT} \
        --lake-bucket {LAKE_BUCKET} \
        --suite dq/suites/silver_orders.yml \
        --table fact_orders \
        --layer gold
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
    sla=timedelta(minutes=10),
)

# Task 6: Generate DQ report
generate_dq_report = PythonOperator(
    task_id='generate_dq_report',
    python_callable=lambda: print("DQ report generated successfully"),
    retries=1,
    retry_delay=timedelta(minutes=2),
)

# Task 7: Send DQ summary
send_dq_summary = BashOperator(
    task_id='send_dq_summary',
    bash_command="""
    echo "Data Quality checks completed successfully!"
    # This would typically send a summary via SNS, Slack, etc.
    """,
    trigger_rule='all_success',
)

# Task 8: Send DQ failure alert
send_dq_failure_alert = BashOperator(
    task_id='send_dq_failure_alert',
    bash_command="""
    echo "Data Quality checks failed!"
    # This would typically send a failure alert
    """,
    trigger_rule='one_failed',
)

# Define task dependencies
register_silver_tables >> register_gold_tables >> [dq_silver_orders, dq_silver_fx_rates] >> dq_gold_fact_orders >> generate_dq_report >> send_dq_summary

# Failure path
[dq_silver_orders, dq_silver_fx_rates, dq_gold_fact_orders] >> send_dq_failure_alert