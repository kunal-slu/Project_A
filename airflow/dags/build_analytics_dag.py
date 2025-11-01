"""
Build Analytics DAG - Bronze to Gold and Publish.

Pipeline:
- Bronze → Silver (clean, validate)
- Silver → Gold (join, aggregate)
- Publish to Snowflake/Redshift
- Register in Glue Catalog

SLA: 1 hour
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'sla': timedelta(hours=1),  # 1 hour SLA
}

dag = DAG(
    'build_analytics',
    default_args=default_args,
    description='Build Silver and Gold layers, publish to warehouses',
    schedule_interval='0 4 * * *',  # Daily at 4 AM UTC (after ingestion completes)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['transform', 'silver', 'gold'],
)

# Bronze to Silver Transformations
bronze_to_silver_behavior = EmrAddStepsOperator(
    task_id='bronze_to_silver_behavior',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Transform Behavior Bronze to Silver',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/bronze_to_silver_behavior.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

bronze_to_silver_crm = EmrAddStepsOperator(
    task_id='bronze_to_silver_crm',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Transform CRM Bronze to Silver',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/bronze_to_silver_crm.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

bronze_to_silver_orders = EmrAddStepsOperator(
    task_id='bronze_to_silver_orders',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Transform Orders Bronze to Silver',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/bronze_to_silver_orders.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

# DQ Check on Silver (Great Expectations)
dq_check_silver = BashOperator(
    task_id='dq_check_silver',
    bash_command=(
        'python aws/scripts/run_ge_checks.py '
        '--suite-name silver_behavior_checkpoint '
        '--data-asset s3://my-etl-lake-demo/silver/behavior '
        '--fail-on-error'
    ),
    dag=dag,
)

# Silver to Gold Transformations
build_customer_360 = EmrAddStepsOperator(
    task_id='build_customer_360',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Build Customer 360 Gold Table',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/silver_build_customer_360.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

build_product_perf = EmrAddStepsOperator(
    task_id='build_product_perf_daily',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Build Product Performance Daily Gold Table',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/silver_build_product_perf.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

# DQ Check on Gold (Great Expectations)
dq_check_gold = BashOperator(
    task_id='dq_check_gold',
    bash_command=(
        'python aws/scripts/run_ge_checks.py '
        '--suite-name gold_customer_360_checkpoint '
        '--data-asset s3://my-etl-lake-demo/gold/customer_360 '
        '--fail-on-error'
    ),
    dag=dag,
)

# Publish to Warehouses
publish_customer_360_snowflake = EmrAddStepsOperator(
    task_id='publish_customer_360_snowflake',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Publish Customer 360 to Snowflake',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/publish_gold_to_snowflake.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml',
                    '--tables', 'customer_360'
                ]
            }
        }
    ],
)

register_glue_tables = BashOperator(
    task_id='register_glue_tables',
    bash_command='python aws/scripts/register_glue_tables.py --date {{ ds }}',
    dag=dag,
)

# Collect run summary and metrics
collect_run_summary = BashOperator(
    task_id='collect_run_summary',
    bash_command=(
        'python jobs/collect_run_summary.py '
        '--run-id "{{ run_id }}" '
        '--execution-date "{{ ds }}" '
        '--config config/prod.yaml'
    ),
    dag=dag,
)

# Task dependencies
[bronze_to_silver_behavior, bronze_to_silver_crm, bronze_to_silver_orders] >> dq_check_silver
dq_check_silver >> [build_customer_360, build_product_perf] >> dq_check_gold
dq_check_gold >> publish_customer_360_snowflake >> register_glue_tables >> collect_run_summary

