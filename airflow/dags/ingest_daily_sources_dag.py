"""
Daily Ingestion DAG - Sources to Bronze.

Ingests data from 5 upstream sources:
- CRM (Salesforce/HubSpot) - incremental
- Snowflake DW - batch replace
- Redshift Analytics - append-only
- FX Rates - daily upsert
- Kafka - stream replayable

SLA: 30 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30),  # 30 minute SLA
}

dag = DAG(
    'ingest_daily_sources',
    default_args=default_args,
    description='Ingest data from all upstream sources to Bronze',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'bronze'],
)

# CRM Incremental Extract
extract_crm_contacts = EmrAddStepsOperator(
    task_id='extract_crm_contacts',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Extract CRM Contacts',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/jobs/extract_crm_contacts.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

extract_crm_accounts = EmrAddStepsOperator(
    task_id='extract_crm_accounts',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Extract CRM Accounts',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/extract_crm_accounts.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

# Snowflake Batch Extract
extract_snowflake_orders = EmrAddStepsOperator(
    task_id='extract_snowflake_orders',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Extract Snowflake Orders',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/extract_snowflake_orders.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

# Redshift Append-Only Extract
extract_redshift_behavior = EmrAddStepsOperator(
    task_id='extract_redshift_behavior',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Extract Redshift Behavior',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/extract_redshift_behavior.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

# FX Rates Daily Upsert
extract_fx_rates = EmrAddStepsOperator(
    task_id='extract_fx_rates',
    dag=dag,
    job_flow_id='{{ var.json.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Extract FX Rates',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.deploy.SparkSubmit',
                    '--master', 'yarn',
                    's3://your-code-bucket/jobs/extract_fx_rates.py',
                    '--config', 's3://your-config-bucket/config/prod.yaml'
                ]
            }
        }
    ],
)

# DQ Check on Bronze
dq_check_bronze = BashOperator(
    task_id='dq_check_bronze',
    bash_command='python aws/scripts/run_ge_checks.py --layer bronze --date {{ ds }}',
    dag=dag,
)

# Task dependencies
[extract_crm_contacts, extract_crm_accounts] >> extract_snowflake_orders >> extract_redshift_behavior >> extract_fx_rates >> dq_check_bronze

