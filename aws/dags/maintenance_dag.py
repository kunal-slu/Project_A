"""
Delta Lake Maintenance DAG

Weekly maintenance operations:
- OPTIMIZE tables (compact files, Z-ORDER)
- VACUUM tables (remove old files)
- Check table health
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'delta_maintenance',
    default_args=default_args,
    description='Weekly Delta Lake maintenance (OPTIMIZE/VACUUM)',
    schedule_interval='0 2 * * 0',  # Every Sunday at 2 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'delta', 'optimization'],
) as dag:
    
    optimize_tables = EmrServerlessStartJobOperator(
        task_id='optimize_delta_tables',
        application_id='{{ var.value.EMR_APP_ID }}',
        execution_role_arn='{{ var.value.EMR_JOB_ROLE_ARN }}',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://{{ var.value.ARTIFACTS_BUCKET }}/jobs/delta_optimize_vacuum.py',
                'sparkSubmitParameters': '--optimize-only --config config/prod.yaml'
            }
        },
        configuration_overrides={
            'monitoringConfiguration': {
                's3MonitoringConfiguration': {
                    'logUri': 's3://{{ var.value.LOGS_BUCKET }}/maintenance/'
                }
            }
        }
    )
    
    vacuum_tables = EmrServerlessStartJobOperator(
        task_id='vacuum_delta_tables',
        application_id='{{ var.value.EMR_APP_ID }}',
        execution_role_arn='{{ var.value.EMR_JOB_ROLE_ARN }}',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://{{ var.value.ARTIFACTS_BUCKET }}/jobs/delta_optimize_vacuum.py',
                'sparkSubmitParameters': '--vacuum-only --config config/prod.yaml'
            }
        },
        configuration_overrides={
            'monitoringConfiguration': {
                's3MonitoringConfiguration': {
                    'logUri': 's3://{{ var.value.LOGS_BUCKET }}/maintenance/'
                }
            }
        }
    )
    
    check_table_health = BashOperator(
        task_id='check_table_health',
        bash_command='python aws/jobs/dq_check_silver.py --config config/prod.yaml'
    )
    
    # Dependencies
    optimize_tables >> vacuum_tables >> check_table_health

