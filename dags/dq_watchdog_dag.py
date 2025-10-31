"""
Data Quality Watchdog DAG

This independent DAG runs data quality checks on a schedule and alerts
when SLAs are broken or data quality issues are detected.

This is monitoring-only - it does NOT move data forward.
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pendulum
import boto3
import json

default_args = {
    'owner': 'data-eng',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
}

# Data Quality Thresholds
DQ_THRESHOLDS = {
    'freshness_hours': 2,  # Max hours old
    'null_percentage': 0.05,  # 5% max null
    'volume_deviation': 0.20,  # 20% deviation from expected
    'quality_score_min': 0.80  # 80% minimum
}


def check_data_quality(**context):
    """Check data quality across Bronze and Silver layers."""
    
    import logging
    logger = logging.getLogger(__name__)
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    bucket_name = 'company-data-lake-ACCOUNT_ID'
    
    dq_results = {
        'bronze': {},
        'silver': {},
        'issues': []
    }
    
    # Check Bronze layer
    for table in ['customers', 'orders', 'products', 'payments']:
        try:
            # Check freshness
            latest_partition = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f'bronze/{table}/'
            )
            
            if 'Contents' in latest_partition:
                latest_file = max(latest_partition['Contents'], key=lambda x: x['LastModified'])
                hours_old = (datetime.utcnow() - latest_file['LastModified'].replace(tzinfo=None)).total_seconds() / 3600
                
                if hours_old > DQ_THRESHOLDS['freshness_hours']:
                    dq_results['bronze'][table] = {
                        'status': 'STALE',
                        'hours_old': hours_old
                    }
                    dq_results['issues'].append(f"Bronze {table}: Stale data ({hours_old:.1f} hours old)")
                else:
                    dq_results['bronze'][table] = {'status': 'OK'}
            else:
                dq_results['issues'].append(f"Bronze {table}: No data found")
                
        except Exception as e:
            logger.error(f"Error checking {table}: {str(e)}")
            dq_results['issues'].append(f"Bronze {table}: Error - {str(e)}")
    
    # Check Silver layer
    for table in ['customers_clean', 'orders_clean', 'products_clean']:
        try:
            latest_partition = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f'silver/{table}/'
            )
            
            if 'Contents' in latest_partition:
                latest_file = max(latest_partition['Contents'], key=lambda x: x['LastModified'])
                hours_old = (datetime.utcnow() - latest_file['LastModified'].replace(tzinfo=None)).total_seconds() / 3600
                
                if hours_old > DQ_THRESHOLDS['freshness_hours']:
                    dq_results['silver'][table] = {
                        'status': 'STALE',
                        'hours_old': hours_old
                    }
                    dq_results['issues'].append(f"Silver {table}: Stale data ({hours_old:.1f} hours old)")
                else:
                    dq_results['silver'][table] = {'status': 'OK'}
            else:
                dq_results['issues'].append(f"Silver {table}: No data found")
                
        except Exception as e:
            logger.error(f"Error checking {table}: {str(e)}")
            dq_results['issues'].append(f"Silver {table}: Error - {str(e)}")
    
    return dq_results


def send_alert_if_needed(**context):
    """Send alert if data quality issues detected."""
    
    import logging
    logger = logging.getLogger(__name__)
    
    ti = context['ti']
    dq_results = ti.xcom_pull(task_ids='check_dq')
    
    if dq_results and dq_results.get('issues'):
        # Send alert via SNS
        sns_client = boto3.client('sns')
        topic_arn = 'arn:aws:sns:us-east-1:ACCOUNT_ID:dq-alerts'
        
        alert_message = {
            'subject': 'Data Quality Alert - SLA Violation',
            'body': {
                'timestamp': datetime.utcnow().isoformat(),
                'issues': dq_results['issues'],
                'bronze_status': dq_results['bronze'],
                'silver_status': dq_results['silver']
            }
        }
        
        try:
            response = sns_client.publish(
                TopicArn=topic_arn,
                Subject=alert_message['subject'],
                Message=json.dumps(alert_message['body'], indent=2)
            )
            logger.info(f"Alert sent: {response['MessageId']}")
            logger.error(f"DQ issues detected: {dq_results['issues']}")
            
        except Exception as e:
            logger.error(f"Failed to send alert: {str(e)}")
    else:
        logger.info("No data quality issues detected")


with DAG(
    'dq_watchdog',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),  # Check every hour
    catchup=False,
    max_active_runs=1,
    tags=['data-quality', 'monitoring', 'watchdog'],
) as dag:
    
    check_dq = PythonOperator(
        task_id='check_dq',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    alert_on_issues = PythonOperator(
        task_id='alert_on_issues',
        python_callable=send_alert_if_needed,
        provide_context=True,
    )
    
    check_dq >> alert_on_issues
