"""
DQ Watchdog DAG - Independent Data Quality Monitoring.

Runs every hour to check:
- Data freshness (latest partition date)
- Volume checks (row count thresholds)
- SLA breaches

Alerts on violations without moving data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dq_watchdog',
    default_args=default_args,
    description='Independent DQ monitoring and alerting',
    schedule_interval='0 * * * *',  # Every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dq', 'monitoring', 'watchdog'],
)


def check_freshness(**context):
    """Check if latest partitions are fresh."""
    import json
    import boto3
    from datetime import datetime, timezone
    
    s3 = boto3.client('s3')
    bucket = 'my-etl-lake-demo'
    
    # Define freshness SLAs (hours)
    slas = {
        'bronze': 1,
        'silver': 2,
        'gold': 4
    }
    
    violations = []
    
    for layer, sla_hours in slas.items():
        # Get latest partition date
        prefix = f'{layer}/'
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        
        if 'CommonPrefixes' in response:
            dates = []
            for prefix_info in response['CommonPrefixes']:
                path = prefix_info['Prefix']
                # Extract date from path like "bronze/orders/_proc_date=2025-10-31/"
                if '_proc_date=' in path:
                    date_str = path.split('_proc_date=')[1].rstrip('/')
                    dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
            
            if dates:
                latest_date = max(dates)
                now = datetime.now(timezone.utc)
                age_hours = (now - latest_date.replace(tzinfo=timezone.utc)).total_seconds() / 3600
                
                if age_hours > sla_hours:
                    violations.append({
                        'layer': layer,
                        'latest_partition': latest_date.isoformat(),
                        'age_hours': age_hours,
                        'sla_hours': sla_hours
                    })
    
    if violations:
        print(f"DQ Freshness Violations: {json.dumps(violations, indent=2)}")
        # In production, send to SNS/Slack
        raise ValueError(f"SLA violations detected: {len(violations)}")
    else:
        print("✅ All freshness checks passed")


def check_volume(**context):
    """Check if row counts meet thresholds."""
    import json
    from pyspark.sql import SparkSession
    from pyspark_interview_project.utils.spark_session import build_spark
    from pyspark_interview_project.utils.config import load_conf
    
    try:
        config = load_conf('config/prod.yaml')
    except FileNotFoundError:
        config = load_conf('config/local.yaml')
    spark = build_spark(app_name='dq_watchdog_volume', config=config)
    
    try:
        # Define volume thresholds
        thresholds = {
            'bronze.behavior': 1000,
            'silver.orders': 50000,
            'gold.customer_360': 10000
        }
        
        violations = []
        
        for table_path, min_rows in thresholds.items():
            try:
                df = spark.read.format('delta').load(f"s3a://my-etl-lake-demo/{table_path.replace('.', '/')}")
                count = df.count()
                
                if count < min_rows:
                    violations.append({
                        'table': table_path,
                        'actual': count,
                        'threshold': min_rows
                    })
            except Exception as e:
                violations.append({
                    'table': table_path,
                    'error': str(e)
                })
        
        if violations:
            print(f"DQ Volume Violations: {json.dumps(violations, indent=2)}")
            raise ValueError(f"Volume checks failed: {len(violations)} violations")
        else:
            print("✅ All volume checks passed")
    finally:
        spark.stop()


# Tasks
check_freshness_task = PythonOperator(
    task_id='check_freshness',
    python_callable=check_freshness,
    dag=dag,
)

check_volume_task = PythonOperator(
    task_id='check_volume',
    python_callable=check_volume,
    dag=dag,
)

run_ge_checks = BashOperator(
    task_id='run_ge_checks',
    bash_command='python aws/scripts/run_ge_checks.py --all --watchdog',
    dag=dag,
)

# Dependencies
[check_freshness_task, check_volume_task] >> run_ge_checks

