"""
Production Delta Lake ETL Pipeline DAG
Runs the production ETL pipeline with Delta Lake support
"""

from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Configuration with safe defaults
PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/project")
VENV_ACTIVATE = os.getenv("VENV_ACTIVATE", ".venv/bin/activate")
DELTA_LAKE_SCRIPT = "src/pyspark_interview_project/production_pipeline.py"

# Safe variable retrieval with defaults
EMR_APP_ID = Variable.get("EMR_APP_ID", default_var="")
EMR_JOB_ROLE_ARN = Variable.get("EMR_JOB_ROLE_ARN", default_var="")
AWS_REGION = Variable.get("AWS_REGION", default_var="us-east-1")
S3_DATA_BUCKET = Variable.get("S3_DATA_BUCKET", default_var="")

# Default arguments with improved retry logic
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# Create DAG
dag = DAG(
    'delta_lake_etl_pipeline_dag',
    default_args=default_args,
    description='Production Delta Lake ETL Pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['production', 'etl', 'delta-lake'],
)

def run_production_etl_pipeline(**context):
    """Run the production ETL pipeline with Delta Lake support."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Change to project directory
        os.chdir(PROJECT_HOME)
        
        # Add src to Python path
        sys.path.insert(0, os.path.join(PROJECT_HOME, 'src'))
        
        # Import and run the production pipeline
        from pyspark_interview_project.production_pipeline import ProductionETLPipeline
        
        config = {
            "base_path": "data/lakehouse",
            "delta_path": "data/lakehouse_delta"
        }
        
        pipeline = ProductionETLPipeline(config)
        success = pipeline.run_pipeline()
        
        if success:
            logger.info("✅ Production ETL Pipeline completed successfully!")
            return {"status": "success", "pipeline_type": "delta_lake"}
        else:
            raise Exception("❌ Production ETL Pipeline failed!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        raise

# Define tasks
start_pipeline = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "🚀 Starting Production Delta Lake ETL Pipeline..."',
    dag=dag,
)

run_etl_pipeline = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_production_etl_pipeline,
    dag=dag,
)

validate_output = BashOperator(
    task_id='validate_output',
    bash_command='''
    echo "🔍 Validating Delta Lake output..."
    ls -la data/lakehouse_delta/
    find data/lakehouse_delta -name "_delta_log" -type d | wc -l | xargs echo "Delta Lake tables with time travel:"
    ''',
    dag=dag,
)

end_pipeline = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "✅ Production Delta Lake ETL Pipeline completed successfully!"',
    dag=dag,
)

# Define task dependencies
start_pipeline >> run_etl_pipeline >> validate_output >> end_pipeline
