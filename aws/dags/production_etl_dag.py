"""
Production ETL DAG - Direct Python Execution
Runs the full ETL pipeline using ProductionETLPipeline
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

# Get project root
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/Users/kunal/IdeaProjects/pyspark_data_engineer_project")

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_production_etl(**context):
    """Run the production ETL pipeline."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Add src to path
    sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
    
    logger.info("🚀 Starting Production ETL Pipeline...")
    
    try:
        # Import after adding to path
        from pyspark_interview_project.production_pipeline import ProductionETLPipeline
        
        # Load config
        config = {
            'delta_path': 'data/lakehouse_delta_standard',
            'environment': 'local',
        }
        
        # Run pipeline
        pipeline = ProductionETLPipeline(config)
        pipeline.run()
        
        logger.info("✅ ETL Pipeline completed successfully")
        return {"status": "success", "message": "ETL completed"}
        
    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {e}")
        raise


def validate_delta_output(**context):
    """Validate Delta Lake output."""
    import logging
    logger = logging.getLogger(__name__)
    
    delta_path = os.path.join(PROJECT_ROOT, 'data/lakehouse_delta_standard')
    
    if not os.path.exists(delta_path):
        raise ValueError(f"Delta Lake path not found: {delta_path}")
    
    layers = ['bronze', 'silver', 'gold']
    total_tables = 0
    total_files = 0
    total_versions = 0
    
    for layer in layers:
        layer_path = os.path.join(delta_path, layer)
        if os.path.exists(layer_path):
            logger.info(f"📊 Checking {layer.upper()} layer...")
            for table_name in os.listdir(layer_path):
                table_path = os.path.join(layer_path, table_name)
                if os.path.isdir(table_path) and not table_name.startswith('.'):
                    total_tables += 1
                    
                    # Count parquet files
                    parquet_files = [f for f in os.listdir(table_path) if f.endswith('.parquet')]
                    
                    # Count versions
                    delta_log_path = os.path.join(table_path, '_delta_log')
                    versions = 0
                    if os.path.exists(delta_log_path):
                        versions = len([f for f in os.listdir(delta_log_path) if f.endswith('.json')])
                    
                    total_files += len(parquet_files)
                    total_versions += versions
                    
                    logger.info(f"  ✅ {layer}.{table_name}: {len(parquet_files)} files, {versions} versions")
    
    logger.info(f"📊 SUMMARY: {total_tables} tables, {total_files} files, {total_versions} versions")
    
    if total_tables == 0 or total_versions == 0:
        raise ValueError("No Delta Lake tables or versions found!")
    
    return {
        "total_tables": total_tables,
        "total_files": total_files,
        "total_versions": total_versions,
    }


with DAG(
    dag_id='production_etl_dag',
    default_args=default_args,
    description='Production ETL Pipeline with Delta Lake',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'etl', 'delta-lake'],
) as dag:
    
    start = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "🚀 Starting Production ETL Pipeline..."',
    )
    
    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=run_production_etl,
        execution_timeout=timedelta(minutes=10),
    )
    
    validate = PythonOperator(
        task_id='validate_output',
        python_callable=validate_delta_output,
        execution_timeout=timedelta(minutes=2),
    )
    
    end = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "✅ Production ETL Pipeline completed successfully!"',
    )
    
    start >> run_etl >> validate >> end

