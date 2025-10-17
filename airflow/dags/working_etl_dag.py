"""
Working ETL Pipeline DAG
This DAG actually runs the ETL pipeline and creates new files in the lakehouse
"""

from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Get the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create DAG
dag = DAG(
    'working_etl_dag',
    default_args=default_args,
    description='Working ETL Pipeline that creates new Delta Lake files',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes for testing
    catchup=False,
    tags=['working', 'etl', 'delta-lake'],
)

def run_etl_pipeline(**context):
    """Run the ETL pipeline and create new files in the lakehouse."""
    import logging
    import yaml
    import pandas as pd
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    
    try:
        # Change to project directory
        os.chdir(PROJECT_DIR)
        
        # Add src to Python path
        sys.path.insert(0, os.path.join(PROJECT_DIR, 'src'))
        
        logger.info(f"🚀 Starting ETL Pipeline from directory: {PROJECT_DIR}")
        
        # Import and run the production pipeline
        from pyspark_interview_project.production_pipeline import ProductionETLPipeline
        
        # Load config
        with open('config/local.yaml', 'r') as f:
            config_data = yaml.safe_load(f)
        
        logger.info(f"📊 Configuration loaded: {config_data}")
        
        # Create pipeline and run it
        pipeline = ProductionETLPipeline(config_data)
        success = pipeline.run_pipeline()
        
        if success:
            logger.info("✅ ETL Pipeline completed successfully!")
            
            # Check if new files were created
            delta_dir = "data/lakehouse_delta"
            if os.path.exists(delta_dir):
                # Count files before and after
                total_files = 0
                for root, dirs, files in os.walk(delta_dir):
                    total_files += len([f for f in files if f.endswith('.parquet')])
                
                logger.info(f"📊 Total Delta Lake parquet files: {total_files}")
                
                # Show latest files
                for layer in ['bronze', 'silver', 'gold']:
                    layer_path = os.path.join(delta_dir, layer)
                    if os.path.exists(layer_path):
                        for table in os.listdir(layer_path):
                            table_path = os.path.join(layer_path, table)
                            if os.path.isdir(table_path):
                                parquet_files = [f for f in os.listdir(table_path) if f.endswith('.parquet')]
                                if parquet_files:
                                    latest_file = max(parquet_files)
                                    file_path = os.path.join(table_path, latest_file)
                                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                                    logger.info(f"📄 Latest {layer}.{table}: {latest_file} (modified: {file_time})")
            
            return {"status": "success", "timestamp": datetime.now().isoformat()}
        else:
            raise Exception("❌ ETL Pipeline failed!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        raise

def check_new_files(**context):
    """Check for new files created by the ETL pipeline."""
    import logging
    import os
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    
    # Change to project directory
    os.chdir(PROJECT_DIR)
    
    logger.info("🔍 Checking for new files in the lakehouse...")
    
    delta_dir = "data/lakehouse_delta"
    if os.path.exists(delta_dir):
        recent_files = []
        cutoff_time = datetime.now() - timedelta(minutes=10)  # Files modified in last 10 minutes
        
        for root, dirs, files in os.walk(delta_dir):
            for file in files:
                if file.endswith('.parquet') or file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if file_time > cutoff_time:
                        relative_path = os.path.relpath(file_path, PROJECT_DIR)
                        recent_files.append((relative_path, file_time))
        
        if recent_files:
            logger.info(f"📊 Found {len(recent_files)} recently modified files:")
            for file_path, file_time in sorted(recent_files, key=lambda x: x[1], reverse=True)[:10]:
                logger.info(f"   📄 {file_path} (modified: {file_time})")
        else:
            logger.warning("⚠️ No recent files found in the lakehouse")
    
    return {"recent_files": len(recent_files)}

# Define tasks
start_pipeline = BashOperator(
    task_id='start_pipeline',
    bash_command=f'echo "🚀 Starting Working ETL Pipeline from {PROJECT_DIR}..."',
    dag=dag,
)

run_etl_pipeline = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_etl_pipeline,
    dag=dag,
)

check_new_files = PythonOperator(
    task_id='check_new_files',
    python_callable=check_new_files,
    dag=dag,
)

validate_output = BashOperator(
    task_id='validate_output',
    bash_command=f'''
    cd {PROJECT_DIR}
    echo "🔍 Validating Delta Lake output..."
    echo "📊 Total Delta Lake files:"
    find data/lakehouse_delta -name "*.parquet" | wc -l
    echo "📊 Total transaction logs:"
    find data/lakehouse_delta -name "*.json" | wc -l
    echo "📊 Recent files (last 5 minutes):"
    find data/lakehouse_delta -name "*.parquet" -mmin -5
    ''',
    dag=dag,
)

end_pipeline = BashOperator(
    task_id='end_pipeline',
    bash_command=f'echo "✅ Working ETL Pipeline completed successfully from {PROJECT_DIR}!"',
    dag=dag,
)

# Define task dependencies
start_pipeline >> run_etl_pipeline >> check_new_files >> validate_output >> end_pipeline
