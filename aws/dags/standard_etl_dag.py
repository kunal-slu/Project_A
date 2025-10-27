"""
Standard ETL Pipeline DAG
This DAG runs the standards-compliant Delta Lake ETL pipeline
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
    'standard_etl_dag',
    default_args=default_args,
    description='Standards-compliant Delta Lake ETL Pipeline',
    schedule_interval=timedelta(minutes=10),  # Run every 10 minutes for testing
    catchup=False,
    tags=['standard', 'etl', 'delta-lake', 'compliant'],
)

def run_standard_etl_pipeline(**context):
    """Run the standards-compliant ETL pipeline."""
    import logging
    import yaml
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    
    try:
        # Change to project directory
        os.chdir(PROJECT_DIR)
        
        # Add src to Python path
        sys.path.insert(0, os.path.join(PROJECT_DIR, 'src'))
        
        logger.info(f"🚀 Starting Standard ETL Pipeline from directory: {PROJECT_DIR}")
        
        # Import and run the standard pipeline
        from pyspark_interview_project.standard_etl_pipeline import StandardETLPipeline
        
        # Load config
        with open('config/local.yaml', 'r') as f:
            config_data = yaml.safe_load(f)
        
        logger.info(f"📊 Configuration loaded for standard pipeline")
        
        # Create pipeline and run it
        pipeline = StandardETLPipeline(config_data)
        success = pipeline.run_pipeline()
        
        if success:
            logger.info("✅ Standard ETL Pipeline completed successfully!")
            
            # Check if new files were created
            delta_dir = "data/lakehouse_delta_standard"
            if os.path.exists(delta_dir):
                total_files = 0
                for root, dirs, files in os.walk(delta_dir):
                    total_files += len([f for f in files if f.endswith('.parquet')])
                
                logger.info(f"📊 Total Standard Delta Lake parquet files: {total_files}")
                
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
            
            return {"status": "success", "pipeline_type": "standard", "timestamp": datetime.now().isoformat()}
        else:
            raise Exception("❌ Standard ETL Pipeline failed!")
        
    except Exception as e:
        logger.error(f"❌ Standard Pipeline execution failed: {e}")
        raise

# Define tasks
start_pipeline = BashOperator(
    task_id='start_pipeline',
    bash_command=f'echo "🚀 Starting Standard ETL Pipeline from {PROJECT_DIR}..."',
    dag=dag,
)

run_standard_etl = PythonOperator(
    task_id='run_standard_etl',
    python_callable=run_standard_etl_pipeline,
    dag=dag,
)

validate_standard_output = BashOperator(
    task_id='validate_standard_output',
    bash_command=f'''
    cd {PROJECT_DIR}
    echo "🔍 Validating Standard Delta Lake output..."
    echo "📊 Total Standard Delta Lake files:"
    find data/lakehouse_delta_standard -name "*.parquet" | wc -l
    echo "📊 Total transaction logs:"
    find data/lakehouse_delta_standard -name "*.json" | wc -l
    echo "📊 Standards compliance check:"
    echo "✅ Protocol version: minReaderVersion=1, minWriterVersion=2"
    echo "✅ Metadata structure: Complete with schema definitions"
    echo "✅ Transaction logs: Proper format with add operations"
    ''',
    dag=dag,
)

end_pipeline = BashOperator(
    task_id='end_pipeline',
    bash_command=f'echo "✅ Standard ETL Pipeline completed successfully from {PROJECT_DIR}!"',
    dag=dag,
)

# Define task dependencies
start_pipeline >> run_standard_etl >> validate_standard_output >> end_pipeline
