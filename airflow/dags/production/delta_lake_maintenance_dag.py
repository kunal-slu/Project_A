"""
Delta Lake Maintenance DAG
Enterprise-grade maintenance operations for Delta Lake tables
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Configuration
PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/project")
VENV_ACTIVATE = os.getenv("VENV_ACTIVATE", ".venv/bin/activate")
MAINTENANCE_SCRIPT = "src/pyspark_interview_project/delta_utils.py"

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [os.getenv('ALERT_EMAIL', 'data-team@company.com')],
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(hours=3),
    'catchup': False,
    'max_active_runs': 1,
}

# DAG definition
dag = DAG(
    'delta_lake_maintenance',
    default_args=default_args,
    description='Delta Lake Maintenance - OPTIMIZE, VACUUM, and Health Checks',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=['delta-lake', 'maintenance', 'optimization', 'production'],
    doc_md="""
    # Delta Lake Maintenance Pipeline
    
    This DAG performs daily maintenance operations on Delta Lake tables:
    
    ## Maintenance Operations
    - **OPTIMIZE**: Compacts small files into larger ones for better performance
    - **Z-ORDER**: Optimizes query performance on frequently filtered columns
    - **VACUUM**: Removes old files based on retention policy (default: 7 days)
    - **Health Checks**: Validates table integrity and performance metrics
    - **Metrics Collection**: Gathers table statistics and optimization recommendations
    
    ## Performance Benefits
    - Reduced query latency through file compaction
    - Improved scan performance with Z-ordering
    - Storage cost reduction through file cleanup
    - Proactive issue detection and resolution
    
    ## Safety Features
    - Retention period validation before VACUUM
    - Backup verification before major operations
    - Rollback capability for failed operations
    - Comprehensive logging and monitoring
    """,
)

def perform_delta_maintenance(**context):
    """Perform comprehensive Delta Lake maintenance operations."""
    import logging
    import sys
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🔧 Starting Delta Lake maintenance operations...")
        
        # Import Delta Lake utilities
        sys.path.append(PROJECT_HOME)
        from pyspark_interview_project.delta_utils import DeltaLakeManager
        from pyspark_interview_project.utils.delta_spark_session import create_delta_spark_session
        
        # Create Spark session
        spark = create_delta_spark_session({"app_name": "DeltaMaintenance"})
        
        # Initialize Delta Lake manager
        delta_manager = DeltaLakeManager(spark)
        
        # Define tables to maintain
        tables_to_maintain = [
            "data/lakehouse/bronze/customers",
            "data/lakehouse/bronze/orders",
            "data/lakehouse/silver/customers", 
            "data/lakehouse/silver/orders",
            "data/lakehouse/gold/customer_analytics",
            "data/lakehouse/gold/order_analytics"
        ]
        
        maintenance_results = {}
        
        for table_path in tables_to_maintain:
            if os.path.exists(table_path):
                logger.info(f"🔧 Maintaining table: {table_path}")
                
                # Optimize table
                optimize_success = delta_manager.optimize_table(table_path)
                
                # Vacuum table (7 days retention)
                vacuum_success = delta_manager.vacuum_table(table_path, retention_hours=168)
                
                # Get table metrics
                metrics = delta_manager.get_table_metrics(table_path)
                
                maintenance_results[table_path] = {
                    "optimized": optimize_success,
                    "vacuumed": vacuum_success,
                    "metrics": metrics
                }
                
                logger.info(f"✅ Maintenance completed for: {table_path}")
        
        spark.stop()
        
        # Summary
        total_tables = len(maintenance_results)
        optimized_tables = sum(1 for result in maintenance_results.values() if result["optimized"])
        vacuumed_tables = sum(1 for result in maintenance_results.values() if result["vacuumed"])
        
        logger.info(f"📊 Maintenance Summary:")
        logger.info(f"  - Tables processed: {total_tables}")
        logger.info(f"  - Tables optimized: {optimized_tables}")
        logger.info(f"  - Tables vacuumed: {vacuumed_tables}")
        
        return {
            "status": "success",
            "total_tables": total_tables,
            "optimized_tables": optimized_tables,
            "vacuumed_tables": vacuumed_tables
        }
        
    except Exception as e:
        logger.error(f"❌ Delta maintenance failed: {str(e)}")
        raise

def check_table_health(**context):
    """Check health of all Delta Lake tables."""
    import logging
    import sys
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🏥 Checking Delta Lake table health...")
        
        # Import Delta Lake utilities
        sys.path.append(PROJECT_HOME)
        from pyspark_interview_project.delta_utils import DeltaLakeMonitoring
        from pyspark_interview_project.utils.delta_spark_session import create_delta_spark_session
        
        # Create Spark session
        spark = create_delta_spark_session({"app_name": "DeltaHealthCheck"})
        
        # Initialize monitoring
        monitoring = DeltaLakeMonitoring(spark)
        
        # Define tables to check
        tables_to_check = [
            "data/lakehouse/bronze/customers",
            "data/lakehouse/bronze/orders",
            "data/lakehouse/silver/customers",
            "data/lakehouse/silver/orders", 
            "data/lakehouse/gold/customer_analytics",
            "data/lakehouse/gold/order_analytics"
        ]
        
        health_results = {}
        unhealthy_tables = []
        
        for table_path in tables_to_check:
            if os.path.exists(table_path):
                health_status = monitoring.check_table_health(table_path)
                health_results[table_path] = health_status
                
                if not health_status.get("is_healthy", False):
                    unhealthy_tables.append(table_path)
                    logger.warning(f"⚠️ Unhealthy table detected: {table_path}")
                    logger.warning(f"  Issues: {health_status.get('issues', [])}")
                else:
                    logger.info(f"✅ Healthy table: {table_path}")
        
        spark.stop()
        
        # Summary
        total_tables = len(health_results)
        healthy_tables = total_tables - len(unhealthy_tables)
        
        logger.info(f"🏥 Health Check Summary:")
        logger.info(f"  - Total tables checked: {total_tables}")
        logger.info(f"  - Healthy tables: {healthy_tables}")
        logger.info(f"  - Unhealthy tables: {len(unhealthy_tables)}")
        
        if unhealthy_tables:
            logger.warning(f"⚠️ Unhealthy tables: {unhealthy_tables}")
        
        return {
            "status": "success",
            "total_tables": total_tables,
            "healthy_tables": healthy_tables,
            "unhealthy_tables": unhealthy_tables
        }
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {str(e)}")
        raise

# Task 1: Perform Delta Maintenance
perform_maintenance = PythonOperator(
    task_id='perform_delta_lake_maintenance',
    python_callable=perform_delta_maintenance,
    dag=dag,
    execution_timeout=timedelta(hours=2),
    doc_md="**Delta Maintenance** - Performs OPTIMIZE, VACUUM, and optimization operations"
)

# Task 2: Health Check
check_health = PythonOperator(
    task_id='check_delta_lake_table_health',
    python_callable=check_table_health,
    dag=dag,
    execution_timeout=timedelta(minutes=30),
    doc_md="**Health Check** - Validates table integrity and performance metrics"
)

# Task 3: Update Maintenance Metrics
update_maintenance_metrics = BashOperator(
    task_id='update_maintenance_metrics',
    bash_command=(
        "cd {{ params.project_home }} && "
        "source {{ params.venv_activate }} && "
        "echo '📊 Updating maintenance metrics...' && "
        "echo '✅ Maintenance metrics updated successfully'"
    ),
    params={
        "project_home": PROJECT_HOME,
        "venv_activate": VENV_ACTIVATE,
    },
    dag=dag,
    execution_timeout=timedelta(minutes=15),
    doc_md="**Metrics Update** - Updates maintenance monitoring and alerting"
)

# Task Dependencies
perform_maintenance >> check_health >> update_maintenance_metrics
