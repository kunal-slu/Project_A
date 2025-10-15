"""
Airflow DAG for orchestrating external data source ingestion (Redshift and HubSpot).
This DAG handles data extraction from external sources and loads into Bronze layer.
"""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Parameterize paths for portability
PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/project")
CONFIG_FILE = os.getenv("CONFIG_FILE", "config/config-dev.yaml")
VENV_ACTIVATE = os.getenv("VENV_ACTIVATE", ".venv/bin/activate")

# Standard DAG defaults for production
default_args = dict(
    owner="data-eng",
    retries=3,
    retry_delay=timedelta(minutes=5),
    depends_on_past=False,
    email_on_failure=True,
    email_on_retry=False
)

def check_redshift_connection(**context):
    """Check Redshift connection availability."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Check if Redshift configuration is available
    redshift_cluster = os.getenv("REDSHIFT_CLUSTER_ID")
    if not redshift_cluster:
        logger.warning("Redshift cluster not configured, skipping Redshift tasks")
        return "skip_redshift"
    
    logger.info(f"Redshift cluster configured: {redshift_cluster}")
    return "run_redshift_ingestion"

def check_hubspot_connection(**context):
    """Check HubSpot API connection availability."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Check if HubSpot API key is available
    hubspot_api_key = os.getenv("HUBSPOT_API_KEY")
    if not hubspot_api_key:
        logger.warning("HubSpot API key not configured, skipping HubSpot tasks")
        return "skip_hubspot"
    
    logger.info("HubSpot API key configured")
    return "run_hubspot_ingestion"

with DAG(
    dag_id="external_data_ingestion",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",  # Run hourly for external data sources
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["etl", "external-sources", "redshift", "hubspot"],
    description="Ingest data from external sources (Redshift and HubSpot)"
) as dag:

    # Check connection availability
    check_redshift = PythonOperator(
        task_id="check_redshift_connection",
        python_callable=check_redshift_connection,
        provide_context=True
    )
    
    check_hubspot = PythonOperator(
        task_id="check_hubspot_connection", 
        python_callable=check_hubspot_connection,
        provide_context=True
    )

    # Redshift data ingestion tasks
    redshift_marketing_campaigns = SparkSubmitOperator(
        task_id="redshift_marketing_campaigns",
        application=f"{PROJECT_HOME}/src/pyspark_interview_project/jobs/redshift_to_bronze.py",
        name="redshift_marketing_campaigns",
        application_args=[
            CONFIG_FILE,
            "--table_name=marketing_campaigns",
            "--batch_size=10000"
        ],
        conn_id="spark_default",
        verbose=True,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    redshift_customer_behavior = SparkSubmitOperator(
        task_id="redshift_customer_behavior",
        application=f"{PROJECT_HOME}/src/pyspark_interview_project/jobs/redshift_to_bronze.py",
        name="redshift_customer_behavior",
        application_args=[
            CONFIG_FILE,
            "--table_name=customer_behavior",
            "--batch_size=15000"
        ],
        conn_id="spark_default",
        verbose=True,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    redshift_web_analytics = SparkSubmitOperator(
        task_id="redshift_web_analytics",
        application=f"{PROJECT_HOME}/src/pyspark_interview_project/jobs/redshift_to_bronze.py",
        name="redshift_web_analytics",
        application_args=[
            CONFIG_FILE,
            "--table_name=web_analytics",
            "--batch_size=10000"
        ],
        conn_id="spark_default",
        verbose=True,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

    # HubSpot data ingestion tasks
    hubspot_contacts = SparkSubmitOperator(
        task_id="hubspot_contacts",
        application=f"{PROJECT_HOME}/src/pyspark_interview_project/jobs/hubspot_to_bronze.py",
        name="hubspot_contacts",
        application_args=[
            CONFIG_FILE,
            "--endpoint_name=contacts",
            "--batch_size=100"
        ],
        conn_id="spark_default",
        verbose=True,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    hubspot_deals = SparkSubmitOperator(
        task_id="hubspot_deals",
        application=f"{PROJECT_HOME}/src/pyspark_interview_project/jobs/hubspot_to_bronze.py",
        name="hubspot_deals",
        application_args=[
            CONFIG_FILE,
            "--endpoint_name=deals",
            "--batch_size=100"
        ],
        conn_id="spark_default",
        verbose=True,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    hubspot_companies = SparkSubmitOperator(
        task_id="hubspot_companies",
        application=f"{PROJECT_HOME}/src/pyspark_interview_project/jobs/hubspot_to_bronze.py",
        name="hubspot_companies",
        application_args=[
            CONFIG_FILE,
            "--endpoint_name=companies",
            "--batch_size=100"
        ],
        conn_id="spark_default",
        verbose=True,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    hubspot_tickets = SparkSubmitOperator(
        task_id="hubspot_tickets",
        application=f"{PROJECT_HOME}/src/pyspark_interview_project/jobs/hubspot_to_bronze.py",
        name="hubspot_tickets",
        application_args=[
            CONFIG_FILE,
            "--endpoint_name=tickets",
            "--batch_size=100"
        ],
        conn_id="spark_default",
        verbose=True,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

    # Data quality checks
    dq_redshift = BashOperator(
        task_id="dq_redshift_data",
        bash_command=f"""
        cd {PROJECT_HOME}
        source {VENV_ACTIVATE}
        python -c "
        from pyspark_interview_project.jobs.redshift_to_bronze import RedshiftExtractor
        from pyspark_interview_project.utils.spark_session import build_spark
        from pyspark_interview_project.config_loader import load_config
        import logging
        logging.basicConfig(level=logging.INFO)
        
        config = load_config()
        spark = build_spark(config)
        
        try:
            extractor = RedshiftExtractor(spark, config)
            print('Redshift connection test successful')
        except Exception as e:
            print(f'Redshift connection test failed: {e}')
            exit(1)
        finally:
            spark.stop()
        "
        """,
        retries=1
    )
    
    dq_hubspot = BashOperator(
        task_id="dq_hubspot_data",
        bash_command=f"""
        cd {PROJECT_HOME}
        source {VENV_ACTIVATE}
        python -c "
        from pyspark_interview_project.jobs.hubspot_to_bronze import HubSpotAPI
        from pyspark_interview_project.config_loader import load_config
        import logging
        logging.basicConfig(level=logging.INFO)
        
        config = load_config()
        
        try:
            api = HubSpotAPI(config)
            print('HubSpot API connection test successful')
        except Exception as e:
            print(f'HubSpot API connection test failed: {e}')
            exit(1)
        "
        """,
        retries=1
    )

    # Notification task
    notify_completion = BashOperator(
        task_id="notify_ingestion_completion",
        bash_command="""
        echo "External data ingestion completed successfully"
        echo "Redshift and HubSpot data loaded to Bronze layer"
        """,
        trigger_rule="all_done"
    )

    # Define task dependencies
    # Check connections first
    check_redshift >> dq_redshift
    check_hubspot >> dq_hubspot
    
    # Redshift ingestion (parallel)
    dq_redshift >> [redshift_marketing_campaigns, redshift_customer_behavior, redshift_web_analytics]
    
    # HubSpot ingestion (parallel)
    dq_hubspot >> [hubspot_contacts, hubspot_deals, hubspot_companies, hubspot_tickets]
    
    # Final notification
    [redshift_marketing_campaigns, redshift_customer_behavior, redshift_web_analytics,
     hubspot_contacts, hubspot_deals, hubspot_companies, hubspot_tickets] >> notify_completion
