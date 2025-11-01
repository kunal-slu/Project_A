"""
Daily batch ETL DAG with P1-P5 integrations.

Demonstrates complete production pipeline with DQ, lineage, monitoring, and alerts.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.utils.airflow_helpers import on_etl_success, on_etl_failure

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'daily_batch_etl_with_monitoring',
    default_args=default_args,
    description='Production ETL with DQ, lineage, monitoring',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'batch', 'etl', 'dq', 'lineage']
)


def run_bronze_ingestion(**context):
    """Ingest from sources to Bronze with lineage tracking."""
    from src.pyspark_interview_project.extract.snowflake_orders import extract_snowflake_orders
    from src.pyspark_interview_project.extract.redshift_behavior import extract_redshift_behavior
    from src.pyspark_interview_project.utils.spark_session import build_spark
    from src.pyspark_interview_project.utils.config import load_conf
    
    config = load_conf("config/prod.yaml")
    spark = build_spark("bronze_ingestion", config)
    
    try:
        # Extract with lineage
        orders_df = extract_snowflake_orders(spark, config)
        behavior_df = extract_redshift_behavior(spark, config)
        
        print(f"✅ Ingested: {orders_df.count()} orders, {behavior_df.count()} behavior events")
        return True
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        raise
    finally:
        spark.stop()


def run_silver_transformation(**context):
    """Transform Bronze to Silver with DQ validation."""
    from src.pyspark_interview_project.transform.bronze_to_silver import transform_bronze_to_silver
    from src.pyspark_interview_project.utils.spark_session import build_spark
    from src.pyspark_interview_project.utils.config import load_conf
    
    config = load_conf("config/prod.yaml")
    spark = build_spark("silver_transformation", config)
    
    try:
        # Read bronze
        df = spark.read.format("delta").load("s3://bucket/bronze/customer_behavior/")
        
        # Transform with DQ (automatically validates)
        silver_df = transform_bronze_to_silver(
            spark=spark,
            df=df,
            table="customer_behavior",
            config=config
        )
        
        print(f"✅ Silver transform: {silver_df.count()} records")
        return True
    except Exception as e:
        print(f"❌ Silver transform failed: {e}")
        raise
    finally:
        spark.stop()


def run_gold_transformation(**context):
    """Transform Silver to Gold with DQ validation."""
    from src.pyspark_interview_project.transform.silver_to_gold import transform_silver_to_gold
    from src.pyspark_interview_project.utils.spark_session import build_spark
    from src.pyspark_interview_project.utils.config import load_conf
    
    config = load_conf("config/prod.yaml")
    spark = build_spark("gold_transformation", config)
    
    try:
        # Read silver
        df = spark.read.format("delta").load("s3://bucket/silver/customer_behavior/")
        
        # Transform with DQ (automatically validates)
        gold_df = transform_silver_to_gold(
            spark=spark,
            df=df,
            table_name="customer_360",
            config=config
        )
        
        print(f"✅ Gold transform: {gold_df.count()} records")
        return True
    except Exception as e:
        print(f"❌ Gold transform failed: {e}")
        raise
    finally:
        spark.stop()


def publish_to_snowflake(**context):
    """Publish Gold to Snowflake for analytics."""
    from src.pyspark_interview_project.io.snowflake_writer import write_df_to_snowflake
    from src.pyspark_interview_project.utils.spark_session import build_spark
    from src.pyspark_interview_project.utils.config import load_conf
    
    config = load_conf("config/prod.yaml")
    spark = build_spark("snowflake_publish", config)
    
    try:
        # Read gold
        df = spark.read.format("delta").load("s3://bucket/gold/customer_360/")
        
        # Write to Snowflake with MERGE
        write_df_to_snowflake(
            spark=spark,
            df=df,
            table_name="ANALYTICS.CUSTOMER_360",
            config=config,
            mode="merge",
            pk=["customer_id", "event_ts"]
        )
        
        print("✅ Published to Snowflake")
        return True
    except Exception as e:
        print(f"❌ Snowflake publish failed: {e}")
        raise
    finally:
        spark.stop()


# Define tasks
bronze_ingestion = PythonOperator(
    task_id='bronze_ingestion',
    python_callable=run_bronze_ingestion,
    on_success_callback=on_etl_success,
    on_failure_callback=on_etl_failure,
    dag=dag
)

silver_transformation = PythonOperator(
    task_id='silver_transformation',
    python_callable=run_silver_transformation,
    on_success_callback=on_etl_success,
    on_failure_callback=on_etl_failure,
    dag=dag
)

gold_transformation = PythonOperator(
    task_id='gold_transformation',
    python_callable=run_gold_transformation,
    on_success_callback=on_etl_success,
    on_failure_callback=on_etl_failure,
    dag=dag
)

snowflake_publish = PythonOperator(
    task_id='snowflake_publish',
    python_callable=publish_to_snowflake,
    on_success_callback=on_etl_success,
    on_failure_callback=on_etl_failure,
    dag=dag
)

# Set dependencies
bronze_ingestion >> silver_transformation >> gold_transformation >> snowflake_publish

