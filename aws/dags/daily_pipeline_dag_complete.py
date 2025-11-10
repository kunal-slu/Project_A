"""
Airflow DAG: Daily ETL Pipeline with Task Boundaries and DQ Gates
Production-ready orchestration for Bronze → Silver → Gold
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
import boto3
import json

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["data-engineering@example.com"]
}


def register_all_glue_tables(**context):
    """Register all Delta tables in Glue Catalog."""
    import boto3
    
    glue = boto3.client("glue", region_name="us-east-1")
    
    tables_to_register = [
        {"database": "pyspark-etl-project_bronze_dev", "table": "snowflake_orders", "path": "s3://my-etl-lake-demo-424570854632/bronze/snowflake/orders/"},
        {"database": "pyspark-etl-project_bronze_dev", "table": "snowflake_customers", "path": "s3://my-etl-lake-demo-424570854632/bronze/snowflake/customers/"},
        {"database": "pyspark-etl-project_silver_dev", "table": "orders", "path": "s3://my-etl-lake-demo-424570854632/silver/orders/"},
        {"database": "pyspark-etl-project_silver_dev", "table": "customers", "path": "s3://my-etl-lake-demo-424570854632/silver/customers/"},
        {"database": "pyspark-etl-project_gold_dev", "table": "fact_sales", "path": "s3://my-etl-lake-demo-424570854632/gold/fact_sales/"},
        {"database": "pyspark-etl-project_gold_dev", "table": "dim_customer", "path": "s3://my-etl-lake-demo-424570854632/gold/dim_customer/"},
    ]
    
    for tbl in tables_to_register:
        try:
            # Check if table exists
            try:
                glue.get_table(DatabaseName=tbl["database"], Name=tbl["table"])
                print(f"✅ Table {tbl['database']}.{tbl['table']} already exists")
            except glue.exceptions.EntityNotFoundException:
                # Create table
                glue.create_table(
                    DatabaseName=tbl["database"],
                    TableInput={
                        "Name": tbl["table"],
                        "StorageDescriptor": {
                            "Columns": [],  # Schema inferred from Delta
                            "Location": tbl["path"],
                            "InputFormat": "org.apache.hadoop.mapred.FileInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveOutputFormat",
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                            }
                        },
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": {
                            "classification": "delta",
                            "typeOfData": "file"
                        }
                    }
                )
                print(f"✅ Registered {tbl['database']}.{tbl['table']}")
        except Exception as e:
            print(f"⚠️  Failed to register {tbl['database']}.{tbl['table']}: {e}")


with DAG(
    "daily_pipeline_complete",
    default_args=default_args,
    description="Daily ETL: Bronze → Silver → Gold with DQ Gates",
    schedule_interval="0 2 * * *",  # 2 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "production", "p0-p6"],
) as dag:
    
    # Extract tasks (parallel)
    with TaskGroup("extract") as extract_group:
        extract_snowflake_orders = EmrServerlessStartJobRunOperator(
            task_id="extract_snowflake_orders",
            application_id="{{ var.value.emr_app_id }}",
            execution_role_arn="{{ var.value.emr_exec_role_arn }}",
            job_driver={
                "sparkSubmit": {
                    "entryPoint": "s3://{{ var.value.artifacts_bucket }}/jobs/ingest/snowflake_to_bronze.py",
                    "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": "s3://{{ var.value.artifacts_bucket }}/emr-logs/"
                    }
                }
            }
        )
        
        extract_snowflake_customers = EmrServerlessStartJobRunOperator(
            task_id="extract_snowflake_customers",
            application_id="{{ var.value.emr_app_id }}",
            execution_role_arn="{{ var.value.emr_exec_role_arn }}",
            job_driver={
                "sparkSubmit": {
                    "entryPoint": "s3://{{ var.value.artifacts_bucket }}/jobs/ingest/snowflake_customers_to_bronze.py",
                    "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": "s3://{{ var.value.artifacts_bucket }}/emr-logs/"
                    }
                }
            }
        )
        
        extract_redshift_behavior = EmrServerlessStartJobRunOperator(
            task_id="extract_redshift_behavior",
            application_id="{{ var.value.emr_app_id }}",
            execution_role_arn="{{ var.value.emr_exec_role_arn }}",
            job_driver={
                "sparkSubmit": {
                    "entryPoint": "s3://{{ var.value.artifacts_bucket }}/jobs/ingest/redshift_to_bronze.py",
                    "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": "s3://{{ var.value.artifacts_bucket }}/emr-logs/"
                    }
                }
            }
        )
    
    # Transform: Bronze → Silver
    bronze_to_silver = EmrServerlessStartJobRunOperator(
        task_id="bronze_to_silver",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_exec_role_arn }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://{{ var.value.artifacts_bucket }}/jobs/transform/bronze_to_silver.py",
                "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://{{ var.value.artifacts_bucket }}/emr-logs/"
                }
            }
        }
    )
    
    # DQ Gate (hard stop on critical failures)
    dq_gate_silver = EmrServerlessStartJobRunOperator(
        task_id="dq_gate_silver",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_exec_role_arn }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://{{ var.value.artifacts_bucket }}/jobs/dq/dq_gate.py",
                "sparkSubmitParameters": "--table orders --layer silver --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
            }
        },
        sla=timedelta(minutes=20),  # SLA: must complete by 3:15 AM UTC
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://{{ var.value.artifacts_bucket }}/emr-logs/"
                }
            }
        }
    )
    
    # Transform: Silver → Gold (SCD2 + Star Schema)
    silver_to_gold_scd2 = EmrServerlessStartJobRunOperator(
        task_id="silver_to_gold_scd2",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_exec_role_arn }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://{{ var.value.artifacts_bucket }}/jobs/gold/dim_customer_scd2.py",
                "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://{{ var.value.artifacts_bucket }}/emr-logs/"
                }
            }
        }
    )
    
    silver_to_gold_star = EmrServerlessStartJobRunOperator(
        task_id="silver_to_gold_star_schema",
        application_id="{{ var.value.emr_app_id }}",
        execution_role_arn="{{ var.value.emr_exec_role_arn }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://{{ var.value.artifacts_bucket }}/jobs/gold/star_schema.py",
                "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://{{ var.value.artifacts_bucket }}/emr-logs/"
                }
            }
        }
    )
    
    # Register Glue tables
    register_glue = PythonOperator(
        task_id="register_glue_tables",
        python_callable=register_all_glue_tables
    )
    
    # Task dependencies
    extract_group >> bronze_to_silver >> dq_gate_silver >> [silver_to_gold_scd2, silver_to_gold_star] >> register_glue
