"""Publish Gold tables to Snowflake using MERGE on customer_id.

Reads Delta from S3 and upserts into ANALYTICS.CUSTOMER_360.
"""

import argparse
import logging
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.secrets import get_snowflake_credentials
from pyspark_interview_project.monitoring.lineage_emitter import emit_start, emit_complete, emit_fail
from pyspark_interview_project.monitoring.metrics_collector import emit_metrics
import time
import uuid

logger = logging.getLogger(__name__)


def load_customer_360_to_snowflake(spark: SparkSession, config: Dict[str, Any]) -> bool:
    """Load customer_360 Gold table to Snowflake with MERGE on customer_id."""
    run_id = str(uuid.uuid4())
    start_time = time.time()
    
    gold_path = config.get("data_lake", {}).get("gold_path", "s3a://my-etl-lake-demo/gold")
    gold_input = f"{gold_path}/customer_360"
    snowflake_output = "snowflake://ANALYTICS/PUBLIC/CUSTOMER_360"
    
    # Emit lineage START
    emit_start(
        job_name="publish_gold_to_snowflake",
        inputs=[{"name": gold_input, "namespace": "s3"}],
        outputs=[{"name": snowflake_output, "namespace": "snowflake"}],
        config=config,
        run_id=run_id
    )
    
    df = spark.read.format("delta").load(gold_input)
    df = df.withColumn("_load_ts", current_timestamp())

    creds = get_snowflake_credentials(config)
    
    # Build Snowflake connection options
    account = creds.get('account', '').replace('.snowflakecomputing.com', '')
    sf_options = {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": creds.get("user"),
        "sfPassword": creds.get("password"),
        "sfDatabase": creds.get("database", "ANALYTICS"),
        "sfSchema": creds.get("schema", "PUBLIC"),
        "sfWarehouse": creds.get("warehouse"),
    }

    staging_table_name = "CUSTOMER_360_STAGING"
    target_table_name = "CUSTOMER_360"
    schema = sf_options["sfSchema"]
    
    logger.info(f"Writing {df.count():,} rows to Snowflake staging table: {schema}.{staging_table_name}")
    
    # Write to staging table (overwrite)
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", staging_table_name) \
        .mode("overwrite") \
        .save()
    
    logger.info("Staging table written successfully")

    # Build MERGE SQL statement
    columns = df.columns
    update_cols = ", ".join([f"t.{c} = s.{c}" for c in columns if c != "customer_id"])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{c}" for c in columns])

    merge_sql = f"""
        MERGE INTO {schema}.{target_table_name} t
        USING {schema}.{staging_table_name} s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """.strip()

    logger.info(f"Executing MERGE SQL for {target_table_name}")
    
    # Execute MERGE via JDBC (Snowflake connector doesn't support direct SQL execution)
    # Build JDBC URL
    jdbc_url = (
        f"jdbc:snowflake://{sf_options['sfURL']}/?"
        f"db={sf_options['sfDatabase']}&"
        f"schema={sf_options['sfSchema']}&"
        f"warehouse={sf_options['sfWarehouse']}"
    )
    
    try:
        # Execute MERGE using JDBC
        spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", sf_options["sfUser"]) \
            .option("password", sf_options["sfPassword"]) \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("query", merge_sql) \
            .load() \
            .collect()
        
        logger.info(f"✅ MERGE to Snowflake {schema}.{target_table_name} completed")
        
        # Clean up staging table
        drop_sql = f"DROP TABLE IF EXISTS {schema}.{staging_table_name}"
        spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", sf_options["sfUser"]) \
            .option("password", sf_options["sfPassword"]) \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("query", drop_sql) \
            .load() \
            .collect()
        logger.info(f"Dropped staging table: {staging_table_name}")
        
        # Emit metrics
        duration = time.time() - start_time
        rows_out = df.count()
        emit_metrics(
            job_name="publish_gold_to_snowflake",
            rows_in=rows_out,
            rows_out=rows_out,
            duration_seconds=duration,
            dq_status="pass",
            config=config
        )
        
        # Emit lineage COMPLETE
        emit_complete(
            job_name="publish_gold_to_snowflake",
            inputs=[{"name": gold_input, "namespace": "s3"}],
            outputs=[{"name": snowflake_output, "namespace": "snowflake"}],
            config=config,
            run_id=run_id,
            metadata={"rows_out": rows_out, "duration_seconds": duration}
        )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to execute MERGE: {e}")
        logger.warning(f"Staging table {staging_table_name} may need manual cleanup")
        
        # Emit lineage FAIL
        emit_fail(
            job_name="publish_gold_to_snowflake",
            inputs=[{"name": gold_input, "namespace": "s3"}],
            outputs=[{"name": snowflake_output, "namespace": "snowflake"}],
            config=config,
            run_id=run_id,
            error=str(e)
        )
        raise


def main():
    parser = argparse.ArgumentParser(description="Publish Gold to Snowflake")
    parser.add_argument("--config", default="config/prod.yaml")
    args = parser.parse_args()

    config = load_conf(args.config)
    spark = build_spark(app_name="publish_gold_to_snowflake", config=config)
    try:
        load_customer_360_to_snowflake(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


