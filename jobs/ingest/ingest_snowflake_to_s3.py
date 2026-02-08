"""
Ingest Snowflake data to S3 raw zone.

ELT pattern: Extract from Snowflake → Land to S3 raw/ → Later transform to bronze
"""

import argparse
import logging
import time
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date

from project_a.monitoring.lineage_decorator import lineage_job
from project_a.monitoring.metrics_collector import emit_metrics
from project_a.utils.config import load_conf
from project_a.utils.secrets import get_snowflake_credentials
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


@lineage_job(
    name="ingest_snowflake_to_s3_raw",
    inputs=["snowflake://ORDERS"],
    outputs=["s3://my-etl-lake-demo/raw/snowflake/orders"],
)
def ingest_snowflake_to_s3_raw(
    spark: SparkSession,
    config: dict[str, Any],
    table: str = "orders",
    execution_date: str = None,
    **kwargs,
):
    """
    Ingest Snowflake table to S3 raw zone.

    Args:
        spark: SparkSession
        config: Configuration dictionary
        table: Table name to ingest (default: orders)
        execution_date: Execution date (YYYY-MM-DD), defaults to today
    """
    start_time = time.time()

    if execution_date is None:
        execution_date = datetime.now().strftime("%Y-%m-%d")

    # Get Snowflake credentials
    creds = get_snowflake_credentials(config)

    # Build query (full load for raw zone)
    query = f"SELECT * FROM {table.upper()}"

    logger.info(f"Ingesting Snowflake {table} to S3 raw")

    # Read from Snowflake
    if config.get("environment") == "local":
        # Local: use sample CSV
        sample_path = config.get("paths", {}).get(f"snowflake_{table}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
    else:
        # Production: use Snowflake JDBC
        sf_options = {
            "sfURL": f"{creds.get('account')}.snowflakecomputing.com",
            "sfUser": creds.get("user"),
            "sfPassword": creds.get("password"),
            "sfDatabase": creds.get("database", "ANALYTICS"),
            "sfSchema": creds.get("schema", "PUBLIC"),
            "sfWarehouse": creds.get("warehouse"),
        }

        df = spark.read.format("snowflake").options(**sf_options).option("query", query).load()

    # Add load metadata
    df = (
        df.withColumn("_load_date", to_date(lit(execution_date)))
        .withColumn("_load_timestamp", current_timestamp())
        .withColumn("_source", lit("snowflake"))
        .withColumn("_table", lit(table))
    )

    # Write to raw zone
    raw_path = f"s3://my-etl-lake-demo/raw/snowflake/{table}/load_dt={execution_date}/"
    logger.info(f"Writing to raw zone: {raw_path}")

    df.write.mode("overwrite").parquet(raw_path)

    record_count = df.count()
    duration = time.time() - start_time

    # Emit metrics
    emit_metrics(
        job_name=f"ingest_snowflake_{table}",
        rows_in=record_count,
        rows_out=record_count,
        duration_seconds=duration,
        dq_status="pass",
        config=config,
    )

    logger.info(f"✅ Ingested {record_count:,} records to raw zone")

    return df


def main():
    parser = argparse.ArgumentParser(description="Ingest Snowflake to S3 raw")
    parser.add_argument("--table", default="orders", help="Table name")
    parser.add_argument("--execution-date", help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--config", default="config/prod.yaml", help="Config file")

    args = parser.parse_args()

    config = load_conf(args.config)
    spark = build_spark(app_name=f"ingest_snowflake_{args.table}", config=config)

    try:
        ingest_snowflake_to_s3_raw(
            spark, config, table=args.table, execution_date=args.execution_date
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
