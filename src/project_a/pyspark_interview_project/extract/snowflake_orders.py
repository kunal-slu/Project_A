"""Extract Snowflake orders data with CDC/incremental support."""

import argparse
import logging
import time
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from project_a.monitoring.lineage_decorator import lineage_job
from project_a.monitoring.metrics_collector import emit_duration, emit_rowcount
from project_a.utils.secrets import get_snowflake_credentials
from project_a.utils.watermark_utils import (
    get_latest_timestamp_from_df,
    get_watermark,
    upsert_watermark,
)

logger = logging.getLogger(__name__)


@lineage_job(
    name="extract_snowflake_orders",
    inputs=["snowflake://ORDERS"],
    outputs=["s3://bucket/bronze/snowflake/orders"],
)
def extract_snowflake_orders(
    spark: SparkSession, config: dict[str, Any], since_ts: datetime | None = None, **kwargs
) -> DataFrame:
    """
    Extract orders from Snowflake source with incremental support.

    Args:
        spark: SparkSession object
        config: Configuration dictionary
        since_ts: Optional timestamp to filter records (if None, uses watermark)
        **kwargs: Additional arguments

    Returns:
        DataFrame with Snowflake orders data
    """
    logger.info("Extracting Snowflake orders data")
    start_time = time.time()

    # Get watermark if not provided
    if since_ts is None:
        watermark = get_watermark("snowflake_orders", config, spark)
        if watermark:
            since_ts = watermark
            logger.info(f"Using watermark: {since_ts.isoformat()}")
        else:
            logger.info("No watermark found, performing full load")

    try:
        # For local dev, use sample data
        if config.get("environment") == "local":
            sample_path = config.get("paths", {}).get(
                "snowflake_orders", "data/samples/snowflake/snowflake_orders_100000.csv"
            )
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
            # Filter by timestamp if watermark exists (simulate incremental)
            if since_ts and "order_date" in df.columns:
                df = df.filter(col("order_date") >= lit(since_ts))
        else:
            # In AWS, use Snowflake JDBC connection with incremental query
            # Get credentials from Secrets Manager
            snowflake_config = get_snowflake_credentials(config)
            # Build connection options
            account = snowflake_config.get("account", "").replace(".snowflakecomputing.com", "")
            snowflake_options = {
                "sfURL": f"{account}.snowflakecomputing.com",
                "sfUser": snowflake_config.get("user"),
                "sfPassword": snowflake_config.get("password"),
                "sfDatabase": snowflake_config.get("database", "ANALYTICS"),
                "sfSchema": snowflake_config.get("schema", "PUBLIC"),
                "sfWarehouse": snowflake_config.get("warehouse"),
            }

            # Build query with watermark filter
            if since_ts:
                query = f"""
                    SELECT * FROM ORDERS
                    WHERE LAST_MODIFIED_TS > '{since_ts.isoformat()}'
                    ORDER BY LAST_MODIFIED_TS
                """
                logger.info(f"Incremental load: WHERE LAST_MODIFIED_TS > {since_ts.isoformat()}")
            else:
                query = "SELECT * FROM ORDERS ORDER BY LAST_MODIFIED_TS"
                logger.info("Full load: no watermark filter")

            df = (
                spark.read.format("snowflake")
                .options(**snowflake_options)
                .option("query", query)
                .load()
            )

        # Add metadata columns
        df = (
            df.withColumn("record_source", lit("snowflake"))
            .withColumn("record_table", lit("orders"))
            .withColumn("_ingestion_ts", current_timestamp())
        )

        record_count = df.count()
        duration_ms = (time.time() - start_time) * 1000

        logger.info(f"Successfully extracted {record_count:,} Snowflake orders")

        # Emit metrics
        emit_rowcount(
            "records_extracted", record_count, {"source": "snowflake", "table": "orders"}, config
        )
        emit_duration("extraction_duration", duration_ms, {"source": "snowflake"}, config)

        # Update watermark if records were loaded
        if record_count > 0:
            latest_ts = get_latest_timestamp_from_df(df, timestamp_col="order_date")
            if latest_ts:
                upsert_watermark("snowflake_orders", latest_ts, config, spark)

        return df

    except Exception as e:
        logger.error(f"Failed to extract Snowflake orders: {e}")
        raise


if __name__ == "__main__":
    # CLI entry point for incremental loads
    parser = argparse.ArgumentParser(description="Extract Snowflake orders")
    parser.add_argument(
        "--since-ts",
        type=str,
        help="ISO timestamp for incremental load (e.g., 2024-01-15T00:00:00Z)",
    )
    parser.add_argument("--config", default="config/local.yaml", help="Config file path")
    args = parser.parse_args()

    since_ts = None
    if args.since_ts:
        since_ts = datetime.fromisoformat(args.since_ts.replace("Z", "+00:00"))

    # Run extraction (would need SparkSession setup)
    # extract_snowflake_orders(spark, config, since_ts=since_ts)
