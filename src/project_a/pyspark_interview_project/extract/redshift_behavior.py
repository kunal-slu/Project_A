"""Extract Redshift customer behavior data with CDC/incremental support."""

import argparse
import logging
import time
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from project_a.monitoring.lineage_decorator import lineage_job
from project_a.monitoring.metrics_collector import emit_duration, emit_rowcount
from project_a.utils.watermark_utils import (
    get_latest_timestamp_from_df,
    get_watermark,
    upsert_watermark,
)

logger = logging.getLogger(__name__)


@lineage_job(
    name="extract_redshift_behavior",
    inputs=["redshift://customer_behavior"],
    outputs=["s3://bucket/bronze/redshift/customer_behavior"],
)
def extract_redshift_behavior(
    spark: SparkSession, config: dict[str, Any], since_ts: datetime | None = None, **kwargs
) -> DataFrame:
    """
    Extract customer behavior from Redshift source with incremental support.

    Args:
        spark: SparkSession object
        config: Configuration dictionary
        since_ts: Optional timestamp to filter records (if None, uses watermark)
        **kwargs: Additional arguments

    Returns:
        DataFrame with Redshift customer behavior data
    """
    logger.info("Extracting Redshift customer behavior data")
    start_time = time.time()

    # Get watermark if not provided
    if since_ts is None:
        watermark = get_watermark("redshift_behavior", config, spark)
        if watermark:
            since_ts = watermark
            logger.info(f"Using watermark: {since_ts.isoformat()}")
        else:
            logger.info("No watermark found, performing full load")

    try:
        # For local dev, use sample data
        if config.get("environment") == "local":
            sample_path = config.get("paths", {}).get(
                "redshift_behavior", "data/samples/redshift/redshift_customer_behavior_50000.csv"
            )
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
            # Filter by timestamp if watermark exists
            if since_ts and "event_ts" in df.columns:
                df = df.filter(col("event_ts") >= lit(since_ts))
        else:
            # In AWS, use Redshift JDBC connection with incremental query
            redshift_config = config.get("data_sources", {}).get("redshift", {})

            # Build query with watermark filter
            table_name = redshift_config.get("table", "customer_behavior")
            if since_ts:
                query = f"""
                    SELECT * FROM {table_name}
                    WHERE event_ts > '{since_ts.isoformat()}'
                    ORDER BY event_ts
                """
                logger.info(f"Incremental load: WHERE event_ts > {since_ts.isoformat()}")
            else:
                query = f"SELECT * FROM {table_name} ORDER BY event_ts"
                logger.info("Full load: no watermark filter")

            df = (
                spark.read.format("jdbc")
                .option("url", redshift_config.get("url"))
                .option("query", query)
                .option("user", redshift_config.get("user"))
                .option("password", redshift_config.get("password"))
                .load()
            )

        # Add metadata columns
        df = (
            df.withColumn("record_source", lit("redshift"))
            .withColumn("record_table", lit("customer_behavior"))
            .withColumn("_ingestion_ts", current_timestamp())
        )

        record_count = df.count()
        duration_ms = (time.time() - start_time) * 1000

        logger.info(f"Successfully extracted {record_count:,} Redshift behavior records")

        # Emit metrics
        emit_rowcount(
            "records_extracted",
            record_count,
            {"source": "redshift", "table": "customer_behavior"},
            config,
        )
        emit_duration("extraction_duration", duration_ms, {"source": "redshift"}, config)

        # Update watermark if records were loaded
        if record_count > 0:
            latest_ts = get_latest_timestamp_from_df(df, timestamp_col="event_ts")
            if latest_ts:
                upsert_watermark("redshift_behavior", latest_ts, config, spark)

        return df

    except Exception as e:
        logger.error(f"Failed to extract Redshift behavior: {e}")
        raise


if __name__ == "__main__":
    # CLI entry point for incremental loads
    parser = argparse.ArgumentParser(description="Extract Redshift behavior")
    parser.add_argument("--since-ts", type=str, help="ISO timestamp for incremental load")
    parser.add_argument("--config", default="config/local.yaml", help="Config file path")
    args = parser.parse_args()

    since_ts = None
    if args.since_ts:
        since_ts = datetime.fromisoformat(args.since_ts.replace("Z", "+00:00"))

    # Run extraction (would need SparkSession setup)
