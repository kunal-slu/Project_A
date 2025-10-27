#!/usr/bin/env python3
"""
Snowflake to Bronze Job - EMR Wrapper

Thin wrapper for EMR Serverless execution.
Reads Snowflake orders and writes to S3 Bronze layer.
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.path_resolver import bronze_path
from pyspark_interview_project.config_loader import load_config_resolved

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for EMR job."""
    # Load configuration
    config = load_config_resolved("config/prod.yaml")
    
    # Build Spark session
    spark = build_spark(config)
    
    logger.info("Starting Snowflake to Bronze job")
    
    # Read Snowflake orders (placeholder - would call extract.snowflake_orders)
    # For now, create dummy data
    from pyspark.sql import Row
    
    rows = [
        Row(order_id="ORD001", customer_id="C001", amount=299.99, date="2025-01-15"),
        Row(order_id="ORD002", customer_id="C002", amount=199.50, date="2025-01-16"),
        Row(order_id="ORD003", customer_id="C001", amount=89.99, date="2025-01-17"),
    ]
    
    orders_df = spark.createDataFrame(rows)
    
    # Get bronze path
    lake_bucket = config["lake"]["root"].replace("s3://", "").split("/")[0]
    output_path = bronze_path(lake_bucket, "snowflake", "orders")
    
    logger.info(f"Writing to: {output_path}")
    
    # Write to bronze
    orders_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .save(output_path)
    
    logger.info("Snowflake to Bronze job completed successfully")
    
    spark.stop()


if __name__ == "__main__":
    main()

