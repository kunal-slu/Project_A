#!/usr/bin/env python3
"""
Bronze to Silver Transformation
- Canonicalize schemas
- Deduplicate on natural keys
- Multi-source join (Snowflake + Redshift)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import yaml

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.transform.bronze_to_silver_multi_source import (
    canonicalize_customers,
    canonicalize_orders,
    join_multi_source
)


def main():
    """Main entry point."""
    # Load config
    config_path = Path("config/dev.yaml")
    if not config_path.exists():
        config_path = Path("config/prod.yaml")
    config = load_config_resolved(str(config_path))
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        lake_bucket = config["buckets"]["lake"]
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Read Bronze tables
        bronze_orders = spark.read.format("delta").load(f"s3a://{lake_bucket}/bronze/snowflake/orders/")
        bronze_customers = spark.read.format("delta").load(f"s3a://{lake_bucket}/bronze/snowflake/customers/")
        bronze_behavior = spark.read.format("delta").load(f"s3a://{lake_bucket}/bronze/redshift/behavior/")
        
        # Canonicalize customers
        silver_customers = canonicalize_customers(bronze_customers) \
            .dropDuplicates(["customer_id"]) \
            .withColumn("effective_ts", F.current_timestamp())
        
        # Canonicalize orders
        silver_orders = canonicalize_orders(bronze_orders) \
            .withColumn("order_date", F.to_date("event_ts")) \
            .dropDuplicates(["order_id"])
        
        # Join multi-source (orders + behavior)
        silver_customer_activity = join_multi_source(
            silver_orders,
            bronze_behavior,
            join_keys=["customer_id", "event_date"]
        )
        
        # Write Silver tables (Delta, partitioned)
        silver_customers.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country") \
            .save(f"s3a://{lake_bucket}/silver/customers/")
        
        silver_orders.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("order_date") \
            .save(f"s3a://{lake_bucket}/silver/orders/")
        
        silver_customer_activity.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("event_date") \
            .save(f"s3a://{lake_bucket}/silver/customer_activity/")
        
        print("✅ Bronze to Silver transformation completed")
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

