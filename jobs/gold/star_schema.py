#!/usr/bin/env python3
"""
Gold Layer: Star Schema Builder
Creates fact_sales, dim_product, dim_date with surrogate keys
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
        
        # Read Silver
        silver_orders = spark.read.format("delta").load(f"s3a://{lake_bucket}/silver/orders/")
        silver_customers = spark.read.format("delta").load(f"s3a://{lake_bucket}/silver/customers/")
        
        # Build dim_date
        dim_date = silver_orders \
            .select(F.col("order_date").alias("date")) \
            .distinct() \
            .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int")) \
            .withColumn("year", F.year("date")) \
            .withColumn("quarter", F.quarter("date")) \
            .withColumn("month", F.month("date")) \
            .withColumn("day_of_week", F.dayofweek("date")) \
            .select("date_sk", "date", "year", "quarter", "month", "day_of_week")
        
        # Build dim_product (from orders)
        dim_product = silver_orders \
            .select("product_id", "product_name", "category", "price_usd") \
            .dropDuplicates(["product_id"]) \
            .withColumn("product_sk", F.monotonically_increasing_id())
        
        # Build dim_customer (from silver customers - should already have customer_sk from SCD2)
        if "customer_sk" not in silver_customers.columns:
            silver_customers = silver_customers.withColumn("customer_sk", F.monotonically_increasing_id())
        
        dim_customer = silver_customers.select("customer_sk", "customer_id", "email", "country", "is_current")
        
        # Build fact_sales (with surrogate keys)
        fact_sales = silver_orders \
            .join(silver_customers.select("customer_id", "customer_sk"), "customer_id", "left") \
            .join(dim_product.select("product_id", "product_sk"), "product_id", "left") \
            .join(dim_date.select("date", "date_sk"), 
                  F.to_date(silver_orders["order_date"]) == dim_date["date"], "left") \
            .select(
                F.col("order_id"),
                F.col("customer_sk"),
                F.col("product_sk"),
                F.col("date_sk"),
                F.col("amount_usd").alias("sales_amount"),
                F.col("quantity"),
                F.col("order_date")
            )
        
        # Write Gold tables
        dim_date.write.format("delta").mode("overwrite").save(f"s3a://{lake_bucket}/gold/dim_date/")
        dim_product.write.format("delta").mode("overwrite").save(f"s3a://{lake_bucket}/gold/dim_product/")
        dim_customer.write.format("delta").mode("overwrite").save(f"s3a://{lake_bucket}/gold/dim_customer/")
        fact_sales.write.format("delta").mode("overwrite").partitionBy("order_date").save(f"s3a://{lake_bucket}/gold/fact_sales/")
        
        print("✅ Gold star schema build completed")
        
    except Exception as e:
        print(f"❌ Star schema build failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

