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
        
        # Build dim_date - handle missing order_date column
        date_col = None
        for col in ["order_date", "event_ts", "date", "created_at"]:
            if col in silver_orders.columns:
                date_col = col
                break
        
        if date_col:
            dim_date = silver_orders \
                .select(F.col(date_col).alias("date")) \
                .distinct() \
                .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int")) \
                .withColumn("year", F.year("date")) \
                .withColumn("quarter", F.quarter("date")) \
                .withColumn("month", F.month("date")) \
                .withColumn("day_of_week", F.dayofweek("date")) \
                .select("date_sk", "date", "year", "quarter", "month", "day_of_week")
        else:
            # Fallback: create date dimension from current date
            from datetime import date
            today = date.today()
            dim_date = spark.createDataFrame([(int(today.strftime("%Y%m%d")), today, today.year, (today.month-1)//3+1, today.month, today.weekday()+1)], 
                                            ["date_sk", "date", "year", "quarter", "month", "day_of_week"])
        
        # Build dim_product (from orders - handle missing columns)
        product_cols = []
        for col in ["product_id", "product_name", "product_name", "category", "price_usd", "amount_usd"]:
            if col in silver_orders.columns:
                product_cols.append(col)
        
        if not product_cols:
            # Fallback: create minimal product dimension
            dim_product = silver_orders \
                .select("product_id") \
                .distinct() \
                .withColumn("product_name", F.lit("Unknown")) \
                .withColumn("category", F.lit("Unknown")) \
                .withColumn("price_usd", F.lit(0.0)) \
                .withColumn("product_sk", F.monotonically_increasing_id())
        else:
            dim_product = silver_orders \
                .select(*product_cols) \
                .dropDuplicates(["product_id"]) \
                .withColumn("product_sk", F.monotonically_increasing_id())
            
            # Ensure required columns exist
            if "product_name" not in dim_product.columns:
                dim_product = dim_product.withColumn("product_name", F.lit("Unknown"))
            if "category" not in dim_product.columns:
                dim_product = dim_product.withColumn("category", F.lit("Unknown"))
            if "price_usd" not in dim_product.columns:
                if "amount_usd" in dim_product.columns:
                    dim_product = dim_product.withColumn("price_usd", F.col("amount_usd"))
                else:
                    dim_product = dim_product.withColumn("price_usd", F.lit(0.0))
        
        # Build dim_customer (from silver customers - should already have customer_sk from SCD2)
        # If SCD2 hasn't run yet, get from gold dim_customer or create surrogate key
        gold_customer_path = f"s3a://{lake_bucket}/gold/dim_customer/"
        try:
            dim_customer = spark.read.format("delta").load(gold_customer_path)
            print("✅ Using existing dim_customer from Gold layer")
        except Exception:
            # Fallback: use silver customers
            if "customer_sk" not in silver_customers.columns:
                silver_customers = silver_customers.withColumn("customer_sk", F.monotonically_increasing_id())
            
            customer_cols = ["customer_sk", "customer_id"]
            for col in ["email", "country", "is_current"]:
                if col in silver_customers.columns:
                    customer_cols.append(col)
            
            dim_customer = silver_customers.select(*customer_cols)
        
        # Build fact_sales (with surrogate keys)
        # Join with dim_customer to get customer_sk
        orders_with_customer = silver_orders \
            .join(dim_customer.select("customer_id", "customer_sk"), "customer_id", "left")
        
        # Join with dim_product
        orders_with_product = orders_with_customer \
            .join(dim_product.select("product_id", "product_sk"), "product_id", "left")
        
        # Join with dim_date
        order_date_col = None
        for col in ["order_date", "event_ts", "date"]:
            if col in orders_with_product.columns:
                order_date_col = col
                break
        
        if order_date_col:
            fact_sales = orders_with_product \
                .join(dim_date.select("date", "date_sk"), 
                      F.to_date(F.col(order_date_col)) == dim_date["date"], "left") \
                .select(
                    F.col("order_id"),
                    F.coalesce(F.col("customer_sk"), F.lit(-1)).alias("customer_sk"),
                    F.coalesce(F.col("product_sk"), F.lit(-1)).alias("product_sk"),
                    F.coalesce(F.col("date_sk"), F.lit(-1)).alias("date_sk"),
                    F.coalesce(F.col("amount_usd"), F.col("total_amount"), F.lit(0.0)).alias("sales_amount"),
                    F.coalesce(F.col("quantity"), F.lit(1)).alias("quantity"),
                    F.col(order_date_col).alias("order_date")
                )
        else:
            # Fallback if no date column
            fact_sales = orders_with_product \
                .withColumn("date_sk", F.lit(-1)) \
                .select(
                    F.col("order_id"),
                    F.coalesce(F.col("customer_sk"), F.lit(-1)).alias("customer_sk"),
                    F.coalesce(F.col("product_sk"), F.lit(-1)).alias("product_sk"),
                    F.col("date_sk"),
                    F.coalesce(F.col("amount_usd"), F.col("total_amount"), F.lit(0.0)).alias("sales_amount"),
                    F.coalesce(F.col("quantity"), F.lit(1)).alias("quantity"),
                    F.current_timestamp().alias("order_date")
                )
        
        # Write Gold tables
        dim_date.write.format("delta").mode("overwrite").save(f"s3a://{lake_bucket}/gold/dim_date/")
        dim_product.write.format("delta").mode("overwrite").save(f"s3a://{lake_bucket}/gold/dim_product/")
        
        # Only write dim_customer if it doesn't exist (SCD2 job handles it)
        try:
            existing_dim_customer = spark.read.format("delta").load(f"s3a://{lake_bucket}/gold/dim_customer/")
            print("✅ dim_customer already exists (managed by SCD2 job)")
        except Exception:
            dim_customer.write.format("delta").mode("overwrite").save(f"s3a://{lake_bucket}/gold/dim_customer/")
        
        # Partition fact_sales by date
        partition_col = order_date_col if order_date_col else "order_date"
        fact_sales.write.format("delta").mode("overwrite").partitionBy(partition_col).save(f"s3a://{lake_bucket}/gold/fact_sales/")
        
        print("✅ Gold star schema build completed")
        
    except Exception as e:
        print(f"❌ Star schema build failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

