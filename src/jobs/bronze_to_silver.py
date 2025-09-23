"""
Bronze to Silver ETL job for EMR Serverless.
Reads raw data from S3 bronze layer and transforms to silver layer.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, upper, when, current_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.io import read_delta, write_delta
from pyspark_interview_project.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def transform_customers_bronze_to_silver(spark: SparkSession, bronze_df, config: Dict[str, Any]):
    """Transform customers from bronze to silver layer."""
    logger.info("Starting customers bronze to silver transformation")
    
    # Clean and standardize data
    silver_df = bronze_df.select(
        col("customer_id").cast(StringType()).alias("customer_id"),
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        trim(col("email")).alias("email"),
        trim(col("address")).alias("address"),
        trim(col("city")).alias("city"),
        trim(col("state")).alias("state"),
        trim(col("country")).alias("country"),
        trim(col("zip")).alias("zip_code"),
        trim(col("phone")).alias("phone_number"),
        to_date(col("registration_date"), "MM/dd/yy").alias("registration_date"),
        upper(trim(col("gender"))).alias("gender"),
        col("age").cast(IntegerType()).alias("age"),
        current_timestamp().alias("processed_at")
    ).filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull() &
        col("age").between(18, 99)
    )
    
    # Remove duplicates on business key
    silver_df = silver_df.dropDuplicates(["customer_id"])
    
    logger.info(f"Customers transformation completed. Rows: {silver_df.count()}")
    return silver_df


def transform_orders_bronze_to_silver(spark: SparkSession, bronze_df, config: Dict[str, Any]):
    """Transform orders from bronze to silver layer."""
    logger.info("Starting orders bronze to silver transformation")
    
    # Handle both nested and flat structures
    columns = bronze_df.columns
    if "payment.method" in columns:
        # Nested structure (from JSON)
        silver_df = bronze_df.select(
            col("order_id").cast(StringType()).alias("order_id"),
            col("customer_id").cast(StringType()).alias("customer_id"),
            col("product_id").cast(StringType()).alias("product_id"),
            col("quantity").cast(IntegerType()).alias("quantity"),
            (col("total_amount") / col("quantity")).cast(DoubleType()).alias("unit_price"),
            col("total_amount").cast(DoubleType()).alias("total_amount"),
            to_date(col("order_date"), "yyyy-MM-dd'T'HH:mm:ss").alias("order_date"),
            col("payment.method").alias("payment_method"),
            col("payment.status").alias("status"),
            current_timestamp().alias("processed_at")
        )
    else:
        # Flat structure
        silver_df = bronze_df.select(
            col("order_id").cast(StringType()).alias("order_id"),
            col("customer_id").cast(StringType()).alias("customer_id"),
            col("product_id").cast(StringType()).alias("product_id"),
            col("quantity").cast(IntegerType()).alias("quantity"),
            col("unit_price").cast(DoubleType()).alias("unit_price"),
            col("total_amount").cast(DoubleType()).alias("total_amount"),
            to_date(col("order_date"), "yyyy-MM-dd'T'HH:mm:ss").alias("order_date"),
            col("payment_method").cast(StringType()).alias("payment_method"),
            col("status").cast(StringType()).alias("status"),
            current_timestamp().alias("processed_at")
        )
    
    silver_df = silver_df.filter(
        (col("order_id").isNotNull()) &
        (col("customer_id").isNotNull()) &
        (col("total_amount") >= 0)
    )
    
    # Remove duplicates on business key
    silver_df = silver_df.dropDuplicates(["order_id"])
    
    logger.info(f"Orders transformation completed. Rows: {silver_df.count()}")
    return silver_df


def transform_products_bronze_to_silver(spark: SparkSession, bronze_df, config: Dict[str, Any]):
    """Transform products from bronze to silver layer."""
    logger.info("Starting products bronze to silver transformation")
    
    # Handle different schemas
    columns = bronze_df.columns
    if "in_stock" in columns:
        # CSV schema with in_stock boolean
        silver_df = bronze_df.select(
            col("product_id").cast(StringType()).alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("brand")).alias("brand"),
            col("price").cast(DoubleType()).alias("price"),
            when(col("in_stock") == "true", 100).otherwise(0).cast(IntegerType()).alias("stock_quantity"),
            current_timestamp().alias("processed_at")
        )
    else:
        # Test schema with stock_quantity already present
        silver_df = bronze_df.select(
            col("product_id").cast(StringType()).alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("brand")).alias("brand"),
            col("price").cast(DoubleType()).alias("price"),
            col("stock_quantity").cast(IntegerType()).alias("stock_quantity"),
            current_timestamp().alias("processed_at")
        )
    
    silver_df = silver_df.filter(
        (col("product_id").isNotNull()) &
        (col("product_name").isNotNull()) &
        (col("price") >= 0)
    )
    
    # Remove duplicates on business key
    silver_df = silver_df.dropDuplicates(["product_id"])
    
    logger.info(f"Products transformation completed. Rows: {silver_df.count()}")
    return silver_df


def main():
    """Main entry point for bronze to silver ETL job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Bronze to Silver ETL Job")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--table", required=True, help="Table to process (customers/orders/products)")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger.info(f"Starting bronze to silver ETL for {args.table} on {args.date}")
    
    try:
        # Load configuration
        config = load_conf(args.config)
        
        # Create Spark session
        spark = get_spark_session(
            app_name=f"bronze-to-silver-{args.table}",
            extra_conf=config.get("emr", {}).get("spark_config", {})
        )
        
        # Read bronze data
        bronze_path = f"{config['s3']['bronze_path']}/{args.table}/ingestion_date={args.date}"
        logger.info(f"Reading bronze data from: {bronze_path}")
        
        bronze_df = spark.read.format("json").load(bronze_path)
        
        # Transform based on table type
        if args.table == "customers":
            silver_df = transform_customers_bronze_to_silver(spark, bronze_df, config)
        elif args.table == "orders":
            silver_df = transform_orders_bronze_to_silver(spark, bronze_df, config)
        elif args.table == "products":
            silver_df = transform_products_bronze_to_silver(spark, bronze_df, config)
        else:
            raise ValueError(f"Unknown table: {args.table}")
        
        # Write to silver layer
        silver_path = f"{config['s3']['silver_path']}/{args.table}"
        logger.info(f"Writing silver data to: {silver_path}")
        
        write_delta(
            silver_df,
            silver_path,
            mode="overwrite",
            partition_by=["ingestion_date"]
        )
        
        # Register table in Glue catalog
        table_name = f"{config['glue']['databases']['silver']}.{args.table}"
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{silver_path}'
        """)
        
        logger.info(f"Successfully processed {args.table} bronze to silver")
        
    except Exception as e:
        logger.error(f"Bronze to silver ETL failed: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
