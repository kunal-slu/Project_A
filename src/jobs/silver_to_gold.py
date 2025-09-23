"""
Silver to Gold ETL job for EMR Serverless.
Reads cleaned data from S3 silver layer and creates business-ready gold layer.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.io import read_delta, write_delta
from pyspark_interview_project.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def create_dim_customers(spark: SparkSession, silver_customers_df, config: Dict[str, Any]):
    """Create customer dimension table from silver data."""
    logger.info("Creating customer dimension table")
    
    dim_customers_df = silver_customers_df.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("address"),
        col("city"),
        col("state"),
        col("country"),
        col("zip_code"),
        col("phone_number"),
        col("registration_date"),
        col("gender"),
        col("age"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    ).dropDuplicates(["customer_id"])
    
    logger.info(f"Customer dimension created. Rows: {dim_customers_df.count()}")
    return dim_customers_df


def create_dim_products(spark: SparkSession, silver_products_df, config: Dict[str, Any]):
    """Create product dimension table from silver data."""
    logger.info("Creating product dimension table")
    
    dim_products_df = silver_products_df.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("brand"),
        col("price"),
        col("stock_quantity"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    ).dropDuplicates(["product_id"])
    
    logger.info(f"Product dimension created. Rows: {dim_products_df.count()}")
    return dim_products_df


def create_fact_orders(spark: SparkSession, silver_orders_df, silver_products_df, config: Dict[str, Any]):
    """Create orders fact table by joining with product dimension."""
    logger.info("Creating orders fact table")
    
    # Join orders with products to get product details at time of order
    fact_orders_df = silver_orders_df.alias("o").join(
        silver_products_df.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "left"
    ).select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product_id"),
        col("o.order_date"),
        col("o.quantity"),
        col("o.unit_price"),
        col("o.total_amount"),
        col("o.payment_method"),
        col("o.status"),
        col("p.product_name"),
        col("p.category"),
        col("p.brand"),
        current_timestamp().alias("created_at")
    ).dropDuplicates(["order_id"])
    
    logger.info(f"Orders fact table created. Rows: {fact_orders_df.count()}")
    return fact_orders_df


def create_aggregated_metrics(spark: SparkSession, fact_orders_df, config: Dict[str, Any]):
    """Create aggregated metrics table for analytics."""
    logger.info("Creating aggregated metrics table")
    
    # Daily sales metrics
    daily_metrics = fact_orders_df.groupBy(
        col("order_date").cast("date").alias("order_date"),
        col("category"),
        col("payment_method")
    ).agg(
        {"total_amount": "sum", "quantity": "sum", "order_id": "count"}
    ).select(
        col("order_date"),
        col("category"),
        col("payment_method"),
        col("sum(total_amount)").alias("total_sales"),
        col("sum(quantity)").alias("total_quantity"),
        col("count(order_id)").alias("order_count"),
        current_timestamp().alias("created_at")
    )
    
    logger.info(f"Daily metrics created. Rows: {daily_metrics.count()}")
    return daily_metrics


def main():
    """Main entry point for silver to gold ETL job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Silver to Gold ETL Job")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger.info(f"Starting silver to gold ETL on {args.date}")
    
    try:
        # Load configuration
        config = load_conf(args.config)
        
        # Create Spark session
        spark = get_spark_session(
            app_name="silver-to-gold",
            extra_conf=config.get("emr", {}).get("spark_config", {})
        )
        
        # Read silver data
        silver_customers_df = read_delta(spark, f"{config['s3']['silver_path']}/customers")
        silver_orders_df = read_delta(spark, f"{config['s3']['silver_path']}/orders")
        silver_products_df = read_delta(spark, f"{config['s3']['silver_path']}/products")
        
        # Create gold layer tables
        dim_customers = create_dim_customers(spark, silver_customers_df, config)
        dim_products = create_dim_products(spark, silver_products_df, config)
        fact_orders = create_fact_orders(spark, silver_orders_df, silver_products_df, config)
        daily_metrics = create_aggregated_metrics(spark, fact_orders, config)
        
        # Write gold layer tables
        gold_tables = {
            "dim_customers": dim_customers,
            "dim_products": dim_products,
            "fact_orders": fact_orders,
            "daily_metrics": daily_metrics
        }
        
        for table_name, table_df in gold_tables.items():
            gold_path = f"{config['s3']['gold_path']}/{table_name}"
            logger.info(f"Writing {table_name} to: {gold_path}")
            
            write_delta(
                table_df,
                gold_path,
                mode="overwrite"
            )
            
            # Register table in Glue catalog
            glue_table_name = f"{config['glue']['databases']['gold']}.{table_name}"
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {glue_table_name}
                USING DELTA
                LOCATION '{gold_path}'
            """)
            
            logger.info(f"Successfully created gold table: {table_name}")
        
        logger.info("Silver to gold ETL completed successfully")
        
    except Exception as e:
        logger.error(f"Silver to gold ETL failed: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
