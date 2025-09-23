"""
Bronze to Silver transformation with data cleaning and schema alignment.
"""

import logging
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, when, to_date, regexp_replace, upper

logger = logging.getLogger(__name__)


def transform_customers_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Transform customers from bronze to silver layer.
    
    Args:
        spark: Spark session
        bronze_df: Bronze customers DataFrame
        
    Returns:
        Cleaned silver customers DataFrame
    """
    logger.info("Transforming customers from bronze to silver")
    
    # Clean and standardize data
    silver_df = bronze_df.select(
        col("customer_id").cast("string").alias("customer_id"),
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        trim(col("email")).alias("email"),
        trim(col("address")).alias("address"),
        trim(col("city")).alias("city"),
        trim(col("state")).alias("state"),
        trim(col("country")).alias("country"),
        trim(col("zip")).alias("zip"),
        trim(col("phone")).alias("phone"),
        to_date(col("registration_date"), "MM/dd/yy").alias("registration_date"),
        upper(trim(col("gender"))).alias("gender"),
        col("age").cast("int").alias("age")
    ).filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull() &
        col("age").between(18, 99)
    )
    
    # Remove duplicates on business key
    silver_df = silver_df.dropDuplicates(["customer_id"])
    
    logger.info("Customers bronze to silver transformation completed")
    return silver_df


def transform_orders_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Transform orders from bronze to silver layer.
    
    Args:
        spark: Spark session
        bronze_df: Bronze orders DataFrame
        
    Returns:
        Cleaned silver orders DataFrame
    """
    logger.info("Transforming orders from bronze to silver")
    
    # Check if we have nested payment structure or flat structure
    columns = bronze_df.columns
    if "payment" in columns and "payment_method" not in columns:
        # Nested structure (from JSON) - payment is a struct
        silver_df = bronze_df.select(
            col("order_id").cast("string").alias("order_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("product_id").cast("string").alias("product_id"),
            col("quantity").cast("int").alias("quantity"),
            # Calculate unit_price from total_amount / quantity
            (col("total_amount") / col("quantity")).cast("double").alias("unit_price"),
            col("total_amount").cast("double").alias("total_amount"),
            to_date(col("order_date"), "yyyy-MM-dd'T'HH:mm:ss").alias("order_date"),
            # Extract payment method from struct
            col("payment.method").alias("payment_method"),
            # Extract payment status as order status
            col("payment.status").alias("status")
        )
    else:
        # Flat structure (for tests)
        silver_df = bronze_df.select(
            col("order_id").cast("string").alias("order_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("product_id").cast("string").alias("product_id"),
            col("quantity").cast("int").alias("quantity"),
            col("unit_price").cast("double").alias("unit_price"),
            col("total_amount").cast("double").alias("total_amount"),
            to_date(col("order_date"), "yyyy-MM-dd'T'HH:mm:ss").alias("order_date"),
            col("payment_method").cast("string").alias("payment_method"),
            col("status").cast("string").alias("status")
        )
    
    silver_df = silver_df.filter(
        (col("order_id").isNotNull()) &
        (col("customer_id").isNotNull()) &
        (col("total_amount") >= 0)
    )
    
    # Remove duplicates on business key
    silver_df = silver_df.dropDuplicates(["order_id"])
    
    logger.info("Orders bronze to silver transformation completed")
    return silver_df


def transform_products_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Transform products from bronze to silver layer.
    
    Args:
        spark: Spark session
        bronze_df: Bronze products DataFrame
        
    Returns:
        Cleaned silver products DataFrame
    """
    logger.info("Transforming products from bronze to silver")
    
    # Check if we have in_stock column or stock_quantity column
    columns = bronze_df.columns
    if "in_stock" in columns:
        # CSV schema with in_stock boolean
        silver_df = bronze_df.select(
            col("product_id").cast("string").alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("brand")).alias("brand"),
            col("price").cast("double").alias("price"),
            # Convert in_stock boolean to stock_quantity integer
            when(col("in_stock") == "true", 100).otherwise(0).cast("int").alias("stock_quantity")
        )
    else:
        # Test schema with stock_quantity already present
        silver_df = bronze_df.select(
            col("product_id").cast("string").alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("brand")).alias("brand"),
            col("price").cast("double").alias("price"),
            col("stock_quantity").cast("int").alias("stock_quantity")
        )
    
    silver_df = silver_df.filter(
        (col("product_id").isNotNull()) &
        (col("product_name").isNotNull()) &
        (col("price") >= 0)
    )
    
    # Remove duplicates on business key
    silver_df = silver_df.dropDuplicates(["product_id"])
    
    logger.info("Products bronze to silver transformation completed")
    return silver_df