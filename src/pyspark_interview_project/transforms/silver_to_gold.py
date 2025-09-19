"""
Silver to Gold transformation creating curated dimensions and facts.
"""

import logging
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

logger = logging.getLogger(__name__)


def create_dim_customers(spark: SparkSession, customers_silver: DataFrame) -> DataFrame:
    """
    Create customer dimension table.
    
    Args:
        spark: Spark session
        customers_silver: Silver customers DataFrame
        
    Returns:
        Customer dimension DataFrame
    """
    logger.info("Creating customer dimension table")
    
    dim_customers = customers_silver.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("address"),
        col("city"),
        col("state"),
        col("country"),
        col("zip"),
        col("phone"),
        col("registration_date"),
        col("gender"),
        col("age"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )
    
    logger.info(f"Customer dimension created. Rows: {dim_customers.count()}")
    return dim_customers


def create_dim_products(spark: SparkSession, products_silver: DataFrame) -> DataFrame:
    """
    Create product dimension table.
    
    Args:
        spark: Spark session
        products_silver: Silver products DataFrame
        
    Returns:
        Product dimension DataFrame
    """
    logger.info("Creating product dimension table")
    
    dim_products = products_silver.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("brand"),
        col("price"),
        col("stock_quantity"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )
    
    logger.info(f"Product dimension created. Rows: {dim_products.count()}")
    return dim_products


def create_fact_orders(
    spark: SparkSession, 
    orders_silver: DataFrame, 
    customers_silver: DataFrame,
    products_silver: DataFrame
) -> DataFrame:
    """
    Create orders fact table with enriched data.
    
    Args:
        spark: Spark session
        orders_silver: Silver orders DataFrame
        customers_silver: Silver customers DataFrame
        products_silver: Silver products DataFrame
        
    Returns:
        Orders fact DataFrame
    """
    logger.info("Creating orders fact table")
    
    # Join with dimensions to enrich fact table
    fact_orders = orders_silver.join(
        customers_silver.select("customer_id", "age", "gender", "country"),
        "customer_id",
        "left"
    ).join(
        products_silver.select("product_id", "category", "brand"),
        "product_id", 
        "left"
    ).select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("total_amount"),
        col("order_date"),
        col("status"),
        col("payment_method"),
        col("age").alias("customer_age"),
        col("gender").alias("customer_gender"),
        col("country").alias("customer_country"),
        col("category").alias("product_category"),
        col("brand").alias("product_brand"),
        current_timestamp().alias("created_at")
    )
    
    logger.info(f"Orders fact table created. Rows: {fact_orders.count()}")
    return fact_orders
