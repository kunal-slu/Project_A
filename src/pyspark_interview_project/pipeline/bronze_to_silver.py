"""
Bronze to Silver transformation with data cleaning and schema alignment.
"""

import logging
import time
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, when, to_date, regexp_replace, upper
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
from pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration
from pyspark_interview_project.utils.schema_validator import validate_schema

logger = logging.getLogger(__name__)


def bronze_to_silver(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, int]:
    """
    Main bronze to silver transformation function.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
        
    Returns:
        Dictionary with record counts by table
    """
    logger.info("Starting Bronze to Silver transformation")
    bronze_path = config.get("data_lake", {}).get("bronze_path", "data/lakehouse_delta/bronze")
    silver_path = config.get("data_lake", {}).get("silver_path", "data/lakehouse_delta/silver")
    
    counts = {}
    
    try:
        # Transform customers
        customers_bronze = spark.read.format("delta").load(f"{bronze_path}/customers")
        customers_silver = transform_customers_bronze_to_silver(spark, customers_bronze, config)
        customers_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
        counts["customers"] = customers_silver.count()
        
        # Transform orders
        orders_bronze = spark.read.format("delta").load(f"{bronze_path}/orders")
        orders_silver = transform_orders_bronze_to_silver(spark, orders_bronze, config)
        orders_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/orders")
        counts["orders"] = orders_silver.count()
        
        # Transform products
        products_bronze = spark.read.format("delta").load(f"{bronze_path}/products")
        products_silver = transform_products_bronze_to_silver(spark, products_bronze, config)
        products_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/products")
        counts["products"] = products_silver.count()
        
        logger.info(f"✅ Bronze to Silver completed: {counts}")
        return counts
        
    except Exception as e:
        logger.error(f"❌ Bronze to Silver failed: {e}")
        raise


@lineage_job(
    name="bronze_to_silver_customers",
    inputs=["s3://bucket/bronze/customers"],
    outputs=["s3://bucket/silver/customers"]
)
def transform_customers_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame, config: Dict[str, Any] = None) -> DataFrame:
    """
    Transform customers from bronze to silver layer.
    
    Args:
        spark: Spark session
        bronze_df: Bronze customers DataFrame
        config: Configuration dictionary
        
    Returns:
        Cleaned silver customers DataFrame
    """
    logger.info("Transforming customers from bronze to silver")
    start_time = time.time()
    
    # Schema validation (if config provided)
    if config:
        try:
            schema_mode = config.get("schema", {}).get("validation_mode", "allow_new")
            bronze_df, validation_result = validate_schema(
                bronze_df,
                expected_schema={"table_name": "customers", "columns": []},
                mode=schema_mode,
                config=config,
                spark=spark
            )
        except Exception as e:
            logger.warning(f"Schema validation warning: {e}")
    
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
    
    record_count = silver_df.count()
    duration_ms = (time.time() - start_time) * 1000
    
    # Emit metrics
    if config:
        emit_rowcount("records_silver", record_count, {"table": "customers", "layer": "silver"}, config)
        emit_duration("transformation_duration", duration_ms, {"stage": "bronze_to_silver", "table": "customers"}, config)
    
    logger.info(f"Customers bronze to silver transformation completed: {record_count:,} records in {duration_ms:.0f}ms")
    return silver_df


@lineage_job(
    name="bronze_to_silver_orders",
    inputs=["s3://bucket/bronze/orders"],
    outputs=["s3://bucket/silver/orders"]
)
def transform_orders_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame, config: Dict[str, Any] = None) -> DataFrame:
    """
    Transform orders from bronze to silver layer.
    
    Args:
        spark: Spark session
        bronze_df: Bronze orders DataFrame
        config: Configuration dictionary
        
    Returns:
        Cleaned silver orders DataFrame
    """
    logger.info("Transforming orders from bronze to silver")
    start_time = time.time()
    
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
    
    record_count = silver_df.count()
    duration_ms = (time.time() - start_time) * 1000
    
    # Emit metrics
    if config:
        emit_rowcount("records_silver", record_count, {"table": "orders", "layer": "silver"}, config)
        emit_duration("transformation_duration", duration_ms, {"stage": "bronze_to_silver", "table": "orders"}, config)
    
    logger.info(f"Orders bronze to silver transformation completed: {record_count:,} records in {duration_ms:.0f}ms")
    return silver_df


@lineage_job(
    name="bronze_to_silver_products",
    inputs=["s3://bucket/bronze/products"],
    outputs=["s3://bucket/silver/products"]
)
def transform_products_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame, config: Dict[str, Any] = None) -> DataFrame:
    """
    Transform products from bronze to silver layer.
    
    Args:
        spark: Spark session
        bronze_df: Bronze products DataFrame
        config: Configuration dictionary
        
    Returns:
        Cleaned silver products DataFrame
    """
    logger.info("Transforming products from bronze to silver")
    start_time = time.time()
    
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
    
    record_count = silver_df.count()
    duration_ms = (time.time() - start_time) * 1000
    
    # Emit metrics
    if config:
        emit_rowcount("records_silver", record_count, {"table": "products", "layer": "silver"}, config)
        emit_duration("transformation_duration", duration_ms, {"stage": "bronze_to_silver", "table": "products"}, config)
    
    logger.info(f"Products bronze to silver transformation completed: {record_count:,} records in {duration_ms:.0f}ms")
    return silver_df
