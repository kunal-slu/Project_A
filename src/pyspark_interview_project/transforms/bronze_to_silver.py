"""
Bronze → Silver transformations with data cleaning and schema alignment.

Functions:
- transform_customers_bronze_to_silver(spark, bronze_df) -> DataFrame
- transform_orders_bronze_to_silver(spark, bronze_df)    -> DataFrame
- transform_products_bronze_to_silver(spark, bronze_df)  -> DataFrame
- run_transform(spark, config) -> Dict[str, int]

Expected config keys:
  io:
    format: csv | json               # how to READ bronze (default csv)
    silver_format: parquet | delta   # how to WRITE silver (default parquet)
  paths:
    bronze:
      customers: <path or glob>
      orders:    <path or glob>
      products:  <path or glob>
    silver:
      customers: <dir>
      orders:    <dir>
      products:  <dir>
"""

import logging
from typing import Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, trim, when, to_date, to_timestamp, upper, lower, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, DateType
)

from ..dq.smoke import run_dq_checks

logger = logging.getLogger(__name__)

# ---------- Strict output schemas to prevent drift ----------
SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),  # Not nullable
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("segment", StringType(), True),
    StructField("country", StringType(), True),
])

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_date", DateType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
])

# Legacy schema for backward compatibility
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("first_name",  StringType()),
    StructField("last_name",   StringType()),
    StructField("email",       StringType()),
    StructField("address",     StringType()),
    StructField("city",        StringType()),
    StructField("state",       StringType()),
    StructField("country",     StringType()),
    StructField("zip",         StringType()),
    StructField("phone",       StringType()),
    StructField("registration_date", DateType()),
    StructField("gender",      StringType()),
    StructField("age",         IntegerType()),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id",       StringType()),
    StructField("customer_id",    StringType()),
    StructField("product_id",     StringType()),
    StructField("quantity",       IntegerType()),
    StructField("unit_price",     DoubleType()),
    StructField("total_amount",   DoubleType()),
    StructField("order_date",     TimestampType()),
    StructField("payment_method", StringType()),
    StructField("status",         StringType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",    StringType()),
    StructField("product_name",  StringType()),
    StructField("category",      StringType()),
    StructField("brand",         StringType()),
    StructField("price",         DoubleType()),
    StructField("stock_quantity", IntegerType()),
])


def _safe_to_date_any(s: DataFrame, colname: str) -> DataFrame:
    """Robust date parser for DATE columns (customers)."""
    parsed = coalesce(
        to_date(col(colname), "MM/dd/yy"),
        to_date(col(colname), "yyyy-MM-dd"),
        to_date(col(colname))  # final fallback (Spark’s best effort)
    )
    return s.withColumn(colname, parsed)


def _safe_to_timestamp_any(s: DataFrame, colname: str) -> DataFrame:
    """Robust timestamp parser for TIMESTAMP columns (orders)."""
    parsed = coalesce(
        to_timestamp(col(colname), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(colname), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(colname), "yyyy-MM-dd"),
        to_timestamp(col(colname))  # final fallback
    )
    return s.withColumn(colname, parsed)


def transform_customers_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Transform customers from bronze to silver layer.
    - Trims strings, casts types, normalizes gender, guards ages, dedupes on customer_id.
    - Includes schema validation and data quality checks.
    """
    logger.info("Transforming customers from bronze to silver")

    # Create silver schema-compliant DataFrame
    df = bronze_df.select(
        col("customer_id").cast("string").alias("customer_id"),
        trim(col("name")).alias("name"),  # Updated to match silver schema
        trim(col("email")).alias("email"),
        col("created_date").alias("created_date"),  # Updated to match silver schema
        trim(col("segment")).alias("segment"),  # Updated to match silver schema
        trim(col("country")).alias("country"),
    )

    # Try multiple date formats for created_date
    df = _safe_to_date_any(df, "created_date")

    # Filter invalids
    df = df.filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull()
    )

    # dedupe
    df = df.dropDuplicates(["customer_id"])

    # enforce schema to avoid drift
    df = spark.createDataFrame(df.rdd, SILVER_CUSTOMERS_SCHEMA)

    # Row count guard
    if df.limit(1).count() == 0:
        raise ValueError("Customers silver is empty; check input/schema mapping")

    # Run data quality checks
    run_dq_checks(df, "customers")

    logger.info("Customers bronze → silver complete")
    return df


def transform_orders_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Transform orders from bronze to silver layer.
    - Supports nested payment struct or flat columns.
    - Guards unit_price division by zero.
    - Dedupe on order_id.
    """
    logger.info("Transforming orders from bronze to silver")

    columns = set(bronze_df.columns)
    if "payment" in columns and "payment_method" not in columns:
        # nested payment struct
        df = bronze_df.select(
            col("order_id").cast("string").alias("order_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("product_id").cast("string").alias("product_id"),
            col("quantity").cast("int").alias("quantity"),
            (col("total_amount")).cast("double").alias("total_amount"),
            col("order_date").alias("order_date"),
            col("payment.method").cast("string").alias("payment_method"),
            col("payment.status").cast("string").alias("status"),
        )
    else:
        # flat layout
        df = bronze_df.select(
            col("order_id").cast("string").alias("order_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("product_id").cast("string").alias("product_id"),
            col("quantity").cast("int").alias("quantity"),
            col("unit_price").cast("double").alias("unit_price"),
            col("total_amount").cast("double").alias("total_amount"),
            col("order_date").alias("order_date"),
            col("payment_method").cast("string").alias("payment_method"),
            col("status").cast("string").alias("status"),
        )

    # parse timestamp with multiple patterns
    df = _safe_to_timestamp_any(df, "order_date")

    # (re)compute unit_price safely if missing or quantity=0
    df = df.withColumn(
        "unit_price",
        when((col("unit_price").isNull()) & (col("quantity") > 0),
             col("total_amount") / col("quantity"))
        .when(col("quantity") <= 0, None)
        .otherwise(col("unit_price"))
        .cast("double")
    )

    # minimal validity checks
    df = df.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        (col("total_amount") >= 0)
    )

    # dedupe
    df = df.dropDuplicates(["order_id"])

    # enforce schema
    df = spark.createDataFrame(df.rdd, SILVER_ORDERS_SCHEMA)

    # Row count guard
    if df.limit(1).count() == 0:
        raise ValueError("Orders silver is empty; check input/schema mapping")

    # Run data quality checks
    run_dq_checks(df, "orders")

    logger.info("Orders bronze → silver complete")
    return df


def transform_products_bronze_to_silver(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Transform products from bronze to silver layer.
    - Supports boolean-ish `in_stock` or numeric `stock_quantity`.
    - Dedupe on product_id.
    """
    logger.info("Transforming products from bronze to silver")

    columns = set(bronze_df.columns)
    if "in_stock" in columns:
        is_in_stock = lower(trim(col("in_stock"))).isin("true", "1", "yes", "y")
        df = bronze_df.select(
            col("product_id").cast("string").alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("brand")).alias("brand"),
            col("price").cast("double").alias("price"),
            when(is_in_stock, 100).otherwise(0).cast("int").alias("stock_quantity"),
        )
    else:
        df = bronze_df.select(
            col("product_id").cast("string").alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("brand")).alias("brand"),
            col("price").cast("double").alias("price"),
            col("stock_quantity").cast("int").alias("stock_quantity"),
        )

    df = df.filter(
        col("product_id").isNotNull() &
        col("product_name").isNotNull() &
        (col("price") >= 0)
    )

    df = df.dropDuplicates(["product_id"])

    # enforce schema
    df = spark.createDataFrame(df.rdd, PRODUCTS_SCHEMA)

    logger.info("Products bronze → silver complete")
    return df


def run_transform(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, int]:
    """
    Orchestrate Bronze → Silver transformations for customers, orders, products.
    Reads Bronze paths from config, writes Silver outputs, and returns row counts.
    """
    logger.info("Starting run_transform for Bronze → Silver")

    # ---- Reader based on config.io.format ----
    def _reader(path: str) -> DataFrame:
        fmt = (config.get("io", {}).get("format") or "csv").lower()
        if fmt == "json":
            return (spark.read
                    .option("multiline", "false")
                    .option("mode", "PERMISSIVE")
                    .json(path))
        # default csv
        return (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "PERMISSIVE")
                .csv(path))

    # ---- Writer based on config.io.silver_format ----
    out_fmt = (config.get("io", {}).get("silver_format") or "parquet").lower()

    def _write(df: DataFrame, path: str):
        if out_fmt == "delta":
            (df.write
             .format("delta")
             .mode("overwrite")
             .save(path))
        else:
            # parquet default
            (df.write
             .mode("overwrite")
             .parquet(path))

    # ---- Read bronze ----
    bronze_paths = config["paths"]["bronze"]
    silver_paths = config["paths"]["silver"]

    bronze_customers = _reader(bronze_paths["customers"])
    bronze_orders    = _reader(bronze_paths["orders"])
    bronze_products  = _reader(bronze_paths["products"])

    # ---- Transform ----
    silver_customers = transform_customers_bronze_to_silver(spark, bronze_customers)
    silver_orders    = transform_orders_bronze_to_silver(spark, bronze_orders)
    silver_products  = transform_products_bronze_to_silver(spark, bronze_products)

    # ---- Write silver ----
    _write(silver_customers, silver_paths["customers"])
    _write(silver_orders,    silver_paths["orders"])
    _write(silver_products,  silver_paths["products"])

    # ---- Return counts ----
    result = {
        "customers": silver_customers.count(),
        "orders":    silver_orders.count(),
        "products":  silver_products.count(),
    }
    logger.info(f"run_transform completed with counts: {result}")
    return result