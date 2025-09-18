"""
extract.py
Extracts data from customers.csv, products.csv, and orders.json with schema enforcement.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType,
                               MapType, StringType, StructField, StructType)


def extract_customers(spark: SparkSession, path: str) -> DataFrame:
    schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    # Allow tests to pass when called from repo root or tests/
    if not os.path.exists(path):
        alt = os.path.join("tests", "data", os.path.basename(path))
        path = alt if os.path.exists(alt) else path
    # Enforce schema on read; fail fast on corrupt records in prod if desired
    df = spark.read.option("header", True).schema(schema).csv(path)
    return df.withColumn(
        "registration_date", to_date(col("registration_date"), "MM/dd/yy")
    )


def extract_products(spark: SparkSession, path: str) -> DataFrame:
    schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("in_stock", BooleanType(), True),
            StructField("launch_date", StringType(), True),
            StructField("rating", DoubleType(), True),
            StructField(
                "tags", StringType(), True
            ),  # If list, can use ArrayType(StringType())
        ]
    )
    if not os.path.exists(path):
        alt = os.path.join("tests", "data", os.path.basename(path))
        path = alt if os.path.exists(alt) else path
    df = spark.read.option("header", True).schema(schema).csv(path)
    df = df.withColumn("launch_date", to_date(col("launch_date"), "MM/dd/yy"))
    # Optional: Parse tags to array if present as string list: ["new", "hot"]
    return df


def extract_orders_json(spark: SparkSession, path: str) -> DataFrame:
    # Nested payment field support
    order_schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("shipping_city", StringType(), True),
            StructField("shipping_state", StringType(), True),
            StructField("shipping_country", StringType(), True),
            StructField(
                "payment", MapType(StringType(), StringType()), True
            ),  # payment.method, payment.status
        ]
    )
    if not os.path.exists(path):
        alt = os.path.join("tests", "data", os.path.basename(path))
        path = alt if os.path.exists(alt) else path
    df = spark.read.schema(order_schema).json(path, multiLine=True)
    return df


def extract_returns(spark: SparkSession, path: str) -> DataFrame:
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("return_date", StringType(), True),
            StructField("reason", StringType(), True),
        ]
    )
    return spark.read.schema(schema).json(path, multiLine=True)


def extract_exchange_rates(spark: SparkSession, path: str) -> DataFrame:
    schema = StructType(
        [
            StructField("currency", StringType(), False),
            StructField("usd_rate", DoubleType(), True),
        ]
    )
    return spark.read.option("header", True).schema(schema).csv(path)


def extract_inventory_snapshots(spark: SparkSession, path: str) -> DataFrame:
    schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("on_hand", IntegerType(), True),
            StructField("warehouse", StringType(), True),
        ]
    )
    return spark.read.option("header", True).schema(schema).csv(path)
