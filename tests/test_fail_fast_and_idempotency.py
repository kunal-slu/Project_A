"""Assertion-level proof tests for fail-fast behavior and deterministic transforms."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from project_a.transform import build_fact_orders, join_examples
from project_a.transforms.bronze_to_silver import (
    transform_customers_bronze_to_silver,
    transform_orders_bronze_to_silver,
    transform_products_bronze_to_silver,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test_fail_fast_and_idempotency")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception:
        pytest.skip("Spark unavailable in current environment")
    yield spark


def test_fail_fast_raises_on_null_primary_keys(spark):
    """Strict mode must fail when primary keys are null."""
    df = spark.createDataFrame(
        [
            ("O1", "C1", "P1", "1", "100.0"),
            (None, "C2", "P2", "1", "200.0"),
        ],
        ["order_id", "customer_id", "product_id", "quantity", "total_amount"],
    )

    with pytest.raises(ValueError, match="null primary keys"):
        transform_orders_bronze_to_silver(spark, df, strict=True)


def test_fail_fast_raises_on_unexpected_row_drop(spark):
    """Strict mode must fail when filtered rows exceed threshold."""
    df = spark.createDataFrame(
        [
            ("O1", "C1", "P1", "1", "100.0"),
            ("O1", "C1", "P1", "1", "100.0"),
            ("O2", "C2", "P2", "1", "200.0"),
        ],
        ["order_id", "customer_id", "product_id", "quantity", "total_amount"],
    )

    with pytest.raises(ValueError, match="Unexpected row drop"):
        transform_orders_bronze_to_silver(spark, df, strict=True, max_row_drop_ratio=0.10)


def test_orders_transform_is_idempotent(spark):
    """Running transform twice on same input should yield same rows."""
    df = spark.createDataFrame(
        [
            ("O1", "C1", "P1", "2", "100.0"),
            ("O1", "C1", "P1", "2", "100.0"),
            ("O2", "C2", "P2", "1", "40.0"),
        ],
        ["order_id", "customer_id", "product_id", "quantity", "total_amount"],
    )

    once = transform_orders_bronze_to_silver(spark, df)
    twice = transform_orders_bronze_to_silver(spark, df)

    once_rows = once.orderBy("order_id").collect()
    twice_rows = twice.orderBy("order_id").collect()
    assert once_rows == twice_rows
    assert once.count() == 2


def test_end_to_end_join_and_aggregation_assertions(spark):
    """Integration-style proof: row counts and aggregate values are deterministic."""
    customers_bronze = spark.createDataFrame(
        [
            ("C1", "Ada", "28"),
            ("C2", "Bob", "31"),
        ],
        ["customer_id", "first_name", "age"],
    )
    products_bronze = spark.createDataFrame(
        [
            ("P1", "Phone", "599.0", "10"),
            ("P2", "Mouse", "50.0", "100"),
        ],
        ["product_id", "product_name", "price", "stock_quantity"],
    )
    orders_bronze = spark.createDataFrame(
        [
            ("O1", "C1", "P1", "1", "599.0", "2025-01-01"),
            ("O2", "C2", "P2", "2", "100.0", "2025-01-02"),
        ],
        ["order_id", "customer_id", "product_id", "quantity", "total_amount", "order_date"],
    )

    customers_silver = transform_customers_bronze_to_silver(spark, customers_bronze)
    products_silver = transform_products_bronze_to_silver(spark, products_bronze)
    orders_silver = transform_orders_bronze_to_silver(spark, orders_bronze)

    joined = join_examples(customers_silver, products_silver, orders_silver)
    fact = build_fact_orders(joined.withColumn("price", F.col("unit_price")))

    assert customers_silver.count() == 2
    assert products_silver.count() == 2
    assert orders_silver.count() == 2
    assert joined.count() == 2

    revenue = fact.agg(F.sum("revenue").alias("total_revenue")).collect()[0]["total_revenue"]
    assert revenue == 699.0
