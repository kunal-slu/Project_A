"""Tests for runtime contract enforcement."""

from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from project_a.contracts.runtime_contracts import load_table_contracts, validate_contract


@pytest.fixture(scope="module")
def spark():
    try:
        spark = (
            SparkSession.builder.master("local[1]").appName("test_runtime_contracts").getOrCreate()
        )
    except Exception:
        pytest.skip("Spark unavailable in current environment")
    yield spark
    if os.environ.get("PROJECT_A_DISABLE_SPARK_STOP") != "1":
        spark.stop()


ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)


def test_orders_contract_fails_on_null_pk(spark):
    contracts = load_table_contracts("config/contracts/silver_contracts.yaml")
    orders_contract = contracts["orders_silver"]

    orders_df = spark.createDataFrame(
        [
            (None, "C1", "P1", "2025-01-01", 10.0, 1, "ok", "2025-01-01T00:00:00"),
        ],
        schema=ORDERS_SCHEMA,
    )
    customers_df = spark.createDataFrame([("C1",)], ["customer_id"])

    with pytest.raises(ValueError, match="Null violations|null primary keys|null primary keys"):
        validate_contract(
            orders_df,
            "orders_silver",
            orders_contract,
            parent_frames={"customers_silver": customers_df},
        )


def test_orders_contract_fails_on_duplicate_pk(spark):
    contracts = load_table_contracts("config/contracts/silver_contracts.yaml")
    orders_contract = contracts["orders_silver"]

    orders_df = spark.createDataFrame(
        [
            ("O1", "C1", "P1", "2025-01-01", 10.0, 1, "ok", "2025-01-01T00:00:00"),
            ("O1", "C1", "P1", "2025-01-01", 10.0, 1, "ok", "2025-01-01T00:00:00"),
        ],
        schema=ORDERS_SCHEMA,
    )
    customers_df = spark.createDataFrame([("C1",)], ["customer_id"])

    with pytest.raises(ValueError, match="duplicate primary keys"):
        validate_contract(
            orders_df,
            "orders_silver",
            orders_contract,
            parent_frames={"customers_silver": customers_df},
        )


def test_orders_contract_fails_on_referential_integrity(spark):
    contracts = load_table_contracts("config/contracts/silver_contracts.yaml")
    orders_contract = contracts["orders_silver"]

    orders_df = spark.createDataFrame(
        [
            ("O1", "C404", "P1", "2025-01-01", 10.0, 1, "ok", "2025-01-01T00:00:00"),
        ],
        schema=ORDERS_SCHEMA,
    )
    customers_df = spark.createDataFrame([("C1",)], ["customer_id"])

    with pytest.raises(ValueError, match="Referential integrity failed"):
        validate_contract(
            orders_df,
            "orders_silver",
            orders_contract,
            parent_frames={"customers_silver": customers_df},
        )


def test_orders_contract_passes_valid_dataset(spark):
    contracts = load_table_contracts("config/contracts/silver_contracts.yaml")
    orders_contract = contracts["orders_silver"]

    orders_df = spark.createDataFrame(
        [
            ("O1", "C1", "P1", "2025-01-01", 10.0, 1, "ok", "2025-01-01T00:00:00"),
            ("O2", "C2", "P2", "2025-01-02", 20.0, 1, "ok", "2025-01-02T00:00:00"),
        ],
        schema=ORDERS_SCHEMA,
    )
    customers_df = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])

    validate_contract(
        orders_df,
        "orders_silver",
        orders_contract,
        parent_frames={"customers_silver": customers_df},
    )
