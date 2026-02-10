"""Production schema registry used by tests and runtime validation."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("nation_key", IntegerType(), True),
        StructField("phone", StringType(), True),
        StructField("account_balance", DoubleType(), True),
        StructField("market_segment", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("_source", StringType(), False),
        StructField("_extracted_at", TimestampType(), False),
        StructField("_proc_date", DateType(), False),
    ]
)

BRONZE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("order_date", DateType(), True),
        StructField("priority", StringType(), True),
        StructField("clerk", StringType(), True),
        StructField("ship_priority", IntegerType(), True),
        StructField("comment", StringType(), True),
        StructField("_source", StringType(), False),
        StructField("_extracted_at", TimestampType(), False),
        StructField("_proc_date", DateType(), False),
    ]
)

SILVER_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_current", StringType(), True),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("_updated_at", TimestampType(), True),
    ]
)

_SCHEMA_REGISTRY = {
    "bronze.customers": BRONZE_CUSTOMERS_SCHEMA,
    "bronze.orders": BRONZE_ORDERS_SCHEMA,
    "silver.customers": SILVER_CUSTOMERS_SCHEMA,
}


def get_schema(table_name: str) -> StructType:
    schema = _SCHEMA_REGISTRY.get(table_name)
    if schema is None:
        raise ValueError(f"Schema for table {table_name} not found")
    return schema


def validate_schema_drift(actual: StructType, expected: StructType, table_name: str) -> None:
    expected_fields = {f.name: f for f in expected.fields}
    actual_fields = {f.name: f for f in actual.fields}

    missing = set(expected_fields) - set(actual_fields)
    if missing:
        raise RuntimeError(
            f"Schema drift detected for {table_name}: missing columns {sorted(missing)}"
        )

    mismatched = []
    for col_name in set(expected_fields) & set(actual_fields):
        if expected_fields[col_name].dataType != actual_fields[col_name].dataType:
            mismatched.append(col_name)

    if mismatched:
        raise RuntimeError(
            f"Schema drift detected for {table_name}: type mismatch in {sorted(mismatched)}"
        )
