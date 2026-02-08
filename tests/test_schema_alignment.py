"""
Test Schema Alignment Across ETL Pipeline

Validates that:
1. Bronze -> Silver transformation produces correct column names
2. Silver -> Gold transformation uses correct column names
3. dbt models expect correct column names
4. Schema validator enforces contracts
"""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from project_a.utils.schema_validator import SchemaValidator


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing."""
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test_schema_alignment")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception:
        pytest.skip("Spark unavailable in current environment")

    yield spark


def test_orders_silver_schema_defined():
    """Test that orders_silver schema is defined in SchemaValidator."""
    schema = SchemaValidator.get_schema("orders_silver")
    assert schema is not None, "orders_silver schema not defined"

    # Verify critical columns
    field_names = [f.name for f in schema.fields]
    assert "order_id" in field_names
    assert "customer_id" in field_names
    assert "total_amount" in field_names  # CRITICAL: Must be total_amount, not amount_usd
    assert "order_date" in field_names

    # Verify amount_usd is NOT in schema (should be renamed to total_amount)
    assert "amount_usd" not in field_names, "Schema should use 'total_amount', not 'amount_usd'"


def test_orders_silver_total_amount_type():
    """Test that total_amount has correct type (DecimalType)."""
    schema = SchemaValidator.get_schema("orders_silver")

    total_amount_field = None
    for field in schema.fields:
        if field.name == "total_amount":
            total_amount_field = field
            break

    assert total_amount_field is not None, "total_amount field not found"
    assert isinstance(total_amount_field.dataType, DecimalType), (
        f"total_amount should be DecimalType, got {type(total_amount_field.dataType)}"
    )
    assert not total_amount_field.nullable, "total_amount should be non-nullable"


def test_bronze_to_silver_transformation_mock(spark):
    """
    Test that Bronze->Silver transformation produces correct schema.

    This simulates the transformation logic from bronze_to_silver.py
    """
    # Create mock bronze data with amount_usd
    bronze_data = [
        (
            "ORD001",
            "CUST001",
            "PROD001",
            date(2024, 1, 1),
            Decimal("100.50"),
            2,
            "completed",
            datetime(2024, 1, 1, 12, 0, 0),
        )
    ]

    bronze_schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("order_date", DateType()),
            StructField("amount_usd", DecimalType(10, 2)),  # Source uses amount_usd
            StructField("quantity", IntegerType()),
            StructField("status", StringType()),
            StructField("updated_at", TimestampType()),
        ]
    )

    bronze_df = spark.createDataFrame(bronze_data, bronze_schema)

    # Simulate transformation (rename amount_usd -> total_amount)
    silver_df = bronze_df.withColumnRenamed("amount_usd", "total_amount")

    # Verify output schema
    assert "total_amount" in silver_df.columns, "Silver layer must have 'total_amount' column"
    assert "amount_usd" not in silver_df.columns, "Silver layer should not have 'amount_usd' column"


def test_schema_validator_primary_key_null_detection(spark):
    """Test that SchemaValidator detects null primary keys."""
    # Create data with null PK
    data = [
        ("ORD001", "CUST001"),
        (None, "CUST002"),  # Null PK
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])

    # Should raise ValueError
    with pytest.raises(ValueError, match="null values in primary key"):
        SchemaValidator.validate_primary_key(df, "order_id", "test_orders")


def test_schema_validator_duplicate_key_detection(spark):
    """Test that SchemaValidator detects duplicate primary keys."""
    # Create data with duplicate PK
    data = [
        ("ORD001", "CUST001"),
        ("ORD001", "CUST002"),  # Duplicate PK
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])

    # Should raise ValueError
    with pytest.raises(ValueError, match="duplicate values"):
        SchemaValidator.validate_primary_key(df, "order_id", "test_orders")


def test_schema_validator_column_validation(spark):
    """Test that SchemaValidator validates required columns."""
    # Create data missing required column
    data = [("ORD001", "CUST001")]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])

    required_columns = ["order_id", "customer_id", "total_amount"]

    # Should raise ValueError for missing column
    with pytest.raises(ValueError, match="Missing required columns"):
        SchemaValidator.validate_columns(df, required_columns, "test_orders")


def test_dbt_silver_orders_source_expectation():
    """
    Test that dbt expects 'total_amount' in silver orders.

    This validates that our transformation aligns with dbt expectations.
    """
    # This is a documentation test - actual dbt sources.yml defines this
    # We validate the expectation here
    expected_columns_from_dbt = [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "total_amount",  # dbt expects this name
        "quantity",
        "status",
        "updated_at",
    ]

    # Get our defined schema
    schema = SchemaValidator.get_schema("orders_silver")
    actual_columns = [f.name for f in schema.fields]

    # All dbt expectations must be met
    for col in expected_columns_from_dbt:
        assert col in actual_columns, (
            f"dbt expects column '{col}' but it's not in our schema definition"
        )


def test_customers_silver_schema_defined():
    """Test that customers_silver schema is defined."""
    schema = SchemaValidator.get_schema("customers_silver")
    assert schema is not None

    field_names = [f.name for f in schema.fields]
    assert "customer_id" in field_names
    assert "email" in field_names
    assert "country" in field_names


def test_products_silver_schema_defined():
    """Test that products_silver schema is defined."""
    schema = SchemaValidator.get_schema("products_silver")
    assert schema is not None

    field_names = [f.name for f in schema.fields]
    assert "product_id" in field_names
    assert "price_usd" in field_names  # Keep price_usd in Silver
    assert "product_name" in field_names


def test_schema_apply_enforcement(spark):
    """Test that apply_schema enforces types correctly."""
    # Create data with wrong types
    data = [("ORD001", "CUST001", "PROD001", "2024-01-01", "100.50", "2", "completed", None)]

    # Create with all string types (simulating inferred schema)
    loose_schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("order_date", StringType()),  # Wrong type
            StructField("total_amount", StringType()),  # Wrong type
            StructField("quantity", StringType()),  # Wrong type
            StructField("status", StringType()),
            StructField("updated_at", StringType()),
        ]
    )

    df = spark.createDataFrame(data, loose_schema)

    # Apply strict schema
    enforced_df = SchemaValidator.apply_schema(df, "orders_silver")

    # Verify types are corrected
    schema_dict = {f.name: f.dataType for f in enforced_df.schema.fields}
    assert isinstance(schema_dict["order_date"], DateType), (
        "order_date should be DateType after enforcement"
    )
    assert isinstance(schema_dict["total_amount"], DecimalType), (
        "total_amount should be DecimalType after enforcement"
    )
    assert isinstance(schema_dict["quantity"], IntegerType), (
        "quantity should be IntegerType after enforcement"
    )


def test_end_to_end_schema_flow(spark):
    """
    End-to-end test: Bronze (amount_usd) -> Silver (total_amount) -> Validation.

    This validates the complete transformation flow.
    """
    # 1. Bronze layer (source data)
    bronze_data = [
        (
            "ORD001",
            "CUST001",
            "PROD001",
            date(2024, 1, 1),
            Decimal("100.50"),
            2,
            "completed",
            datetime(2024, 1, 1, 12, 0, 0),
        )
    ]

    bronze_schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("order_date", DateType()),
            StructField("amount_usd", DecimalType(10, 2)),  # Source name
            StructField("quantity", IntegerType()),
            StructField("status", StringType()),
            StructField("updated_at", TimestampType()),
        ]
    )

    bronze_df = spark.createDataFrame(bronze_data, bronze_schema)

    # 2. Transform to Silver (rename column)
    silver_df = bronze_df.withColumnRenamed("amount_usd", "total_amount")

    # 3. Validate against schema contract
    SchemaValidator.validate_columns(
        silver_df, ["order_id", "customer_id", "total_amount", "order_date"], "orders_silver"
    )

    # 4. Validate primary key
    SchemaValidator.validate_primary_key(silver_df, "order_id", "orders_silver")

    # Success - schema flow is correct
    assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
