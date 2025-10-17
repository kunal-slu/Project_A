"""
Unit tests for orders transformation functions.
Quick proof of correctness for bronze to silver transforms.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from src.pyspark_interview_project.transforms.bronze_to_silver import transform_orders_bronze_to_silver


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


def test_orders_basic(spark):
    """Test basic orders transformation."""
    # Create test data
    test_data = [
        ("O1", "C1", "P1", 1, 100.0, 100.0, "2025-01-01T00:00:00", "Card", "PAID"),
        ("O2", "C2", "P2", 2, 50.0, 100.0, "2025-01-02T00:00:00", "Cash", "PAID"),
        ("O3", "C3", "P3", 1, 75.0, 75.0, "2025-01-03T00:00:00", "Card", "PENDING"),
    ]
    
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_date", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Transform
    result = transform_orders_bronze_to_silver(spark, df)
    
    # Assertions
    assert result.count() == 3
    assert len(result.columns) == 9  # Expected silver schema columns
    
    # Check that all required columns exist
    expected_columns = ["order_id", "customer_id", "product_id", "quantity", 
                       "unit_price", "total_amount", "order_date", "payment_method", "status"]
    for col in expected_columns:
        assert col in result.columns
    
    # Check data types
    assert result.schema["order_id"].dataType == StringType()
    assert result.schema["customer_id"].dataType == StringType()
    assert result.schema["quantity"].dataType == IntegerType()
    assert result.schema["unit_price"].dataType == DoubleType()
    assert result.schema["total_amount"].dataType == DoubleType()


def test_orders_with_nested_payment(spark):
    """Test orders transformation with nested payment structure."""
    from pyspark.sql.functions import struct
    
    # Create test data with nested payment
    test_data = [
        ("O1", "C1", "P1", 1, 100.0, "2025-01-01T00:00:00", "Card", "PAID"),
        ("O2", "C2", "P2", 2, 50.0, "2025-01-02T00:00:00", "Cash", "PENDING"),
    ]
    
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_date", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Add nested payment structure
    df = df.withColumn("payment", struct(
        df.payment_method.alias("method"),
        df.status.alias("status")
    ))
    
    # Transform
    result = transform_orders_bronze_to_silver(spark, df)
    
    # Assertions
    assert result.count() == 2
    assert "payment_method" in result.columns
    assert "status" in result.columns


def test_orders_filters_invalid_data(spark):
    """Test that invalid orders are filtered out."""
    # Create test data with invalid records
    test_data = [
        ("O1", "C1", "P1", 1, 100.0, 100.0, "2025-01-01T00:00:00", "Card", "PAID"),  # Valid
        (None, "C2", "P2", 2, 50.0, 100.0, "2025-01-02T00:00:00", "Cash", "PAID"),   # Invalid: null order_id
        ("O3", None, "P3", 1, 75.0, 75.0, "2025-01-03T00:00:00", "Card", "PENDING"),  # Invalid: null customer_id
        ("O4", "C4", "P4", 1, -10.0, -10.0, "2025-01-04T00:00:00", "Card", "PAID"),   # Invalid: negative amount
    ]
    
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_date", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Transform
    result = transform_orders_bronze_to_silver(spark, df)
    
    # Should only keep valid records
    assert result.count() == 1  # Only O1 should pass
    assert result.collect()[0]["order_id"] == "O1"


def test_orders_deduplication(spark):
    """Test that duplicate orders are removed."""
    # Create test data with duplicates
    test_data = [
        ("O1", "C1", "P1", 1, 100.0, 100.0, "2025-01-01T00:00:00", "Card", "PAID"),
        ("O1", "C1", "P1", 1, 100.0, 100.0, "2025-01-01T00:00:00", "Card", "PAID"),  # Duplicate
        ("O2", "C2", "P2", 2, 50.0, 100.0, "2025-01-02T00:00:00", "Cash", "PAID"),
    ]
    
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_date", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Transform
    result = transform_orders_bronze_to_silver(spark, df)
    
    # Should remove duplicates
    assert result.count() == 2  # O1 and O2, duplicate removed
    order_ids = [row["order_id"] for row in result.collect()]
    assert len(set(order_ids)) == 2  # No duplicates
