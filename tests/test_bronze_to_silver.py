"""
Tests for bronze to silver transformations.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from pyspark_interview_project.transforms.bronze_to_silver import (
    transform_customers_bronze_to_silver,
    transform_orders_bronze_to_silver,
    transform_products_bronze_to_silver
)


@pytest.fixture
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()


@pytest.fixture
def sample_customers_df(spark):
    """Create sample customers DataFrame."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
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
        StructField("age", StringType(), True)
    ])
    
    data = [
        ("C001", "John", "Doe", "john@example.com", "123 Main St", "City", "State", "Country", "12345", "123-456-7890", "01/01/2023", "M", "25"),
        ("C002", "Jane", "Smith", "jane@example.com", "456 Oak Ave", "Town", "Province", "Country", "67890", "098-765-4321", "02/01/2023", "F", "30"),
        (None, "Bob", "Johnson", "bob@example.com", "789 Pine Rd", "Village", "Region", "Country", "11111", "555-123-4567", "03/01/2023", "M", "35")  # Null customer_id
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_orders_df(spark):
    """Create sample orders DataFrame."""
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])
    
    data = [
        ("O001", "C001", "P001", "2", "10.50", "21.00", "2023-01-01", "completed", "credit_card"),
        ("O002", "C002", "P002", "1", "25.00", "25.00", "2023-01-02", "pending", "paypal"),
        ("O003", "C003", "P003", "3", "15.75", "47.25", "2023-01-03", "completed", "debit_card")
    ]
    
    return spark.createDataFrame(data, schema)


def test_transform_customers_bronze_to_silver(spark, sample_customers_df):
    """Test customers bronze to silver transformation."""
    result_df = transform_customers_bronze_to_silver(spark, sample_customers_df)
    
    # Check that null customer_id rows are filtered out
    assert result_df.count() == 2  # Only 2 valid customers
    
    # Check that data types are correct
    assert result_df.schema["age"].dataType == IntegerType()
    assert result_df.schema["first_name"].dataType == StringType()
    
    # Check that duplicates are removed
    result_df_with_duplicates = sample_customers_df.union(sample_customers_df)
    deduplicated_df = transform_customers_bronze_to_silver(spark, result_df_with_duplicates)
    assert deduplicated_df.count() == 2  # Should still be 2 after deduplication


def test_transform_orders_bronze_to_silver(spark, sample_orders_df):
    """Test orders bronze to silver transformation."""
    result_df = transform_orders_bronze_to_silver(spark, sample_orders_df)
    
    # Check that all orders are processed
    assert result_df.count() == 3
    
    # Check that data types are correct
    assert result_df.schema["quantity"].dataType == IntegerType()
    assert result_df.schema["unit_price"].dataType == DoubleType()
    assert result_df.schema["total_amount"].dataType == DoubleType()
    
    # Check that negative amounts are filtered out
    negative_amount_data = [
        ("O004", "C001", "P001", "1", "10.00", "-5.00", "2023-01-04", "completed", "credit_card")
    ]
    negative_df = spark.createDataFrame(negative_amount_data, sample_orders_df.schema)
    negative_result = transform_orders_bronze_to_silver(spark, negative_df)
    assert negative_result.count() == 0  # Should be filtered out


def test_transform_products_bronze_to_silver(spark):
    """Test products bronze to silver transformation."""
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("stock_quantity", StringType(), True)
    ])
    
    data = [
        ("P001", "Widget A", "Electronics", "BrandX", "29.99", "100"),
        ("P002", "Widget B", "Electronics", "BrandY", "39.99", "50"),
        ("P003", None, "Books", "BrandZ", "19.99", "25")  # Null product_name
    ]
    
    products_df = spark.createDataFrame(data, schema)
    result_df = transform_products_bronze_to_silver(spark, products_df)
    
    # Check that null product_name rows are filtered out
    assert result_df.count() == 2  # Only 2 valid products
    
    # Check that data types are correct
    assert result_df.schema["price"].dataType == DoubleType()
    assert result_df.schema["stock_quantity"].dataType == IntegerType()
