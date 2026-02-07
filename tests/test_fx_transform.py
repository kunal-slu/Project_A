"""
Tests for FX data transformation logic.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

from project_a.jobs.fx_bronze_to_silver import (
    deduplicate_fx_rates,
    add_rate_categories,
    validate_fx_rates
)


@pytest.fixture
def spark_session():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test-fx-transform") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@pytest.fixture
def sample_fx_data(spark_session):
    """Create sample FX data for testing."""
    schema = StructType([
        StructField("ccy", StringType(), False),
        StructField("rate_to_base", DoubleType(), False),
        StructField("as_of_date", DateType(), False),
        StructField("base_currency", StringType(), False),
        StructField("ingestion_timestamp", StringType(), False)
    ])
    
    data = [
        ("USD", 1.0, "2025-01-27", "USD", "2025-01-27T10:00:00"),
        ("EUR", 0.85, "2025-01-27", "USD", "2025-01-27T10:00:00"),
        ("GBP", 0.75, "2025-01-27", "USD", "2025-01-27T10:00:00"),
        ("USD", 1.0, "2025-01-27", "USD", "2025-01-27T11:00:00"),  # Duplicate
        ("JPY", 110.0, "2025-01-27", "USD", "2025-01-27T10:00:00"),
        ("EUR", 0.86, "2025-01-28", "USD", "2025-01-28T10:00:00"),
    ]
    
    return spark_session.createDataFrame(data, schema)


def test_deduplicate_fx_rates(spark_session, sample_fx_data):
    """Test FX rates deduplication logic."""
    # Add duplicate records
    duplicate_data = [
        ("USD", 1.0, "2025-01-27", "USD", "2025-01-27T12:00:00"),  # Later timestamp
        ("EUR", 0.84, "2025-01-27", "USD", "2025-01-27T12:00:00"),  # Later timestamp
    ]
    
    duplicate_df = spark_session.createDataFrame(duplicate_data, sample_fx_data.schema)
    combined_df = sample_fx_data.union(duplicate_df)
    
    # Test deduplication
    result_df = deduplicate_fx_rates(spark_session, combined_df)
    
    # Should have 5 unique records (USD and EUR for 2025-01-27 should be deduplicated)
    assert result_df.count() == 5
    
    # Check that latest timestamps are kept
    usd_records = result_df.filter(col("ccy") == "USD").collect()
    assert len(usd_records) == 1
    assert usd_records[0]["ingestion_timestamp"] == "2025-01-27T12:00:00"


def test_add_rate_categories(spark_session, sample_fx_data):
    """Test rate category assignment."""
    result_df = add_rate_categories(spark_session, sample_fx_data)
    
    # Check categories
    categories = result_df.select("ccy", "rate_category").collect()
    category_map = {row["ccy"]: row["rate_category"] for row in categories}
    
    assert category_map["USD"] == "major"
    assert category_map["EUR"] == "major"
    assert category_map["GBP"] == "major"
    assert category_map["JPY"] == "major"


def test_validate_fx_rates_valid_data(spark_session, sample_fx_data):
    """Test validation with valid data."""
    result_df = add_rate_categories(spark_session, sample_fx_data)
    assert validate_fx_rates(result_df) is True


def test_validate_fx_rates_null_values(spark_session):
    """Test validation with null values."""
    # Create data with null values
    schema = StructType([
        StructField("ccy", StringType(), True),
        StructField("rate_to_base", DoubleType(), True),
        StructField("as_of_date", DateType(), True),
        StructField("base_currency", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True)
    ])
    
    data = [
        (None, 1.0, "2025-01-27", "USD", "2025-01-27T10:00:00"),  # Null ccy
        ("USD", None, "2025-01-27", "USD", "2025-01-27T10:00:00"),  # Null rate
        ("EUR", 0.85, None, "USD", "2025-01-27T10:00:00"),  # Null date
    ]
    
    df = spark_session.createDataFrame(data, schema)
    assert validate_fx_rates(df) is False


def test_validate_fx_rates_negative_rates(spark_session):
    """Test validation with negative rates."""
    schema = StructType([
        StructField("ccy", StringType(), False),
        StructField("rate_to_base", DoubleType(), False),
        StructField("as_of_date", DateType(), False),
        StructField("base_currency", StringType(), False),
        StructField("ingestion_timestamp", StringType(), False)
    ])
    
    data = [
        ("USD", -1.0, "2025-01-27", "USD", "2025-01-27T10:00:00"),  # Negative rate
        ("EUR", 0.0, "2025-01-27", "USD", "2025-01-27T10:00:00"),   # Zero rate
    ]
    
    df = spark_session.createDataFrame(data, schema)
    assert validate_fx_rates(df) is False


def test_validate_fx_rates_duplicates(spark_session):
    """Test validation with duplicate (as_of_date, ccy) combinations."""
    schema = StructType([
        StructField("ccy", StringType(), False),
        StructField("rate_to_base", DoubleType(), False),
        StructField("as_of_date", DateType(), False),
        StructField("base_currency", StringType(), False),
        StructField("ingestion_timestamp", StringType(), False)
    ])
    
    data = [
        ("USD", 1.0, "2025-01-27", "USD", "2025-01-27T10:00:00"),
        ("USD", 1.1, "2025-01-27", "USD", "2025-01-27T11:00:00"),  # Duplicate ccy+date
    ]
    
    df = spark_session.createDataFrame(data, schema)
    assert validate_fx_rates(df) is False
