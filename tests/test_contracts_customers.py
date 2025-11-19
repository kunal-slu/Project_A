"""
Test data contracts for customers table.
"""
import pytest
from pyspark.sql import SparkSession

from project_a.utils.contracts import load_contract, validate_schema, enforce_not_null


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for tests."""
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_customers_contract(spark):
    """Test that customers DataFrame validates against contract."""
    contract = load_contract("snowflake_customers.schema.json")
    
    # Create test DataFrame with valid data
    df = spark.createDataFrame(
        [
            ("c1", "Alice", "alice@example.com", "US"),
            ("c2", "Bob", "bob@example.com", "GB"),
        ],
        ["customer_id", "customer_name", "email", "country"],
    )
    
    # Should not raise
    validate_schema(df, contract)
    
    # Test not-null enforcement
    df_clean = enforce_not_null(df, contract.required)
    assert df_clean.count() == 2


def test_customers_contract_missing_required(spark):
    """Test that missing required columns raise ValueError."""
    contract = load_contract("snowflake_customers.schema.json")
    
    # Create DataFrame missing required column
    df = spark.createDataFrame(
        [
            ("Alice", "alice@example.com"),
            ("Bob", "bob@example.com"),
        ],
        ["customer_name", "email"],  # Missing customer_id
    )
    
    # Should raise ValueError
    with pytest.raises(ValueError, match="missing required columns"):
        validate_schema(df, contract)


def test_customers_contract_null_filtering(spark):
    """Test that null values in required columns are filtered."""
    contract = load_contract("snowflake_customers.schema.json")
    
    # Create DataFrame with nulls in required columns
    df = spark.createDataFrame(
        [
            ("c1", "Alice", "alice@example.com", "US"),
            (None, "Bob", "bob@example.com", "GB"),  # null customer_id
            ("c3", None, "charlie@example.com", "CA"),  # null customer_name
        ],
        ["customer_id", "customer_name", "email", "country"],
    )
    
    # Filter nulls
    df_clean = enforce_not_null(df, contract.required)
    
    # Should only have 1 row (c1, Alice)
    assert df_clean.count() == 1

