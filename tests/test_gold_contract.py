"""
Tests for gold layer schema contracts.
"""

import pytest
import json
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from project_a.schema.validator import SchemaValidator


@pytest.fixture
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@pytest.fixture
def schema_validator(spark):
    """Create schema validator instance."""
    return SchemaValidator(spark)


@pytest.fixture
def fact_orders_contract():
    """Create fact_orders schema contract."""
    return {
        "type": "struct",
        "fields": [
            {"name": "order_id", "type": "string", "nullable": False},
            {"name": "customer_id", "type": "string", "nullable": False},
            {"name": "product_id", "type": "string", "nullable": False},
            {"name": "quantity", "type": "integer", "nullable": False},
            {"name": "unit_price", "type": "double", "nullable": False},
            {"name": "total_amount", "type": "double", "nullable": False},
            {"name": "order_date", "type": "date", "nullable": False},
            {"name": "status", "type": "string", "nullable": False},
            {"name": "payment_method", "type": "string", "nullable": False},
            {"name": "customer_age", "type": "integer", "nullable": True},
            {"name": "customer_gender", "type": "string", "nullable": True},
            {"name": "customer_country", "type": "string", "nullable": True},
            {"name": "product_category", "type": "string", "nullable": True},
            {"name": "product_brand", "type": "string", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": False}
        ]
    }


def test_bronze_schema_validation(schema_validator):
    """Test bronze layer validation (permissive)."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("data", StringType(), True)
    ])
    
    df = schema_validator.spark.createDataFrame([], schema)
    
    # Bronze validation should always pass
    assert schema_validator.validate_bronze_schema(df, "test_table") == True


def test_silver_schema_compatibility(schema_validator):
    """Test silver layer schema compatibility."""
    # Create existing schema
    existing_schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False)
    ])
    
    # Create compatible schema (additive nullable field)
    compatible_schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("email", StringType(), True)  # New nullable field
    ])
    
    # Create incompatible schema (removed field)
    incompatible_schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False)
        # age field removed
    ])
    
    # Test compatible schema
    compatible_df = schema_validator.spark.createDataFrame([], compatible_schema)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(existing_schema.jsonValue(), f)
        existing_schema_path = f.name

    try:
        assert schema_validator.validate_silver_schema(compatible_df, "test_table", existing_schema_path) == True
    finally:
        if Path(existing_schema_path).exists():
            Path(existing_schema_path).unlink()

    # Test incompatible schema
    incompatible_df = schema_validator.spark.createDataFrame([], incompatible_schema)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(existing_schema.jsonValue(), f)
        existing_schema_path = f.name
    
    try:
        assert schema_validator.validate_silver_schema(incompatible_df, "test_table", existing_schema_path) == False
    finally:
        if Path(existing_schema_path).exists():
            Path(existing_schema_path).unlink()


def test_gold_schema_contract(schema_validator, fact_orders_contract):
    """Test gold layer schema contract enforcement."""
    # Create contract file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(fact_orders_contract, f)
        contract_path = f.name
    
    try:
        # Create matching schema
        matching_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DoubleType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("order_date", StringType(), False),  # Note: date vs string
            StructField("status", StringType(), False),
            StructField("payment_method", StringType(), False),
            StructField("customer_age", IntegerType(), True),
            StructField("customer_gender", StringType(), True),
            StructField("customer_country", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_brand", StringType(), True),
            StructField("created_at", TimestampType(), False)
        ])
        
        matching_df = schema_validator.spark.createDataFrame([], matching_schema)
        
        # This should fail due to type mismatch (date vs string)
        assert schema_validator.validate_gold_schema(matching_df, "fact_orders", contract_path) == False
        
        # Create non-matching schema (missing field)
        non_matching_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False)
            # Missing other required fields
        ])
        
        non_matching_df = schema_validator.spark.createDataFrame([], non_matching_schema)
        assert schema_validator.validate_gold_schema(non_matching_df, "fact_orders", contract_path) == False
        
    finally:
        Path(contract_path).unlink()


def test_schema_contract_file_not_found(schema_validator):
    """Test behavior when contract file doesn't exist."""
    schema = StructType([
        StructField("id", StringType(), False)
    ])
    
    df = schema_validator.spark.createDataFrame([], schema)
    
    # Should fail when contract file doesn't exist
    assert schema_validator.validate_gold_schema(df, "test_table", "nonexistent.json") == False
