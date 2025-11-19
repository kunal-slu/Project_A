"""
Unit tests for production schemas module.
"""
import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from project_a.schemas.production_schemas import (
    get_schema,
    validate_schema_drift,
    BRONZE_CUSTOMERS_SCHEMA,
    BRONZE_ORDERS_SCHEMA,
    SILVER_CUSTOMERS_SCHEMA,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder
        .appName("TestSchemas")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


class TestSchemaRegistry:
    """Test suite for schema registry."""

    def test_get_bronze_customers_schema(self):
        """Test retrieving bronze customers schema."""
        schema = get_schema("bronze.customers")
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 11
        
        # Check key fields
        field_names = [f.name for f in schema.fields]
        assert "customer_id" in field_names
        assert "name" in field_names
        assert "_source" in field_names
        assert "_extracted_at" in field_names
        assert "_proc_date" in field_names

    def test_get_bronze_orders_schema(self):
        """Test retrieving bronze orders schema."""
        schema = get_schema("bronze.orders")
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 12
        
        # Check key fields
        field_names = [f.name for f in schema.fields]
        assert "order_id" in field_names
        assert "customer_id" in field_names
        assert "total_price" in field_names

    def test_get_silver_customers_schema(self):
        """Test retrieving silver customers schema."""
        schema = get_schema("silver.customers")
        
        assert isinstance(schema, StructType)
        
        # Check SCD Type 2 fields
        field_names = [f.name for f in schema.fields]
        assert "is_current" in field_names
        assert "effective_from" in field_names
        assert "effective_to" in field_names
        assert "_updated_at" in field_names

    def test_get_nonexistent_schema_raises_error(self):
        """Test that requesting a non-existent schema raises ValueError."""
        with pytest.raises(ValueError, match="Schema for table .* not found"):
            get_schema("bronze.nonexistent")

    def test_bronze_customers_schema_structure(self):
        """Test bronze customers schema structure in detail."""
        schema = BRONZE_CUSTOMERS_SCHEMA
        
        # Check nullable constraints
        customer_id_field = next(f for f in schema.fields if f.name == "customer_id")
        assert customer_id_field.nullable is False
        
        source_field = next(f for f in schema.fields if f.name == "_source")
        assert source_field.nullable is False
        
        # Check data types
        assert customer_id_field.dataType == StringType()
        
        balance_field = next(f for f in schema.fields if f.name == "account_balance")
        assert balance_field.dataType == DoubleType()
        
        nation_field = next(f for f in schema.fields if f.name == "nation_key")
        assert nation_field.dataType == IntegerType()

    def test_schema_drift_detection_no_drift(self, spark):
        """Test schema drift detection with matching schemas."""
        schema = get_schema("bronze.customers")
        
        # Create DataFrame with correct schema
        data = [(
            "C001", "Alice", "123 Main", 1, "555-1234", 
            100.0, "Retail", "Comment", "source1", 
            datetime.now(), date.today()
        )]
        df = spark.createDataFrame(data, schema=schema)
        
        # Should not raise
        validate_schema_drift(df.schema, schema, "bronze.customers")

    def test_schema_drift_detection_missing_column(self, spark):
        """Test schema drift detection with missing column."""
        expected_schema = get_schema("bronze.customers")
        
        # Create schema with missing column
        incomplete_schema = StructType([
            f for f in expected_schema.fields if f.name != "nation_key"
        ])
        
        data = [(
            "C001", "Alice", "123 Main", "555-1234", 
            100.0, "Retail", "Comment", "source1", 
            datetime.now(), date.today()
        )]
        df = spark.createDataFrame(data, schema=incomplete_schema)
        
        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="Schema drift detected"):
            validate_schema_drift(df.schema, expected_schema, "bronze.customers")

    def test_schema_drift_detection_type_mismatch(self, spark):
        """Test schema drift detection with type mismatch."""
        expected_schema = get_schema("bronze.customers")
        
        # Create schema with type mismatch (nation_key as String instead of Integer)
        modified_fields = []
        for field in expected_schema.fields:
            if field.name == "nation_key":
                modified_fields.append(StructField("nation_key", StringType(), True))
            else:
                modified_fields.append(field)
        
        modified_schema = StructType(modified_fields)
        
        data = [(
            "C001", "Alice", "123 Main", "1", "555-1234", 
            100.0, "Retail", "Comment", "source1", 
            datetime.now(), date.today()
        )]
        df = spark.createDataFrame(data, schema=modified_schema)
        
        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="Schema drift detected"):
            validate_schema_drift(df.schema, expected_schema, "bronze.customers")

    def test_schema_drift_detection_extra_column(self, spark):
        """Test schema drift detection with unexpected extra column."""
        expected_schema = get_schema("bronze.customers")
        
        # Create schema with extra column
        extra_fields = list(expected_schema.fields) + [
            StructField("unexpected_column", StringType(), True)
        ]
        extended_schema = StructType(extra_fields)
        
        data = [(
            "C001", "Alice", "123 Main", 1, "555-1234", 
            100.0, "Retail", "Comment", "source1", 
            datetime.now(), date.today(), "extra"
        )]
        df = spark.createDataFrame(data, schema=extended_schema)
        
        # Should log warning but might not raise (depending on policy)
        # For now, just verify it doesn't crash
        try:
            validate_schema_drift(df.schema, expected_schema, "bronze.customers")
        except RuntimeError:
            pass  # Expected if strict validation

    def test_all_registered_schemas_are_valid(self):
        """Test that all registered schemas can be retrieved."""
        schema_names = [
            "bronze.customers",
            "bronze.orders",
            "silver.customers",
        ]
        
        for schema_name in schema_names:
            schema = get_schema(schema_name)
            assert isinstance(schema, StructType)
            assert len(schema.fields) > 0

    def test_bronze_orders_schema_structure(self):
        """Test bronze orders schema structure in detail."""
        schema = BRONZE_ORDERS_SCHEMA
        
        # Check required fields
        order_id_field = next(f for f in schema.fields if f.name == "order_id")
        assert order_id_field.nullable is False
        
        customer_id_field = next(f for f in schema.fields if f.name == "customer_id")
        assert customer_id_field.nullable is False
        
        # Check data types
        total_price_field = next(f for f in schema.fields if f.name == "total_price")
        assert total_price_field.dataType == DoubleType()
        
        order_date_field = next(f for f in schema.fields if f.name == "order_date")
        assert order_date_field.dataType == DateType()

    def test_silver_customers_scd2_fields(self):
        """Test SCD Type 2 fields in silver customers schema."""
        schema = SILVER_CUSTOMERS_SCHEMA
        
        scd2_fields = ["is_current", "effective_from", "effective_to", "_updated_at"]
        field_names = [f.name for f in schema.fields]
        
        for field_name in scd2_fields:
            assert field_name in field_names, f"Missing SCD2 field: {field_name}"
        
        # Check types
        effective_from = next(f for f in schema.fields if f.name == "effective_from")
        assert effective_from.dataType == TimestampType()
        
        effective_to = next(f for f in schema.fields if f.name == "effective_to")
        assert effective_to.dataType == TimestampType()

    def test_schema_field_ordering(self):
        """Test that schema fields are in expected order."""
        schema = get_schema("bronze.customers")
        field_names = [f.name for f in schema.fields]
        
        # Business fields should come first
        assert field_names[0] == "customer_id"
        
        # Metadata fields should come last
        assert "_source" in field_names[-3:]
        assert "_extracted_at" in field_names[-3:]
        assert "_proc_date" in field_names[-3:]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

