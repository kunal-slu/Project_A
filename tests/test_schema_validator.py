"""
Tests for schema validator.
"""

from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from project_a.utils.schema_validator import SchemaValidator


@pytest.fixture
def expected_schema():
    """Expected schema."""
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )


@pytest.fixture
def actual_schema_match():
    """Actual schema that matches."""
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )


def test_validate_schema_match(spark, expected_schema, actual_schema_match):
    """Test schema validation with matching schemas."""
    if isinstance(spark, MagicMock):
        pytest.skip("Spark unavailable in current environment")
    data = [("1", "John", 30)]
    df = spark.createDataFrame(data, actual_schema_match)

    # Should pass validation
    # Should not raise
    SchemaValidator.validate_schema(df, expected_schema, table_name="test_table", strict=True)


def test_validate_schema_missing_column(spark, expected_schema):
    """Test schema validation with missing columns."""
    if isinstance(spark, MagicMock):
        pytest.skip("Spark unavailable in current environment")
    # Schema missing "age" column
    actual_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )
    data = [("1", "John")]
    df = spark.createDataFrame(data, actual_schema)

    with pytest.raises(ValueError):
        SchemaValidator.validate_schema(df, expected_schema, table_name="test_table", strict=True)
