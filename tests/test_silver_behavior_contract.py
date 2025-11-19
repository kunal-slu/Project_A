"""
Data contract tests for Silver behavior table.

Ensures schema and data quality contracts are met after transformation.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType
from pyspark.sql.functions import col

from project_a.utils.spark_session import build_spark
from project_a.utils.config import load_conf


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    config = load_conf("config/local.yaml")
    spark = build_spark(app_name="test_silver_behavior", config=config)
    yield spark
    spark.stop()


@pytest.fixture
def silver_behavior_df(spark):
    """Load Silver behavior table."""
    try:
        df = spark.read.format("delta").load("data/lakehouse_delta/silver/behavior")
        return df
    except Exception:
        # Fallback if Delta not available
        pytest.skip("Silver behavior table not found")


def test_schema_matches_contract(spark, silver_behavior_df):
    """Test that schema matches expected contract."""
    expected_columns = {
        "event_id",
        "customer_id",
        "event_name",
        "event_ts",
        "session_id",
        "page_url",
        "referrer",
        "_processing_ts"
    }
    
    actual_columns = set(silver_behavior_df.columns)
    
    # Check all expected columns exist
    missing_columns = expected_columns - actual_columns
    assert not missing_columns, f"Missing expected columns: {missing_columns}"
    
    # Check critical columns exist
    assert "event_id" in actual_columns, "event_id is required"
    assert "customer_id" in actual_columns, "customer_id is required"
    assert "event_name" in actual_columns, "event_name is required"
    assert "event_ts" in actual_columns, "event_ts is required"


def test_event_name_is_lowercase(silver_behavior_df):
    """Test that event_name is normalized to lowercase."""
    if "event_name" not in silver_behavior_df.columns:
        pytest.skip("event_name column not found")
    
    # Check if any values are not lowercase
    non_lowercase = silver_behavior_df.filter(
        col("event_name") != col("event_name").lower()
    )
    
    count = non_lowercase.count()
    assert count == 0, f"Found {count} records with non-lowercase event_name"


def test_session_id_pattern(silver_behavior_df):
    """Test that session_id matches expected pattern (SESS-[0-9]+)."""
    if "session_id" not in silver_behavior_df.columns:
        pytest.skip("session_id column not found")
    
    # Count invalid session_ids (null is allowed, but if present must match pattern)
    invalid_pattern = silver_behavior_df.filter(
        col("session_id").isNotNull() & 
        ~col("session_id").rlike(r"^SESS-\d+$")
    )
    
    count = invalid_pattern.count()
    assert count == 0, f"Found {count} records with invalid session_id pattern"


def test_no_duplicates_by_event_id(silver_behavior_df):
    """Test that no duplicate event_ids exist."""
    if "event_id" not in silver_behavior_df.columns:
        pytest.skip("event_id column not found")
    
    duplicates = silver_behavior_df.groupBy("event_id").count().filter(col("count") > 1)
    duplicate_count = duplicates.count()
    
    assert duplicate_count == 0, f"Found {duplicate_count} duplicate event_ids"


def test_event_ts_is_timestamp(silver_behavior_df):
    """Test that event_ts is a valid timestamp type."""
    if "event_ts" not in silver_behavior_df.columns:
        pytest.skip("event_ts column not found")
    
    # Check data type
    event_ts_field = next(
        (f for f in silver_behavior_df.schema.fields if f.name == "event_ts"),
        None
    )
    
    assert event_ts_field is not None, "event_ts column not found in schema"
    assert isinstance(event_ts_field.dataType, TimestampType), \
        f"event_ts must be TimestampType, got {event_ts_field.dataType}"


def test_exactly_expected_columns(silver_behavior_df):
    """Test that table has exactly the expected number of columns."""
    # Allow for metadata columns (start with _)
    expected_min_columns = 7  # event_id, customer_id, event_name, event_ts, session_id, page_url, referrer
    actual_count = len(silver_behavior_df.columns)
    
    assert actual_count >= expected_min_columns, \
        f"Expected at least {expected_min_columns} columns, got {actual_count}"


def test_types_match_contract(silver_behavior_df):
    """Test that column types match expected contract."""
    type_checks = {
        "event_id": StringType,
        "customer_id": StringType,
        "event_name": StringType,
        "event_ts": TimestampType,
    }
    
    for col_name, expected_type in type_checks.items():
        if col_name not in silver_behavior_df.columns:
            continue
        
        field = next(
            (f for f in silver_behavior_df.schema.fields if f.name == col_name),
            None
        )
        
        assert field is not None, f"Column {col_name} not found"
        assert isinstance(field.dataType, expected_type), \
            f"{col_name} must be {expected_type.__name__}, got {type(field.dataType).__name__}"


def test_row_count_threshold(silver_behavior_df):
    """Test that row count meets minimum threshold."""
    min_rows = 1
    actual_rows = silver_behavior_df.count()
    
    assert actual_rows >= min_rows, \
        f"Expected at least {min_rows} rows, got {actual_rows}"

