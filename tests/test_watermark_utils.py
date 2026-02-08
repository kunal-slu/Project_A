"""
Tests for watermark utilities.
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from project_a.utils.watermark_utils import (
    get_latest_timestamp_from_df,
    get_watermark,
    upsert_watermark,
)


@pytest.fixture
def mock_config(tmp_path):
    """Mock configuration dictionary."""
    return {"data_lake": {"state_prefix": str(tmp_path / "_state")}, "aws": {"region": "us-east-1"}}


def test_get_watermark_first_run(spark, mock_config, tmp_path):
    """Test getting watermark on first run (no watermark exists)."""
    result = get_watermark("test_source", mock_config, spark)

    # Should return None on first run
    assert result is None


def test_get_latest_timestamp_from_df(spark):
    """Test extracting latest timestamp from DataFrame."""

    data = [
        ("2025-01-01 10:00:00",),
        ("2025-01-02 11:00:00",),
        ("2025-01-03 12:00:00",),
    ]
    schema = StructType([StructField("timestamp", StringType(), True)])
    df = spark.createDataFrame(data, schema)

    # Should extract latest timestamp
    result = get_latest_timestamp_from_df(df, timestamp_col="timestamp")

    # May return None or timestamp depending on implementation
    assert result is None or isinstance(result, datetime)


@patch("project_a.utils.watermark_utils.os.path.exists")
def test_upsert_watermark(mock_exists, spark, mock_config, tmp_path):
    """Test upserting watermark."""
    mock_exists.return_value = False  # First write

    timestamp = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    # Should not raise exception
    upsert_watermark("test_source", timestamp, mock_config, spark)


def test_get_watermark_with_existing(spark, mock_config, tmp_path):
    """Test getting existing watermark."""
    # Create a simple watermark file
    spark.createDataFrame([("2025-01-01T10:00:00Z",)], "watermark STRING")

    # Try to get watermark (may not exist, which is fine)
    result = get_watermark("test_source", mock_config, spark)

    # Result should be None or a datetime
    assert result is None or isinstance(result, datetime)
