"""
Tests for bronze_to_silver_behavior job.
"""

from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from jobs.bronze_to_silver_behavior import transform_bronze_to_silver_behavior


@pytest.fixture
def mock_spark(spark):
    """Mock SparkSession with DataFrame operations."""
    return spark


@pytest.fixture
def sample_bronze_behavior_data(spark):
    """Sample behavior data for testing."""
    data = [
        (
            "E1",
            "C1",
            "page_view",
            "2025-01-01 10:00:00",
            "SESS-123",
            "https://example.com/page1",
            None,
            "desktop",
            "US",
        ),
        (
            "E2",
            "C1",
            "CLICK",
            "2025-01-01 10:05:00",
            "SESS-123",
            "https://example.com/page2",
            "https://example.com",
            "mobile",
            "US",
        ),
        (
            "E3",
            "C2",
            "page_view",
            "2025-01-01 11:00:00",
            "SESS-456",
            "https://example.com/page1",
            None,
            "desktop",
            "CA",
        ),
        (
            "E1",
            "C1",
            "page_view",
            "2025-01-01 10:00:00",
            "SESS-123",
            "https://example.com/page1",
            None,
            "desktop",
            "US",
        ),  # Duplicate
    ]
    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("event_name", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("country", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_config():
    """Mock configuration dictionary."""
    return {
        "data_lake": {
            "bronze_path": "data/lakehouse_delta/bronze",
            "silver_path": "data/lakehouse_delta/silver",
        },
        "lineage": {"enabled": False},
        "monitoring": {"metrics_enabled": False},
    }


@patch("jobs.bronze_to_silver_behavior.emit_start")
@patch("jobs.bronze_to_silver_behavior.emit_complete")
@patch("jobs.bronze_to_silver_behavior.emit_metrics")
@patch("jobs.bronze_to_silver_behavior.GreatExpectationsRunner")
def test_transform_bronze_to_silver_behavior_success(
    mock_ge_runner,
    mock_emit_metrics,
    mock_emit_complete,
    mock_emit_start,
    mock_spark,
    sample_bronze_behavior_data,
    mock_config,
    tmp_path,
):
    """Test successful transformation from Bronze to Silver."""
    # Setup mocks
    mock_ge_instance = Mock()
    mock_ge_instance.init_context.return_value = None
    mock_ge_instance.run_checkpoint.return_value = {"success": True}
    mock_ge_runner.return_value = mock_ge_instance

    # Write sample data to bronze path
    bronze_path = tmp_path / "bronze" / "redshift" / "behavior"
    bronze_path.mkdir(parents=True)

    sample_bronze_behavior_data.write.format("delta").mode("overwrite").save(str(bronze_path))

    # Update config to use temp path
    mock_config["data_lake"]["bronze_path"] = str(tmp_path / "bronze")
    mock_config["data_lake"]["silver_path"] = str(tmp_path / "silver")

    # Execute transformation
    result_df = transform_bronze_to_silver_behavior(mock_spark, mock_config)

    # Assertions
    assert result_df is not None
    mock_emit_start.assert_called_once()
    mock_emit_complete.assert_called_once()
    mock_emit_metrics.assert_called_once()

    # Verify data transformations
    assert "event_name" in result_df.columns
    assert "_processing_ts" in result_df.columns


@patch("jobs.bronze_to_silver_behavior.emit_start")
@patch("jobs.bronze_to_silver_behavior.emit_complete")
@patch("jobs.bronze_to_silver_behavior.emit_fail")
def test_transform_handles_missing_bronze_data(
    mock_emit_fail, mock_emit_complete, mock_emit_start, mock_spark, mock_config, tmp_path
):
    """Test transformation handles missing bronze data gracefully."""
    # Set paths to non-existent location
    mock_config["data_lake"]["bronze_path"] = str(tmp_path / "nonexistent" / "bronze")
    mock_config["data_lake"]["silver_path"] = str(tmp_path / "silver")

    # Should handle missing data
    with pytest.raises((RuntimeError, ValueError, FileNotFoundError, OSError)):
        transform_bronze_to_silver_behavior(mock_spark, mock_config)

    # Should emit fail event
    mock_emit_fail.assert_called_once()


@patch("jobs.bronze_to_silver_behavior.emit_start")
@patch("jobs.bronze_to_silver_behavior.emit_complete")
@patch("jobs.bronze_to_silver_behavior.GreatExpectationsRunner")
def test_transform_with_ge_failure(
    mock_ge_runner,
    mock_emit_complete,
    mock_emit_start,
    mock_spark,
    sample_bronze_behavior_data,
    mock_config,
    tmp_path,
):
    """Test transformation fails when GE validation fails."""
    # Setup GE to fail
    mock_ge_instance = Mock()
    mock_ge_instance.init_context.return_value = None
    mock_ge_instance.run_checkpoint.return_value = {"success": False}
    mock_ge_runner.return_value = mock_ge_instance

    bronze_path = tmp_path / "bronze" / "redshift" / "behavior"
    bronze_path.mkdir(parents=True)
    sample_bronze_behavior_data.write.format("delta").mode("overwrite").save(str(bronze_path))

    mock_config["data_lake"]["bronze_path"] = str(tmp_path / "bronze")
    mock_config["data_lake"]["silver_path"] = str(tmp_path / "silver")

    # Should raise error on GE failure
    with pytest.raises(RuntimeError):
        transform_bronze_to_silver_behavior(mock_spark, mock_config)
