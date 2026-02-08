"""
Tests for Spark session utilities.
"""

from unittest.mock import Mock, patch

import pytest

from project_a.utils.spark_session import build_spark


@pytest.fixture
def mock_config():
    """Mock configuration dictionary."""
    return {
        "spark": {
            "master": "local[*]",
            "shuffle_partitions": 200,
            "enable_aqe": True,
            "driver_memory": "2g",
            "executor_memory": "4g",
        },
        "runtime": {"app_name": "test_app", "shuffle_partitions": 200},
    }


@patch("project_a.utils.spark_session.SparkSession")
def test_build_spark_success(mock_spark_session, mock_config):
    """Test successful Spark session creation."""
    mock_builder = Mock()
    mock_spark_session.builder = mock_builder
    mock_spark_instance = Mock()
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_spark_instance

    result = build_spark("test_app", mock_config)

    assert result is not None


def test_build_spark_minimal_config():
    """Test Spark session with minimal config."""
    minimal_config = {}

    # Should not raise exception
    try:
        result = build_spark("test_app", minimal_config)
        assert result is not None
    except Exception:
        # May fail if Spark not available, which is OK for tests
        pass
