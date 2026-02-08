"""
Tests for metrics collector.
"""

from unittest.mock import Mock, patch

import pytest

from project_a.monitoring.metrics_collector import emit_duration, emit_metrics, emit_rowcount


@pytest.fixture
def mock_config():
    """Mock configuration dictionary."""
    return {
        "monitoring": {"metrics_enabled": True, "cloudwatch": {"namespace": "ETL/Pipeline"}},
        "aws": {"region": "us-east-1"},
    }


@patch("project_a.monitoring.metrics_collector.boto3")
def test_emit_rowcount_cloudwatch(mock_boto3, mock_config):
    """Test emit_rowcount with CloudWatch."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    emit_rowcount("test_metric", 100, {}, mock_config)

    # Verify CloudWatch put_metric_data called
    mock_client.put_metric_data.assert_called_once()


def test_emit_rowcount_local(mock_config):
    """Test emit_rowcount in local mode."""
    mock_config["monitoring"]["metrics_enabled"] = False

    # Should not raise exception
    emit_rowcount("test_metric", 100, {}, mock_config)


@patch("project_a.monitoring.metrics_collector.boto3")
def test_emit_duration(mock_boto3, mock_config):
    """Test emit_duration."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    emit_duration("test_duration", 5.5, {}, mock_config)

    mock_client.put_metric_data.assert_called_once()


@patch("project_a.monitoring.metrics_collector.boto3")
def test_emit_metrics(mock_boto3, mock_config):
    """Test emit_metrics."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    emit_metrics(
        job_name="test_job",
        rows_in=1000,
        rows_out=950,
        duration_seconds=60.5,
        dq_status="pass",
        config=mock_config,
    )

    # Should call put_metric_data multiple times
    assert mock_client.put_metric_data.call_count > 0
