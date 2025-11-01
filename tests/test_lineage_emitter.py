"""
Tests for lineage emitter.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from pyspark_interview_project.monitoring.lineage_emitter import (
    emit_lineage_event, emit_start, emit_complete, emit_fail
)


@pytest.fixture
def mock_config():
    """Mock configuration dictionary."""
    return {
        "lineage": {
            "enabled": True,
            "url": "http://localhost:5000"
        },
        "environment": "test"
    }


@patch('pyspark_interview_project.monitoring.lineage_emitter.requests.post')
def test_emit_lineage_event_success(mock_post, mock_config):
    """Test successful lineage event emission."""
    mock_post.return_value.raise_for_status = Mock()
    
    result = emit_lineage_event(
        event_type="START",
        job_name="test_job",
        inputs=[{"name": "s3://bucket/input"}],
        outputs=[{"name": "s3://bucket/output"}],
        config=mock_config
    )
    
    assert result is True
    mock_post.assert_called_once()


def test_emit_lineage_event_disabled(mock_config):
    """Test lineage emission when disabled."""
    mock_config["lineage"]["enabled"] = False
    
    result = emit_lineage_event(
        event_type="START",
        job_name="test_job",
        inputs=[],
        outputs=[],
        config=mock_config
    )
    
    assert result is False


def test_emit_lineage_event_no_url(mock_config):
    """Test lineage emission with no URL configured."""
    mock_config["lineage"]["url"] = ""
    
    result = emit_lineage_event(
        event_type="START",
        job_name="test_job",
        inputs=[],
        outputs=[],
        config=mock_config
    )
    
    assert result is False


@patch('pyspark_interview_project.monitoring.lineage_emitter.requests.post')
def test_emit_start(mock_post, mock_config):
    """Test emit_start convenience function."""
    mock_post.return_value.raise_for_status = Mock()
    
    result = emit_start(
        job_name="test_job",
        inputs=[{"name": "input"}],
        outputs=[{"name": "output"}],
        config=mock_config
    )
    
    assert result is True


@patch('pyspark_interview_project.monitoring.lineage_emitter.requests.post')
def test_emit_complete(mock_post, mock_config):
    """Test emit_complete convenience function."""
    mock_post.return_value.raise_for_status = Mock()
    
    result = emit_complete(
        job_name="test_job",
        inputs=[{"name": "input"}],
        outputs=[{"name": "output"}],
        config=mock_config,
        metadata={"rows": 100}
    )
    
    assert result is True


@patch('pyspark_interview_project.monitoring.lineage_emitter.requests.post')
def test_emit_fail(mock_post, mock_config):
    """Test emit_fail convenience function."""
    mock_post.return_value.raise_for_status = Mock()
    
    result = emit_fail(
        job_name="test_job",
        inputs=[{"name": "input"}],
        outputs=[{"name": "output"}],
        config=mock_config,
        error="Test error"
    )
    
    assert result is True


