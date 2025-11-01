"""
Tests for Great Expectations runner.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from pyspark_interview_project.dq.great_expectations_runner import GreatExpectationsRunner, run_dq_checkpoint


def test_ge_runner_init():
    """Test GE runner initialization."""
    runner = GreatExpectationsRunner()
    assert runner.context_root is not None
    assert runner.context is None


@patch('pyspark_interview_project.dq.great_expectations_runner.Path')
@patch('pyspark_interview_project.dq.great_expectations_runner.DataContext')
def test_ge_runner_init_context_success(mock_data_context, mock_path):
    """Test successful GE context initialization."""
    mock_path_instance = Mock()
    mock_path_instance.exists.return_value = True
    mock_path.return_value = mock_path_instance
    
    mock_context = Mock()
    mock_data_context.return_value = mock_context
    
    runner = GreatExpectationsRunner()
    runner.init_context()
    
    assert runner.context is not None


@patch('pyspark_interview_project.dq.great_expectations_runner.Path')
def test_ge_runner_init_context_missing(mock_path):
    """Test GE context initialization with missing context."""
    mock_path_instance = Mock()
    mock_path_instance.exists.return_value = False
    mock_path.return_value = mock_path_instance
    
    runner = GreatExpectationsRunner()
    runner.init_context()
    
    # Context should be None if GE not configured
    assert runner.context is None


@patch('pyspark_interview_project.dq.great_expectations_runner.DataContext')
def test_ge_runner_run_checkpoint_no_context():
    """Test checkpoint run when context not initialized."""
    runner = GreatExpectationsRunner()
    runner.context = None
    
    result = runner.run_checkpoint("test_checkpoint", fail_on_error=False)
    
    assert result["skipped"] is True
    assert result["success"] is True


@patch('pyspark_interview_project.dq.great_expectations_runner.DataContext')
def test_ge_runner_run_checkpoint_success():
    """Test successful checkpoint run."""
    runner = GreatExpectationsRunner()
    mock_context = Mock()
    mock_checkpoint = Mock()
    mock_context.get_checkpoint.return_value = mock_checkpoint
    mock_checkpoint.run.return_value = {
        "success": True,
        "run_results": {}
    }
    runner.context = mock_context
    
    result = runner.run_checkpoint("test_checkpoint", fail_on_error=False)
    
    assert result["success"] is True


@patch('pyspark_interview_project.dq.great_expectations_runner.DataContext')
def test_ge_runner_run_checkpoint_failure():
    """Test checkpoint run with failure."""
    runner = GreatExpectationsRunner()
    mock_context = Mock()
    mock_checkpoint = Mock()
    mock_context.get_checkpoint.return_value = mock_checkpoint
    mock_checkpoint.run.return_value = {
        "success": False,
        "run_results": {}
    }
    runner.context = mock_context
    
    with pytest.raises(RuntimeError):
        runner.run_checkpoint("test_checkpoint", fail_on_error=True)


def test_run_dq_checkpoint_convenience():
    """Test convenience function."""
    with patch('pyspark_interview_project.dq.great_expectations_runner.GreatExpectationsRunner') as mock_runner_class:
        mock_runner = Mock()
        mock_runner.init_context.return_value = None
        mock_runner.run_checkpoint.return_value = {"success": True}
        mock_runner_class.return_value = mock_runner
        
        result = run_dq_checkpoint("test_checkpoint", fail_on_error=False)
        
        assert result["success"] is True
        mock_runner.init_context.assert_called_once()
        mock_runner.run_checkpoint.assert_called_once()


