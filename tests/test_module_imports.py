"""
Test module imports to ensure correct package structure.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_pyspark_interview_project_imports():
    """Test that pyspark_interview_project package imports correctly."""
    try:
        import pyspark_interview_project
        assert pyspark_interview_project.__name__ == "pyspark_interview_project"
    except ImportError as e:
        pytest.fail(f"Failed to import pyspark_interview_project: {e}")


def test_pipeline_imports():
    """Test that pipeline modules import correctly."""
    try:
        from pyspark_interview_project.pipeline import bronze_to_silver
        from pyspark_interview_project.pipeline import silver_to_gold
        assert bronze_to_silver is not None
        assert silver_to_gold is not None
    except ImportError as e:
        pytest.fail(f"Failed to import pipeline modules: {e}")


def test_utils_imports():
    """Test that utility modules import correctly."""
    try:
        from pyspark_interview_project.utils import config
        from pyspark_interview_project.utils import spark
        from pyspark_interview_project.utils import logging
        assert config is not None
        assert spark is not None
        assert logging is not None
    except ImportError as e:
        pytest.fail(f"Failed to import utility modules: {e}")


def test_jobs_imports():
    """Test that job modules import correctly."""
    try:
        from pyspark_interview_project.jobs import kafka_orders_stream
        from pyspark_interview_project.jobs import salesforce_to_bronze
        from pyspark_interview_project.jobs import snowflake_to_bronze
        assert kafka_orders_stream is not None
        assert salesforce_to_bronze is not None
        assert snowflake_to_bronze is not None
    except ImportError as e:
        pytest.fail(f"Failed to import job modules: {e}")


def test_no_src_prefix_imports():
    """Test that imports don't use src.* prefix."""
    # This test ensures we're not using src.* imports
    import pyspark_interview_project.utils.config as config_module
    
    # Check that the module path doesn't contain 'src.'
    assert 'src.' not in config_module.__name__
    assert config_module.__name__.startswith('pyspark_interview_project')
