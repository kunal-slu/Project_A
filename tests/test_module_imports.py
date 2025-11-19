"""
Test module imports to ensure correct package structure.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_project_a_imports():
    """Test that project_a package imports correctly."""
    try:
        import project_a
        assert project_a.__name__ == "project_a"
    except ImportError as e:
        pytest.fail(f"Failed to import project_a: {e}")


def test_pipeline_imports():
    """Test that pipeline modules import correctly."""
    try:
        from project_a.pipeline import bronze_to_silver
        from project_a.pipeline import silver_to_gold
        assert bronze_to_silver is not None
        assert silver_to_gold is not None
    except ImportError as e:
        pytest.fail(f"Failed to import pipeline modules: {e}")


def test_utils_imports():
    """Test that utility modules import correctly."""
    try:
        from project_a.utils import config
        from project_a.utils import spark
        from project_a.utils import logging
        assert config is not None
        assert spark is not None
        assert logging is not None
    except ImportError as e:
        pytest.fail(f"Failed to import utility modules: {e}")


def test_jobs_imports():
    """Test that job modules import correctly."""
    try:
        from project_a.jobs import kafka_orders_stream
        from project_a.jobs import salesforce_to_bronze
        from project_a.jobs import snowflake_to_bronze
        assert kafka_orders_stream is not None
        assert salesforce_to_bronze is not None
        assert snowflake_to_bronze is not None
    except ImportError as e:
        pytest.fail(f"Failed to import job modules: {e}")


def test_no_src_prefix_imports():
    """Test that imports don't use src.* prefix."""
    # This test ensures we're not using src.* imports
    import project_a.utils.config as config_module
    
    # Check that the module path doesn't contain 'src.'
    assert 'src.' not in config_module.__name__
    assert config_module.__name__.startswith('project_a')
