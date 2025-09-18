"""
Test the new pipeline components integration.
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark_interview_project.pipeline_stages.bronze_to_silver import run as b2s_run
from pyspark_interview_project.pipeline_stages.silver_to_gold import run as s2g_run
from pyspark_interview_project.logging_setup import get_logger, new_correlation_id
from pyspark_interview_project.config.paths import BRONZE, SILVER, GOLD

class TestPipelineComponents:
    """Test the new pipeline components."""

    def setup_method(self):
        """Set up test environment."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.original_env = os.environ.copy()

        # Set test environment variables
        os.environ["DATA_ROOT"] = str(self.test_dir)
        os.environ["APP_ENV"] = "test"

        # Create test directories
        (self.test_dir / "bronze" / "returns_raw").mkdir(parents=True, exist_ok=True)
        (self.test_dir / "silver").mkdir(parents=True, exist_ok=True)
        (self.test_dir / "gold").mkdir(parents=True, exist_ok=True)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)
        os.environ.clear()
        os.environ.update(self.original_env)

    def test_logging_setup(self):
        """Test logging setup functionality."""
        logger = get_logger("test_logger")
        assert logger.name == "test_logger"

        # Test correlation ID
        cid = new_correlation_id()
        assert cid is not None
        assert os.getenv("CORRELATION_ID") == cid

    def test_paths_configuration(self):
        """Test path configuration."""
        assert str(BRONZE).endswith("bronze")
        assert str(SILVER).endswith("silver")
        assert str(GOLD).endswith("gold")

    @patch('pyspark_interview_project.pipeline_stages.bronze_to_silver.build_spark')
    def test_bronze_to_silver_import(self, mock_build_spark):
        """Test that bronze_to_silver module can be imported and run function exists."""
        mock_spark = Mock()
        mock_build_spark.return_value = mock_spark

        # Test that the function exists and can be called
        assert callable(b2s_run)

    @patch('pyspark_interview_project.pipeline_stages.silver_to_gold.build_spark')
    def test_silver_to_gold_import(self, mock_build_spark):
        """Test that silver_to_gold module can be imported and run function exists."""
        mock_spark = Mock()
        mock_build_spark.return_value = mock_spark

        # Test that the function exists and can be called
        assert callable(s2g_run)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
