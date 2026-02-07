"""
Tests for config utilities.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import yaml
from pathlib import Path

from project_a.utils.config import load_conf


@pytest.fixture
def sample_config_file(tmp_path):
    """Create a sample config file."""
    config_file = tmp_path / "test_config.yaml"
    config_data = {
        "app_name": "test_app",
        "spark": {
            "master": "local[*]"
        }
    }
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)
    return str(config_file)


def test_load_conf_from_file(sample_config_file):
    """Test loading config from file."""
    result = load_conf(sample_config_file)
    
    assert result is not None
    assert result["app_name"] == "test_app"
    assert result["spark"]["master"] == "local[*]"


def test_load_conf_file_not_found():
    """Test loading config when file doesn't exist."""
    with pytest.raises(FileNotFoundError):
        load_conf("nonexistent_config.yaml")


