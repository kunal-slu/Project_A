"""
Tests for config utilities.
"""

import pytest
import yaml

from project_a.utils.config import load_conf


@pytest.fixture
def sample_config_file(tmp_path):
    """Create a sample config file."""
    config_file = tmp_path / "test_config.yaml"
    config_data = {"app_name": "test_app", "spark": {"master": "local[*]"}}
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


def test_load_conf_resolves_short_env_placeholder(monkeypatch, tmp_path):
    """Test ${VAR} interpolation from environment variables."""
    monkeypatch.setenv("PROJECT_A_TEST_ENV", "resolved-value")
    config_file = tmp_path / "env_short.yaml"
    config_file.write_text("token: ${PROJECT_A_TEST_ENV}\n", encoding="utf-8")

    result = load_conf(str(config_file))
    assert result["token"] == "resolved-value"


def test_load_conf_resolves_short_env_default(monkeypatch, tmp_path):
    """Test ${VAR:-default} interpolation fallback."""
    monkeypatch.delenv("PROJECT_A_MISSING_ENV", raising=False)
    config_file = tmp_path / "env_default.yaml"
    config_file.write_text("token: ${PROJECT_A_MISSING_ENV:-fallback}\n", encoding="utf-8")

    result = load_conf(str(config_file))
    assert result["token"] == "fallback"
