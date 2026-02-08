"""
Pytest configuration for aws/ tests.
"""

from pathlib import Path

import pytest


@pytest.fixture
def aws_root():
    """Return aws/ directory path."""
    return Path(__file__).parent.parent


@pytest.fixture
def terraform_dir(aws_root):
    """Return terraform directory path."""
    return aws_root / "terraform"


@pytest.fixture
def jobs_dir(aws_root):
    """Return jobs directory path."""
    return aws_root / "jobs"


@pytest.fixture
def config_dir(aws_root):
    """Return config directory path."""
    return aws_root / "config"
