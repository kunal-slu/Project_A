"""
Tests for secrets manager utilities.
"""

import os
from unittest.mock import Mock, patch

import pytest

from project_a.utils.secrets import (
    get_redshift_credentials,
    get_secret_from_manager,
    get_snowflake_credentials,
)


@pytest.fixture
def mock_config():
    """Mock configuration dictionary."""
    return {
        "secrets": {
            "snowflake": {"secret_name": "test/snowflake"},
            "redshift": {"secret_name": "test/redshift"},
        },
        "aws": {"region": "us-east-1"},
        "data_sources": {
            "snowflake": {"account": "test-account", "user": "test_user", "password": "test_pass"},
            "redshift": {"host": "test.redshift.amazonaws.com", "user": "test_user"},
        },
    }


@patch("project_a.utils.secrets.boto3")
def test_get_secret_from_manager_success(mock_boto3, mock_config):
    """Test successful secret retrieval from Secrets Manager."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client
    mock_client.get_secret_value.return_value = {
        "SecretString": '{"user": "test_user", "password": "test_pass"}'
    }

    result = get_secret_from_manager("test/secret")

    assert result["user"] == "test_user"
    assert result["password"] == "test_pass"


@patch("project_a.utils.secrets.boto3")
def test_get_secret_from_manager_not_found(mock_boto3):
    """Test secret retrieval when secret not found."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_secret_value.side_effect = Mock(side_effect=Exception())
    mock_client.exceptions.ClientError = type("ClientError", (Exception,), {})

    # Should fall back to environment variables
    with patch.dict(os.environ, {"SNOWFLAKE_USER": "env_user"}):
        result = get_secret_from_manager("test/secret")
        # Should attempt env fallback
        assert isinstance(result, dict)


@patch("project_a.utils.secrets.get_secret_from_manager")
def test_get_snowflake_credentials_from_secrets(mock_get_secret, mock_config):
    """Test getting Snowflake credentials from Secrets Manager."""
    mock_get_secret.return_value = {
        "account": "test-account",
        "user": "test_user",
        "password": "test_pass",
        "warehouse": "test_warehouse",
    }

    result = get_snowflake_credentials(mock_config)

    assert result["account"] == "test-account"
    assert result["user"] == "test_user"
    mock_get_secret.assert_called_once()


def test_get_snowflake_credentials_from_config(mock_config):
    """Test getting Snowflake credentials from config."""
    mock_config["secrets"]["snowflake"] = {}  # No secret name

    result = get_snowflake_credentials(mock_config)

    assert result["account"] == "test-account"
    assert result["user"] == "test_user"


@patch("project_a.utils.secrets.get_secret_from_manager")
def test_get_redshift_credentials_from_secrets(mock_get_secret, mock_config):
    """Test getting Redshift credentials from Secrets Manager."""
    mock_get_secret.return_value = {
        "host": "test.redshift.amazonaws.com",
        "user": "test_user",
        "password": "test_pass",
    }

    result = get_redshift_credentials(mock_config)

    assert result["host"] == "test.redshift.amazonaws.com"
    assert result["user"] == "test_user"
