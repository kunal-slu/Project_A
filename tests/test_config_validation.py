"""
Config validation tests.

Ensures all config files have required keys and valid values.
"""

import pytest
import yaml
from pathlib import Path
from pyspark_interview_project.utils.config import load_conf


def get_config_files():
    """Get all config YAML files."""
    config_dir = Path("config")
    return list(config_dir.glob("*.yaml")) + list(config_dir.glob("*.yml"))


@pytest.mark.parametrize("config_file", get_config_files())
def test_config_loads_without_error(config_file):
    """Test that config file loads without errors."""
    try:
        config = load_conf(str(config_file))
        assert isinstance(config, dict), f"Config {config_file} must be a dict"
    except Exception as e:
        pytest.fail(f"Failed to load config {config_file}: {e}")


@pytest.mark.parametrize("config_file", get_config_files())
def test_config_has_lake_bucket(config_file):
    """Test that config has data_lake.bucket or s3.bucket."""
    config = load_conf(str(config_file))
    
    has_lake_bucket = config.get("data_lake", {}).get("bucket") is not None
    has_s3_bucket = config.get("s3", {}).get("bucket") is not None
    
    assert has_lake_bucket or has_s3_bucket, \
        f"Config {config_file} must have data_lake.bucket or s3.bucket"


@pytest.mark.parametrize("config_file", get_config_files())
def test_config_has_region(config_file):
    """Test that config has AWS region."""
    config = load_conf(str(config_file))
    
    has_aws_region = config.get("aws", {}).get("region") is not None
    has_region = config.get("region") is not None
    
    assert has_aws_region or has_region, \
        f"Config {config_file} must have aws.region or region"


def test_local_config_has_all_paths():
    """Test that local.yaml has all required paths."""
    config = load_conf("config/local.yaml")
    
    required_paths = [
        "data_lake.bronze_path",
        "data_lake.silver_path",
        "data_lake.gold_path"
    ]
    
    for path in required_paths:
        keys = path.split(".")
        value = config
        for key in keys:
            assert key in value, f"Missing {path} in local.yaml"
            value = value[key]
        assert value is not None, f"{path} is None in local.yaml"


def test_local_config_has_ingestion_settings():
    """Test that local.yaml has ingestion configuration."""
    config = load_conf("config/local.yaml")
    
    assert "ingestion" in config, "Missing ingestion section"
    assert "mode" in config["ingestion"], "Missing ingestion.mode"
    assert config["ingestion"]["mode"] in ["schema_on_write", "schema_on_read"], \
        "ingestion.mode must be schema_on_write or schema_on_read"
    
    assert "on_unknown_column" in config["ingestion"], "Missing ingestion.on_unknown_column"
    assert config["ingestion"]["on_unknown_column"] in ["quarantine", "drop", "fail"], \
        "on_unknown_column must be quarantine, drop, or fail"


def test_config_schema_validity():
    """Test that config files have valid YAML syntax."""
    config_dir = Path("config")
    
    for config_file in config_dir.glob("*.yaml"):
        try:
            with open(config_file, 'r') as f:
                yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"Invalid YAML in {config_file}: {e}")


def test_prod_config_secrets_references():
    """Test that prod.yaml references Secrets Manager (not hardcoded)."""
    try:
        config = load_conf("config/prod.yaml")
        
        # Check that sensitive values are not hardcoded
        snowflake_config = config.get("data_sources", {}).get("snowflake", {})
        
        # Should use secret references, not actual passwords
        if "password" in snowflake_config:
            password = snowflake_config["password"]
            assert password.startswith("${") or "secret" in str(password).lower(), \
                "snowflake.password should reference secret, not be hardcoded"
    except FileNotFoundError:
        pytest.skip("prod.yaml not found (expected for local dev)")


def test_config_spark_settings():
    """Test that configs have valid Spark settings."""
    config_files = ["config/local.yaml"]
    
    for config_file in config_files:
        config = load_conf(config_file)
        
        if "spark" in config:
            assert "master" in config["spark"] or "environment" in config, \
                "Spark config should have master or environment"
            
            if "shuffle_partitions" in config.get("spark", {}):
                partitions = config["spark"]["shuffle_partitions"]
                assert isinstance(partitions, int) and partitions > 0, \
                    "shuffle_partitions must be positive integer"

