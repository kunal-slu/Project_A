"""
Unified Configuration Loader for Project_A

Handles loading YAML configs from local files or S3, with environment variable
and secret resolution. Works for both local Spark and AWS EMR execution.
"""

import os
import re
import logging
from pathlib import Path
from typing import Any, Optional, Dict

import yaml

logger = logging.getLogger(__name__)

# Matches ${ENV:VAR} or ${paths.bronze_root} style variable references
_ENV_PATTERN = re.compile(r"\$\{ENV:([^}]+)\}")
_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve_env_vars(value: str) -> str:
    """Resolve ${ENV:VAR_NAME} patterns."""
    def replace_env(match: re.Match) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))
    return _ENV_PATTERN.sub(replace_env, value)


def _resolve_config_vars(value: str, config: Dict[str, Any]) -> str:
    """Resolve ${paths.bronze_root} style variable references."""
    def replace_var(match: re.Match) -> str:
        var_path = match.group(1)
        try:
            parts = var_path.split(".")
            result = config
            for part in parts:
                if isinstance(result, dict):
                    result = result.get(part)
                else:
                    return match.group(0)
            if result is not None:
                return str(result)
        except Exception:
            pass
        return match.group(0)
    return _VAR_PATTERN.sub(replace_var, value)


def _resolve_value(value: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Resolve environment variables and config references in a value."""
    if not isinstance(value, str):
        return value
    
    # First resolve environment variables
    value = _resolve_env_vars(value)
    
    # Then resolve config variable references (requires config to be loaded)
    if config:
        value = _resolve_config_vars(value, config)
    
    return value


def _resolve_secrets(obj: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Recursively resolve secrets and variables in config."""
    if isinstance(obj, dict):
        resolved = {k: _resolve_secrets(v, config) for k, v in obj.items()}
        # Update config reference for variable substitution
        if config is None:
            config = resolved
        # Second pass: resolve config variable references now that config is loaded
        resolved = {k: _resolve_value(v, resolved) for k, v in resolved.items()}
        return resolved
    if isinstance(obj, list):
        return [_resolve_secrets(v, config) for v in obj]
    return _resolve_value(obj, config)


def load_config_from_s3(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from S3 using boto3.
    
    Args:
        config_path: S3 path (s3://bucket/key)
        
    Returns:
        Configuration dictionary
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        s3 = boto3.client("s3")
        path_parts = config_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""
        
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
        cfg = yaml.safe_load(content)
        return cfg
    except ImportError:
        logger.warning("boto3 not available, trying Spark-based S3 read")
        # Fallback: use Spark to read from S3
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("config_loader").getOrCreate()
            config_lines = spark.sparkContext.textFile(config_path).collect()
            config_content = "\n".join(config_lines)
            cfg = yaml.safe_load(config_content)
            spark.stop()
            return cfg
        except Exception as e:
            raise ValueError(f"Failed to load config from S3: {e}")
    except Exception as e:
        raise ValueError(f"Failed to load config from S3: {e}")


def load_config_resolved(config_path: Optional[str] = None, env: Optional[str] = None) -> Dict[str, Any]:
    """
    Load and resolve configuration from file (local or S3).
    
    Args:
        config_path: Path to config file (local or s3://...)
        env: Environment name (dev/staging/prod/local)
        
    Returns:
        Resolved configuration dictionary
    """
    # Determine config path
    if config_path is None:
        env = env or os.environ.get("APP_ENV") or os.environ.get("ENV", "dev")
        candidates = {
            "dev": "config/dev.yaml",
            "local": "local/config/local.yaml",
            "staging": "config/staging.yaml",
            "prod": "config/prod.yaml",
        }
        config_path = candidates.get(env, "config/dev.yaml")
    
    # Load config
    if config_path.startswith("s3://"):
        logger.info(f"Loading config from S3: {config_path}")
        cfg = load_config_from_s3(config_path)
    else:
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        logger.info(f"Loading config from local file: {config_path}")
        with open(config_file) as f:
            cfg = yaml.safe_load(f)
    
    # Resolve secrets and variables (two-pass: first load, then resolve vars)
    cfg = _resolve_secrets(cfg, cfg)
    
    return cfg


def get_config_value(config: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """
    Get a config value using dot-notation path.
    
    Args:
        config: Configuration dictionary
        key_path: Dot-notation path (e.g., "paths.bronze_root")
        default: Default value if not found
        
    Returns:
        Config value or default
    """
    parts = key_path.split(".")
    result = config
    for part in parts:
        if isinstance(result, dict):
            result = result.get(part)
            if result is None:
                return default
        else:
            return default
    return result if result is not None else default

