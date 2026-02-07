"""
Configuration Loader

Load and resolve configuration per environment with secure secret interpolation.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Optional, Dict

import boto3
import yaml
from botocore.exceptions import ClientError

# Matches ${ENV:VAR} or ${SECRET:scope:key} or ${paths.bronze_root} style variable references
_SECRET_PATTERN = re.compile(r"\$\{(ENV|SECRET):([^}:]+)(?::([^}]+))?\}")
_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve_value(value: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Resolve secret and variable references in a value."""
    if not isinstance(value, str):
        return value

    # First resolve secrets (ENV:)
    def replace_secret(match: re.Match) -> str:
        kind = match.group(1)
        p1 = match.group(2)
        if kind == "ENV":
            return os.environ.get(p1, "")
        return match.group(0)

    value = _SECRET_PATTERN.sub(replace_secret, value)

    # Then resolve config variable references (${paths.bronze_root})
    def replace_var(match: re.Match) -> str:
        var_path = match.group(1)
        if config:
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


def _resolve_secrets(obj: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Recursively resolve secrets and variables in config."""
    if isinstance(obj, dict):
        resolved = {k: _resolve_secrets(v, config) for k, v in obj.items()}
        if config is None:
            config = resolved
        return resolved
    if isinstance(obj, list):
        return [_resolve_secrets(v, config) for v in obj]
    return _resolve_value(obj, config)


def load_config(config_path: str, env: str = "dev") -> Dict[str, Any]:
    """
    Load configuration from file (local or S3).

    Args:
        config_path: Path to config file (local or s3://...)
        env: Environment name (dev/staging/prod)

    Returns:
        Configuration dictionary
    """
    # Handle S3 paths
    if config_path.startswith("s3://"):
        s3 = boto3.client("s3")
        path_parts = config_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read().decode("utf-8")
            cfg = yaml.safe_load(content)
        except ClientError as e:
            raise ValueError(f"Failed to load config from S3: {e}")
    else:
        # Local file
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_file) as f:
            cfg = yaml.safe_load(f)

    # Resolve secrets and variables
    cfg = _resolve_secrets(cfg, cfg)
    return cfg


def load_config_resolved(config_path: Optional[str] = None, env: Optional[str] = None) -> Dict[str, Any]:
    """
    Load config from a path, or pick by environment if path not provided.

    Args:
        config_path: Path to config file (local or s3://...)
        env: Environment name (dev/staging/prod)

    Returns:
        Configuration dictionary
    """
    if config_path is None:
        env = env or os.environ.get("APP_ENV") or os.environ.get("ENV", "dev")
        candidates = {
            "dev": "config/dev.yaml",
            "local": "local/config/local.yaml",
            "staging": "config/staging.yaml",
            "prod": "config/prod.yaml",
        }
        config_path = candidates.get(env, "config/dev.yaml")

    return load_config(config_path, env or "dev")
