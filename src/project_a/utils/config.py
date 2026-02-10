"""
Configuration Loader

Load and resolve configuration per environment with secure secret interpolation.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

# Matches ${ENV:VAR} or ${SECRET:scope:key} or ${paths.bronze_root} style variable references
_SECRET_PATTERN = re.compile(r"\$\{(ENV|SECRET):([^}:]+)(?::([^}]+))?\}")
_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _normalize_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Backfill legacy keys expected by tests and old jobs."""
    cfg = dict(cfg or {})

    aws = dict(cfg.get("aws") or {})
    region = aws.get("region") or cfg.get("region") or "us-east-1"
    aws["region"] = region
    cfg["aws"] = aws
    cfg["region"] = region

    data_lake = dict(cfg.get("data_lake") or {})
    bucket = (
        data_lake.get("bucket")
        or (cfg.get("buckets") or {}).get("lake")
        or aws.get("lake_bucket")
        or (cfg.get("s3") or {}).get("bucket")
        or "local-data-lake"
    )
    data_lake["bucket"] = bucket

    # Path aliases across legacy/new config formats.
    path_map = cfg.get("paths") or {}
    data_lake["bronze_path"] = (
        data_lake.get("bronze_path")
        or path_map.get("bronze_root")
        or path_map.get("bronze")
        or "data/bronze"
    )
    data_lake["silver_path"] = (
        data_lake.get("silver_path")
        or path_map.get("silver_root")
        or path_map.get("silver")
        or "data/silver"
    )
    data_lake["gold_path"] = (
        data_lake.get("gold_path")
        or path_map.get("gold_root")
        or path_map.get("gold")
        or "data/gold"
    )
    cfg["data_lake"] = data_lake

    ingestion = dict(cfg.get("ingestion") or {})
    ingestion.setdefault("mode", "schema_on_write")
    ingestion.setdefault("on_unknown_column", "quarantine")
    cfg["ingestion"] = ingestion

    spark_cfg = dict(cfg.get("spark") or {})
    spark_cfg.setdefault("master", "local[*]")
    cfg["spark"] = spark_cfg

    return cfg


def _resolve_value(value: Any, config: dict[str, Any] | None = None) -> Any:
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
    # and short env forms (${VAR}, ${VAR:-default}).
    def replace_var(match: re.Match) -> str:
        var_path = match.group(1)

        # Support bash-like default expansion.
        if ":-" in var_path:
            var_name, default = var_path.split(":-", 1)
            return os.environ.get(var_name, default)

        # Keep legacy explicit tokens untouched in this phase.
        if var_path.startswith(("ENV:", "SECRET:")):
            return match.group(0)

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

        # Plain ${VAR} -> environment variable lookup.
        if var_path in os.environ:
            return os.environ[var_path]

        return match.group(0)

    return _VAR_PATTERN.sub(replace_var, value)


def _resolve_secrets(obj: Any, config: dict[str, Any] | None = None) -> Any:
    """Recursively resolve secrets and variables in config."""
    if isinstance(obj, dict):
        resolved = {k: _resolve_secrets(v, config) for k, v in obj.items()}
        if config is None:
            config = resolved
        return resolved
    if isinstance(obj, list):
        return [_resolve_secrets(v, config) for v in obj]
    return _resolve_value(obj, config)


def load_config(config_path: str, env: str = "dev") -> dict[str, Any]:
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
            raise ValueError(f"Failed to load config from S3: {e}") from e
    else:
        # Local file
        config_file = Path(config_path)
        if not config_file.exists():
            fallback_candidates = []
            if config_path == "config/local.yaml":
                fallback_candidates.append(Path("local/config/local.yaml"))
            if config_path == "config/dq.yaml":
                fallback_candidates.append(Path("config/dq/dq_rules.yaml"))

            for candidate in fallback_candidates:
                if candidate.exists():
                    config_file = candidate
                    break
            else:
                raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_file) as f:
            cfg = yaml.safe_load(f)

    # Resolve secrets and variables
    cfg = _resolve_secrets(cfg, cfg)
    return _normalize_config(cfg)


def load_conf(config_path: str, env: str = "dev") -> dict[str, Any]:
    """
    Backwards-compatible alias for load_config.

    Args:
        config_path: Path to config file (local or s3://...)
        env: Environment name (dev/staging/prod)

    Returns:
        Configuration dictionary
    """
    return load_config(config_path, env)


def load_config_resolved(config_path: str | None = None, env: str | None = None) -> dict[str, Any]:
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
