"""
config_loader.py
Load and resolve configuration per environment with secure secret interpolation.

Secrets syntax supported in YAML values:
- ${ENV:VAR_NAME} -> from environment variable
- ${SECRET:scope:key} -> from Databricks secret scope (dbutils.secrets)

Fallbacks: if secret not found, value remains unresolved.
"""

from __future__ import annotations

import os
import re
from typing import Any

# Matches ${ENV:VAR} or ${SECRET:scope:key} or ${paths.bronze_root} style variable references
_SECRET_PATTERN = re.compile(r"\$\{(ENV|SECRET):([^}:]+)(?::([^}]+))?\}")
_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


class ConfigLoader:
    """Configuration loader class that provides config loading and validation."""

    def __init__(self, config_path: str | None = None, env: str | None = None):
        self.config_path = config_path
        self.env = env
        self.config = None

    def load_config(self) -> dict[str, Any]:
        """Load and resolve configuration."""
        self.config = load_config_resolved(self.config_path, self.env)
        return self.config

    def validate_config(self, config: dict[str, Any]) -> bool:
        """Validate configuration structure and required fields."""
        try:
            required_sections = ["unity_catalog", "azure_security", "disaster_recovery"]

            for section in required_sections:
                if section not in config:
                    return False
                if not isinstance(config[section], dict):
                    return False

            # Validate unity_catalog
            uc_config = config["unity_catalog"]
            if not uc_config.get("catalog_name") or not uc_config.get("metastore_id"):
                return False

            # Validate azure_security
            sec_config = config["azure_security"]
            if not sec_config.get("subscription_id") or not sec_config.get("resource_group"):
                return False

            # Validate disaster_recovery
            dr_config = config["disaster_recovery"]
            if not dr_config.get("primary_region") or not dr_config.get("secondary_region"):
                return False

            return True

        except Exception:
            return False

    def get_config(self) -> dict[str, Any]:
        """Get the loaded configuration."""
        if self.config is None:
            self.load_config()
        return self.config


def _get_dbutils():
    try:
        import IPython  # noqa
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils
    except Exception:
        try:
            # Databricks notebooks inject dbutils into global scope
            return globals().get("dbutils")
        except Exception:
            return None


def _resolve_value(value: Any, dbutils, config: dict[str, Any] | None = None) -> Any:
    if not isinstance(value, str):
        return value

    # First resolve secrets (ENV: and SECRET:)
    def replace_secret(match: re.Match) -> str:
        kind = match.group(1)
        p1 = match.group(2)
        p2 = match.group(3)
        if kind == "ENV":
            return os.environ.get(p1, "")
        if kind == "SECRET":
            if dbutils:
                try:
                    return dbutils.secrets.get(scope=p1, key=p2)  # type: ignore
                except Exception:
                    return ""
            return ""
        return match.group(0)

    value = _SECRET_PATTERN.sub(replace_secret, value)

    # Then resolve config variable references (${paths.bronze_root})
    def replace_var(match: re.Match) -> str:
        var_path = match.group(1)
        if config:
            try:
                # Support nested paths like paths.bronze_root
                parts = var_path.split(".")
                result = config
                for part in parts:
                    if isinstance(result, dict):
                        result = result.get(part)
                    else:
                        return match.group(0)  # Return original if path invalid
                if result is not None:
                    return str(result)
            except Exception:
                pass
        return match.group(0)  # Return original if not found

    return _VAR_PATTERN.sub(replace_var, value)


def _resolve_secrets(obj: Any, dbutils, config: dict[str, Any] | None = None) -> Any:
    if isinstance(obj, dict):
        # Pass config to nested resolution
        resolved = {k: _resolve_secrets(v, dbutils, config) for k, v in obj.items()}
        # Update config reference for variable substitution
        if config is None:
            config = resolved
        return resolved
    if isinstance(obj, list):
        return [_resolve_secrets(v, dbutils, config) for v in obj]
    return _resolve_value(obj, dbutils, config)


def load_config_resolved(
    config_path: str | None = None, env: str | None = None
) -> dict[str, Any]:
    """
    Load config from a path, or pick by environment if path not provided.
    Environment resolution order:
      - explicit env arg
      - APP_ENV environment variable (e.g., dev|staging|prod|azure-prod)
      - default to dev
    """
    from project_a.utils.config import load_config_resolved as _load_config_resolved

    # Compatibility: support historic paths used in tests/scripts.
    if config_path == "config/config-dev.yaml":
        config_path = "config/dev.yaml"

    cfg = _load_config_resolved(config_path=config_path, env=env)

    # Backfill legacy aliases expected by old integration tests.
    paths = cfg.get("paths", {})
    cfg.setdefault(
        "input",
        {
            "customer_path": paths.get("bronze_root", "data/input/customers.csv"),
            "orders_path": "data/input/orders.json",
            "products_path": "data/input/products.csv",
        },
    )
    cfg.setdefault(
        "output",
        {
            "bronze_path": paths.get("bronze_root", "data/bronze"),
            "silver_path": paths.get("silver_root", "data/silver"),
            "gold_path": paths.get("gold_root", "data/gold"),
        },
    )
    return cfg
