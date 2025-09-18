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
from typing import Any, Dict

import yaml

# Matches ${ENV:VAR} or ${SECRET:scope:key}
_SECRET_PATTERN = re.compile(r"\$\{(ENV|SECRET):([^}:]+)(?::([^}]+))?\}")


class ConfigLoader:
    """Configuration loader class that provides config loading and validation."""

    def __init__(self, config_path: str | None = None, env: str | None = None):
        self.config_path = config_path
        self.env = env
        self.config = None

    def load_config(self) -> Dict[str, Any]:
        """Load and resolve configuration."""
        self.config = load_config_resolved(self.config_path, self.env)
        return self.config

    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration structure and required fields."""
        try:
            required_sections = ['unity_catalog', 'azure_security', 'disaster_recovery']

            for section in required_sections:
                if section not in config:
                    return False
                if not isinstance(config[section], dict):
                    return False

            # Validate unity_catalog
            uc_config = config['unity_catalog']
            if not uc_config.get('catalog_name') or not uc_config.get('metastore_id'):
                return False

            # Validate azure_security
            sec_config = config['azure_security']
            if not sec_config.get('subscription_id') or not sec_config.get('resource_group'):
                return False

            # Validate disaster_recovery
            dr_config = config['disaster_recovery']
            if not dr_config.get('primary_region') or not dr_config.get('secondary_region'):
                return False

            return True

        except Exception:
            return False

    def get_config(self) -> Dict[str, Any]:
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


def _resolve_value(value: Any, dbutils) -> Any:
    if not isinstance(value, str):
        return value

    def replace(match: re.Match) -> str:
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

    return _SECRET_PATTERN.sub(replace, value)


def _resolve_secrets(obj: Any, dbutils) -> Any:
    if isinstance(obj, dict):
        return {k: _resolve_secrets(v, dbutils) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_secrets(v, dbutils) for v in obj]
    return _resolve_value(obj, dbutils)


def load_config_resolved(
    config_path: str | None = None, env: str | None = None
) -> Dict[str, Any]:
    """
    Load config from a path, or pick by environment if path not provided.
    Environment resolution order:
      - explicit env arg
      - APP_ENV environment variable (e.g., dev|staging|prod|azure-prod)
      - default to dev
    """
    if config_path is None:
        env = env or os.environ.get("APP_ENV", "dev")
        candidates = {
            "dev": "config/config-dev.yaml",
            "staging": "config/config-staging.yaml",
            "prod": "config/config-prod.yaml",
            "azure-dev": "config/config-azure-dev.yaml",
            "azure-staging": "config/config-azure-staging.yaml",
            "azure-prod": "config/config-azure-prod.yaml",
        }
        config_path = candidates.get(env, "config/config-dev.yaml")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    dbutils = _get_dbutils()
    return _resolve_secrets(cfg, dbutils)
