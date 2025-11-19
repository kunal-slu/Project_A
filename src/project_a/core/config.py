"""
Project Configuration Management

Loads and validates configuration from YAML files (local or S3).
Provides typed access to all configuration values.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
import yaml
from botocore.exceptions import ClientError


class ProjectConfig:
    """
    Centralized configuration management for Project A.
    
    Handles:
    - Loading from local files or S3
    - Environment variable interpolation (${ENV:VAR})
    - Config variable interpolation (${paths.bronze_root})
    - Type-safe access to config sections
    """
    
    def __init__(self, config_path: str, env: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to config file (local or s3://...)
            env: Environment name (dev/staging/prod) - optional override
        """
        self.config_path = config_path
        self._raw_config: Dict[str, Any] = {}
        self._config: Dict[str, Any] = {}
        self._env = env
        
        self._load()
        self._resolve()
    
    def _load(self) -> None:
        """Load config from file (local or S3)."""
        if self.config_path.startswith("s3://"):
            s3 = boto3.client("s3")
            path_parts = self.config_path.replace("s3://", "").split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ""
            
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                content = obj["Body"].read().decode("utf-8")
                self._raw_config = yaml.safe_load(content)
            except ClientError as e:
                raise ValueError(f"Failed to load config from S3: {e}")
        else:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {self.config_path}")
            
            with open(config_file) as f:
                self._raw_config = yaml.safe_load(f)
    
    def _resolve_value(self, value: Any) -> Any:
        """Resolve secret and variable references in a value."""
        if not isinstance(value, str):
            return value
        
        # Resolve environment variables: ${ENV:VAR}
        def replace_env(match: re.Match) -> str:
            var_name = match.group(1)
            return os.environ.get(var_name, "")
        
        value = re.sub(r"\$\{ENV:([^}]+)\}", replace_env, value)
        
        # Resolve config variables: ${paths.bronze_root}
        def replace_var(match: re.Match) -> str:
            var_path = match.group(1)
            try:
                parts = var_path.split(".")
                result = self._config
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
        
        return re.sub(r"\$\{([^}]+)\}", replace_var, value)
    
    def _resolve(self) -> None:
        """Recursively resolve all variable references."""
        def resolve_recursive(obj: Any) -> Any:
            if isinstance(obj, dict):
                resolved = {k: resolve_recursive(v) for k, v in obj.items()}
                # Update _config as we go for variable interpolation
                self._config.update(resolved)
                return resolved
            if isinstance(obj, list):
                return [resolve_recursive(v) for v in obj]
            return self._resolve_value(obj)
        
        self._config = resolve_recursive(self._raw_config)
    
    @property
    def environment(self) -> str:
        """Get environment name (dev/staging/prod)."""
        return self._config.get("environment") or self._config.get("env") or "dev"
    
    @property
    def project_name(self) -> str:
        """Get project name."""
        return self._config.get("project_name", "project-a")
    
    @property
    def paths(self) -> Dict[str, str]:
        """Get path configurations."""
        return self._config.get("paths", {})
    
    @property
    def sources(self) -> Dict[str, Any]:
        """Get source configurations."""
        return self._config.get("sources", {})
    
    @property
    def tables(self) -> Dict[str, Dict[str, str]]:
        """Get table name definitions."""
        return self._config.get("tables", {})
    
    @property
    def aws(self) -> Dict[str, Any]:
        """Get AWS configuration."""
        return self._config.get("aws", {})
    
    @property
    def buckets(self) -> Dict[str, str]:
        """Get S3 bucket names."""
        return self._config.get("buckets", {})
    
    @property
    def glue(self) -> Dict[str, str]:
        """Get Glue database names."""
        return self._config.get("glue", {})
    
    @property
    def emr(self) -> Dict[str, Any]:
        """Get EMR configuration."""
        return self._config.get("emr", {})
    
    @property
    def kafka(self) -> Dict[str, Any]:
        """Get Kafka configuration."""
        return self._config.get("sources", {}).get("kafka", {})
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value by key (supports dot notation)."""
        parts = key.split(".")
        result = self._config
        for part in parts:
            if isinstance(result, dict):
                result = result.get(part)
            else:
                return default
            if result is None:
                return default
        return result
    
    def is_local(self) -> bool:
        """Check if running in local environment."""
        env = self.environment.lower()
        return env in ("local", "dev_local")
    
    def is_aws(self) -> bool:
        """Check if running on AWS (EMR)."""
        env = self.environment.lower()
        return env in ("emr", "aws", "prod", "staging")

