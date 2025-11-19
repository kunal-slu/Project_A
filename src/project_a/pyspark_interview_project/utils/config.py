"""
Configuration loader with YAML and environment variable support.
"""

import os
import yaml
import logging
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


def load_conf(env_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file and overlay with environment variables.
    
    Args:
        env_path: Path to configuration file
        
    Returns:
        Resolved configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If required configuration keys are missing
    """
    config_path = Path(env_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {env_path}")
    
    # Load YAML configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Overlay environment variables
    config = _overlay_env_vars(config)
    
    # Validate required keys
    _validate_config(config)
    
    logger.info(f"Configuration loaded from {env_path}")
    return config


def _overlay_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """Overlay environment variables on configuration and resolve template variables."""
    def _resolve_vars(obj):
        if isinstance(obj, dict):
            return {k: _resolve_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_resolve_vars(v) for v in obj]
        elif isinstance(obj, str):
            # First resolve template variables like ${project}-${environment}
            obj = _resolve_template_variables(obj, config)
            # Then resolve environment variables like ${ENV_VAR}
            if obj.startswith("${") and obj.endswith("}") and "." not in obj[2:-1]:
                env_var = obj[2:-1]  # Remove ${ and }
                return os.environ.get(env_var, obj)
            return obj
        else:
            return obj
    
    return _resolve_vars(config)


def _resolve_template_variables(value: str, config: Dict[str, Any]) -> str:
    """Resolve template variables like ${project}-${environment} in string values."""
    import re
    
    def replace_var(match):
        var_path = match.group(1)
        if '.' in var_path:
            # Handle nested variables like s3.data_lake_bucket
            parts = var_path.split('.')
            current = config
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return match.group(0)  # Return original if not found
            return str(current)
        else:
            # Handle simple variables
            if var_path in config:
                return str(config[var_path])
            return match.group(0)  # Return original if not found
    
    return re.sub(r'\$\{([^}]+)\}', replace_var, value)


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate required configuration keys."""
    required_keys = [
        "env",
        "lake.bronze_path",
        "lake.silver_path", 
        "lake.gold_path",
        "runtime.shuffle_partitions",
        "runtime.app_name"
    ]
    
    missing_keys = []
    for key in required_keys:
        if not _get_nested_value(config, key):
            missing_keys.append(key)
    
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {missing_keys}")


def _get_nested_value(config: Dict[str, Any], key: str) -> Any:
    """Get nested value from configuration using dot notation."""
    keys = key.split('.')
    value = config
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return None
    return value
