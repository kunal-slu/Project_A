"""
Project A job module exports.

This package intentionally uses lazy imports to avoid circular-import failures
and to provide compatibility aliases for legacy module names.
"""

from importlib import import_module
from types import ModuleType

_JOB_MODULES = {
    # Core entry points used by project_a.pipeline.run_pipeline
    "fx_json_to_bronze": "project_a.jobs.fx_json_to_bronze",
    "snowflake_to_bronze": "project_a.jobs.snowflake_to_bronze",
    "crm_to_bronze": "project_a.jobs.crm_to_bronze",
    "redshift_to_bronze": "project_a.jobs.redshift_to_bronze",
    "kafka_csv_to_bronze": "project_a.jobs.kafka_csv_to_bronze",
    "bronze_to_silver": "project_a.jobs.bronze_to_silver",
    "dq_silver_gate": "project_a.jobs.dq_silver_gate",
    "silver_to_gold": "project_a.jobs.silver_to_gold",
    "dq_gold_gate": "project_a.jobs.dq_gold_gate",
    "publish_gold_to_redshift": "project_a.jobs.publish_gold_to_redshift",
    "publish_gold_to_snowflake": "project_a.jobs.publish_gold_to_snowflake",
    # Compatibility aliases expected by existing tests/scripts
    "salesforce_to_bronze": "project_a.jobs.salesforce_to_bronze",
    "kafka_orders_stream": "project_a.jobs.kafka_orders_stream",
    "contacts_silver": "project_a.jobs.contacts_silver",
}

__all__ = sorted(_JOB_MODULES)


def __getattr__(name: str) -> ModuleType:
    """Lazily import job modules on first access."""
    target = _JOB_MODULES.get(name)
    if target is None:
        raise AttributeError(f"module 'project_a.jobs' has no attribute {name!r}")

    module = import_module(target)
    globals()[name] = module
    return module
