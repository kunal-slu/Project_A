"""
Project A pipeline package exports.
"""

from importlib import import_module
from types import ModuleType

_PIPELINE_MODULES = {
    "bronze_to_silver": "project_a.jobs.bronze_to_silver",
    "silver_to_gold": "project_a.jobs.silver_to_gold",
    "run_pipeline": "project_a.pipeline.run_pipeline",
}

__all__ = sorted(_PIPELINE_MODULES)


def __getattr__(name: str) -> ModuleType:
    """Lazily import pipeline modules and compatibility wrappers."""
    target = _PIPELINE_MODULES.get(name)
    if target is None:
        raise AttributeError(f"module 'project_a.pipeline' has no attribute {name!r}")

    module = import_module(target)
    globals()[name] = module
    return module
