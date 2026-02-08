"""
Compatibility shim so that `import pyspark_interview_project`
works even though the real package lives under `project_a.pyspark_interview_project`.

This allows both import styles to work:
- `from pyspark_interview_project.utils.spark_session import build_spark` (legacy)
- `from project_a.pyspark_interview_project.utils.spark_session import build_spark` (canonical)
"""

import sys

# Import everything from the real package
from project_a.pyspark_interview_project import *  # noqa: F401,F403

# Make submodules accessible via pyspark_interview_project.*
# This ensures imports like `from pyspark_interview_project.monitoring import ...` work
# Import submodules gracefully - some may not exist in all versions
_submodules = {}
_submodule_names = [
    "monitoring",
    "utils",
    "extract",
    "transform",
    "dq",
    "jobs",
    "pipeline",
    "metrics",
    "lineage",
    "io",
    "config",
    "common",
    "dr",
    "validation",
]

for submodule_name in _submodule_names:
    try:
        submodule = __import__(
            f"project_a.pyspark_interview_project.{submodule_name}", fromlist=[submodule_name]
        )
        _submodules[submodule_name] = submodule
        # Make it available at package level
        globals()[submodule_name] = submodule
        # Register in sys.modules for direct imports
        sys.modules[f"pyspark_interview_project.{submodule_name}"] = submodule
    except ImportError:
        # Submodule doesn't exist, skip it
        pass

# Expose submodules at package level
__all__ = list(_submodules.keys()) if _submodules else []

# Register this module as 'pyspark_interview_project' in sys.modules
# This ensures that imports like `import pyspark_interview_project` resolve to this module
sys.modules["pyspark_interview_project"] = sys.modules[__name__]
