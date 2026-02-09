"""Legacy compatibility modules (not part of the canonical pipeline)."""

from __future__ import annotations

import warnings

warnings.warn(
    "project_a.legacy contains compatibility and experimental modules. "
    "These are not used by the canonical Project_A pipeline.",
    DeprecationWarning,
    stacklevel=2,
)
