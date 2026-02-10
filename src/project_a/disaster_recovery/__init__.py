"""Disaster recovery API surface."""

from __future__ import annotations

from .plans import (
    BackupManager,
    BackupPlan,
    RecoveryManager,
    get_backup_manager,
    get_recovery_manager,
)

BackupStrategy = BackupPlan

try:  # pragma: no cover - optional legacy bridge
    from project_a.legacy.disaster_recovery import DisasterRecoveryExecutor
except Exception:  # pragma: no cover - package may not exist in lean installs
    DisasterRecoveryExecutor = None

__all__ = [
    "BackupManager",
    "RecoveryManager",
    "BackupStrategy",
    "get_backup_manager",
    "get_recovery_manager",
    "DisasterRecoveryExecutor",
]
