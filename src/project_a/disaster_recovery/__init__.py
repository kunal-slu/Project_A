"""Disaster Recovery Module."""

try:
    from .plans import (
        BackupManager,
        BackupStrategy,
        RecoveryManager,
        get_backup_manager,
        get_recovery_manager,
    )
except Exception:  # pragma: no cover - compatibility fallback
    BackupManager = None
    BackupStrategy = None
    RecoveryManager = None

    def get_backup_manager(*_args, **_kwargs):
        return None

    def get_recovery_manager(*_args, **_kwargs):
        return None

from project_a.legacy.disaster_recovery import DisasterRecoveryExecutor

__all__ = [
    "BackupManager",
    "RecoveryManager",
    "BackupStrategy",
    "get_backup_manager",
    "get_recovery_manager",
    "DisasterRecoveryExecutor",
]
