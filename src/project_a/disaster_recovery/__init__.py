"""Disaster recovery API surface."""

from __future__ import annotations

from .plans import (
    BackupManager,
    BackupPlan,
    RecoveryManager,
    RecoveryPlan,
    get_backup_manager,
    get_recovery_manager,
)

BackupStrategy = BackupPlan


class DisasterRecoveryExecutor:
    """Thin compatibility executor built on top of `RecoveryManager`."""

    def __init__(self, spark=None, config: dict | None = None):
        self.spark = spark
        self.config = config or {}
        self._backup_manager = get_backup_manager(self.config)
        self._recovery_manager = get_recovery_manager(self._backup_manager)

    def execute_recovery_plan(self, plan: dict):
        """Execute a dict-style recovery plan and return recovery result."""
        recovery_plan = RecoveryPlan(
            name=str(plan.get("name", "default_recovery_plan")),
            backup_source=str(plan.get("backup_source", "")),
            recovery_targets=list(plan.get("recovery_targets", [])),
            recovery_point_objective=int(plan.get("recovery_point_objective", 24)),
            recovery_time_objective=int(plan.get("recovery_time_objective", 4)),
            priority=str(plan.get("priority", "high")),
            notification_emails=list(plan.get("notification_emails", [])),
        )
        return self._recovery_manager.execute_recovery(recovery_plan)


__all__ = [
    "BackupManager",
    "RecoveryManager",
    "BackupStrategy",
    "get_backup_manager",
    "get_recovery_manager",
    "DisasterRecoveryExecutor",
]
