"""
Disaster Recovery Plans for Project_A

Manages backup strategies, recovery procedures, and disaster recovery plans.
"""

import json
import logging
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass
class BackupPlan:
    """Defines a backup plan"""

    name: str
    schedule: str  # cron-like schedule
    source_paths: list[str]
    destination_path: str
    retention_days: int
    encryption_enabled: bool
    compression_enabled: bool
    notification_emails: list[str]
    backup_type: str  # full, incremental, differential


@dataclass
class RecoveryPlan:
    """Defines a disaster recovery plan"""

    name: str
    backup_source: str
    recovery_targets: list[str]
    recovery_point_objective: int  # RPO in hours
    recovery_time_objective: int  # RTO in hours
    priority: str  # critical, high, medium, low
    notification_emails: list[str]


class BackupManager:
    """Manages backup operations"""

    def __init__(self, backup_base_path: str = "backups", config_path: str = "config/backups"):
        self.backup_base_path = Path(backup_base_path)
        self.config_path = Path(config_path)
        self.backup_base_path.mkdir(parents=True, exist_ok=True)
        self.config_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def create_backup(self, plan: BackupPlan, backup_name: str = None) -> str:
        """Create a backup based on the plan"""
        if backup_name is None:
            backup_name = f"backup_{plan.name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        backup_path = self.backup_base_path / backup_name
        backup_path.mkdir(parents=True, exist_ok=True)

        # Create backup manifest
        manifest = {
            "backup_name": backup_name,
            "plan_name": plan.name,
            "created_at": datetime.utcnow().isoformat(),
            "source_paths": plan.source_paths,
            "backup_type": plan.backup_type,
            "encrypted": plan.encryption_enabled,
            "compressed": plan.compression_enabled,
        }

        # Backup each source path
        backed_up_paths = []
        for source_path in plan.source_paths:
            source_path_obj = Path(source_path)
            if not source_path_obj.exists():
                self.logger.warning(f"Source path does not exist: {source_path}")
                continue

            # Create destination path within backup
            dest_path = backup_path / source_path_obj.name
            if source_path_obj.is_file():
                if plan.compression_enabled:
                    # Compress single file
                    compressed_path = backup_path / f"{source_path_obj.name}.zip"
                    with zipfile.ZipFile(compressed_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                        zipf.write(source_path_obj, source_path_obj.name)
                    backed_up_paths.append(str(compressed_path))
                else:
                    shutil.copy2(source_path_obj, dest_path)
                    backed_up_paths.append(str(dest_path))
            else:
                # Directory backup
                if plan.compression_enabled:
                    # Compress entire directory
                    compressed_path = backup_path / f"{source_path_obj.name}.zip"
                    shutil.make_archive(
                        str(compressed_path.with_suffix("")), "zip", source_path_obj
                    )
                    backed_up_paths.append(str(compressed_path))
                else:
                    shutil.copytree(source_path_obj, dest_path)
                    backed_up_paths.append(str(dest_path))

        manifest["backed_up_paths"] = backed_up_paths
        manifest["size_bytes"] = sum(self._get_path_size(Path(p)) for p in backed_up_paths)

        # Save manifest
        manifest_file = backup_path / "manifest.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)

        # Optionally encrypt backup
        if plan.encryption_enabled:
            self._encrypt_backup(backup_path)

        self.logger.info(f"Backup created: {backup_path}")
        return str(backup_path)

    def _get_path_size(self, path: Path) -> int:
        """Get size of file or directory in bytes"""
        if path.is_file():
            return path.stat().st_size
        else:
            total_size = 0
            for dirpath, _dirnames, filenames in path.walk():
                for filename in filenames:
                    filepath = dirpath / filename
                    total_size += filepath.stat().st_size
            return total_size

    def _encrypt_backup(self, backup_path: Path):
        """Encrypt backup contents (placeholder - implement actual encryption)"""
        # In a real implementation, you would encrypt the backup files
        # This is a placeholder that just creates an indicator file
        encryption_indicator = backup_path / ".encrypted"
        encryption_indicator.touch()
        self.logger.info(f"Backup marked as encrypted: {backup_path}")

    def restore_backup(self, backup_path: str, restore_path: str) -> bool:
        """Restore backup to specified location"""
        backup_path_obj = Path(backup_path)
        restore_path_obj = Path(restore_path)

        if not backup_path_obj.exists():
            self.logger.error(f"Backup path does not exist: {backup_path}")
            return False

        # Check if backup is encrypted
        encrypted = (backup_path_obj / ".encrypted").exists()
        if encrypted:
            self.logger.info("Backup is encrypted, decryption required")
            # In a real implementation, decrypt here

        # Load manifest
        manifest_file = backup_path_obj / "manifest.json"
        if not manifest_file.exists():
            self.logger.error("Manifest file not found in backup")
            return False

        with open(manifest_file) as f:
            manifest = json.load(f)

        # Restore each backed up path
        restore_path_obj.mkdir(parents=True, exist_ok=True)

        for backed_up_path in manifest["backed_up_paths"]:
            backed_up_path_obj = Path(backed_up_path)

            # Handle compressed files
            if backed_up_path_obj.suffix == ".zip":
                # Extract zip file
                extract_to = restore_path_obj / backed_up_path_obj.stem
                with zipfile.ZipFile(backed_up_path_obj, "r") as zipf:
                    zipf.extractall(extract_to)
            else:
                # Copy uncompressed file/directory
                if backed_up_path_obj.is_file():
                    shutil.copy2(backed_up_path_obj, restore_path_obj / backed_up_path_obj.name)
                else:
                    restore_dest = restore_path_obj / backed_up_path_obj.name
                    if restore_dest.exists():
                        shutil.rmtree(restore_dest)
                    shutil.copytree(backed_up_path_obj, restore_dest)

        self.logger.info(f"Backup restored: {backup_path} -> {restore_path}")
        return True

    def cleanup_old_backups(self, plan_name: str, retention_days: int):
        """Clean up backups older than retention period"""
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)

        for backup_dir in self.backup_base_path.iterdir():
            if backup_dir.is_dir() and plan_name in backup_dir.name:
                # Get creation date from directory name or manifest
                try:
                    # Try to parse date from directory name
                    date_str = backup_dir.name.split("_")[-2] + "_" + backup_dir.name.split("_")[-1]
                    backup_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")

                    if backup_date < cutoff_date:
                        shutil.rmtree(backup_dir)
                        self.logger.info(f"Old backup cleaned up: {backup_dir}")
                except ValueError:
                    # If date parsing fails, check manifest
                    manifest_file = backup_dir / "manifest.json"
                    if manifest_file.exists():
                        with open(manifest_file) as f:
                            manifest = json.load(f)

                        backup_date = datetime.fromisoformat(manifest["created_at"])
                        if backup_date < cutoff_date:
                            shutil.rmtree(backup_dir)
                            self.logger.info(f"Old backup cleaned up: {backup_dir}")


class RecoveryManager:
    """Manages disaster recovery operations"""

    def __init__(self, backup_manager: BackupManager):
        self.backup_manager = backup_manager
        self.logger = logging.getLogger(__name__)

    def execute_recovery(self, plan: RecoveryPlan) -> dict[str, Any]:
        """Execute a disaster recovery plan"""
        start_time = datetime.utcnow()

        try:
            # Find most recent backup
            latest_backup = self._find_latest_backup(plan.backup_source)
            if not latest_backup:
                return {
                    "status": "failed",
                    "error": "No recent backup found",
                    "started_at": start_time.isoformat(),
                    "completed_at": datetime.utcnow().isoformat(),
                    "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                }

            # Execute recovery for each target
            recovery_results = []
            for target in plan.recovery_targets:
                result = self.backup_manager.restore_backup(latest_backup, target)
                recovery_results.append(
                    {"target": target, "success": result, "restored_from": latest_backup}
                )

            # Check if all recoveries were successful
            all_successful = all(r["success"] for r in recovery_results)

            return {
                "status": "completed" if all_successful else "partial",
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "plan_name": plan.name,
                "backup_used": latest_backup,
                "recovery_results": recovery_results,
                "all_successful": all_successful,
            }

        except Exception as e:
            self.logger.error(f"Recovery failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            }

    def _find_latest_backup(self, backup_source: str) -> str | None:
        """Find the most recent backup for a source"""
        latest_backup = None
        latest_time = datetime.min

        for backup_dir in self.backup_manager.backup_base_path.iterdir():
            if backup_dir.is_dir() and backup_source in backup_dir.name:
                manifest_file = backup_dir / "manifest.json"
                if manifest_file.exists():
                    with open(manifest_file) as f:
                        manifest = json.load(f)

                    backup_time = datetime.fromisoformat(manifest["created_at"])
                    if backup_time > latest_time:
                        latest_time = backup_time
                        latest_backup = str(backup_dir)

        return latest_backup

    def validate_recovery_plan(self, plan: RecoveryPlan) -> dict[str, Any]:
        """Validate a recovery plan before execution"""
        issues = []

        # Check if backup source exists
        latest_backup = self._find_latest_backup(plan.backup_source)
        if not latest_backup:
            issues.append("No recent backup found for backup source")

        # Check if recovery targets are accessible
        for target in plan.recovery_targets:
            target_path = Path(target)
            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                issues.append(f"Permission denied accessing recovery target: {target}")

        return {
            "plan_name": plan.name,
            "is_valid": len(issues) == 0,
            "issues": issues,
            "validation_time": datetime.utcnow().isoformat(),
        }


class DRPlanManager:
    """Manages disaster recovery plans"""

    def __init__(self, plans_path: str = "config/dr_plans"):
        self.plans_path = Path(plans_path)
        self.plans_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.plans = {}

    def create_backup_plan(
        self,
        name: str,
        schedule: str,
        source_paths: list[str],
        destination_path: str,
        retention_days: int,
        backup_type: str = "full",
        encryption_enabled: bool = True,
        compression_enabled: bool = True,
        notification_emails: list[str] = None,
    ) -> BackupPlan:
        """Create a backup plan"""
        if notification_emails is None:
            notification_emails = []

        plan = BackupPlan(
            name=name,
            schedule=schedule,
            source_paths=source_paths,
            destination_path=destination_path,
            retention_days=retention_days,
            encryption_enabled=encryption_enabled,
            compression_enabled=compression_enabled,
            notification_emails=notification_emails,
            backup_type=backup_type,
        )

        self.save_backup_plan(plan)
        self.plans[f"backup_{name}"] = plan

        self.logger.info(f"Backup plan created: {name}")
        return plan

    def create_recovery_plan(
        self,
        name: str,
        backup_source: str,
        recovery_targets: list[str],
        recovery_point_objective: int = 24,
        recovery_time_objective: int = 4,
        priority: str = "medium",
        notification_emails: list[str] = None,
    ) -> RecoveryPlan:
        """Create a disaster recovery plan"""
        if notification_emails is None:
            notification_emails = []

        plan = RecoveryPlan(
            name=name,
            backup_source=backup_source,
            recovery_targets=recovery_targets,
            recovery_point_objective=recovery_point_objective,
            recovery_time_objective=recovery_time_objective,
            priority=priority,
            notification_emails=notification_emails,
        )

        self.save_recovery_plan(plan)
        self.plans[f"recovery_{name}"] = plan

        self.logger.info(f"Recovery plan created: {name}")
        return plan

    def save_backup_plan(self, plan: BackupPlan):
        """Save backup plan to file"""
        plan_file = self.plans_path / f"backup_{plan.name}_plan.json"

        with open(plan_file, "w") as f:
            plan_dict = {
                "name": plan.name,
                "schedule": plan.schedule,
                "source_paths": plan.source_paths,
                "destination_path": plan.destination_path,
                "retention_days": plan.retention_days,
                "encryption_enabled": plan.encryption_enabled,
                "compression_enabled": plan.compression_enabled,
                "notification_emails": plan.notification_emails,
                "backup_type": plan.backup_type,
            }
            json.dump(plan_dict, f, indent=2)

    def save_recovery_plan(self, plan: RecoveryPlan):
        """Save recovery plan to file"""
        plan_file = self.plans_path / f"recovery_{plan.name}_plan.json"

        with open(plan_file, "w") as f:
            plan_dict = {
                "name": plan.name,
                "backup_source": plan.backup_source,
                "recovery_targets": plan.recovery_targets,
                "recovery_point_objective": plan.recovery_point_objective,
                "recovery_time_objective": plan.recovery_time_objective,
                "priority": plan.priority,
                "notification_emails": plan.notification_emails,
            }
            json.dump(plan_dict, f, indent=2)

    def get_backup_plan(self, name: str) -> BackupPlan | None:
        """Get backup plan by name"""
        plan_key = f"backup_{name}"
        if plan_key not in self.plans:
            plan_file = self.plans_path / f"backup_{name}_plan.json"
            if plan_file.exists():
                with open(plan_file) as f:
                    plan_data = json.load(f)

                plan = BackupPlan(
                    name=plan_data["name"],
                    schedule=plan_data["schedule"],
                    source_paths=plan_data["source_paths"],
                    destination_path=plan_data["destination_path"],
                    retention_days=plan_data["retention_days"],
                    encryption_enabled=plan_data["encryption_enabled"],
                    compression_enabled=plan_data["compression_enabled"],
                    notification_emails=plan_data["notification_emails"],
                    backup_type=plan_data.get("backup_type", "full"),
                )
                self.plans[plan_key] = plan
                return plan
        return self.plans.get(plan_key)

    def get_recovery_plan(self, name: str) -> RecoveryPlan | None:
        """Get recovery plan by name"""
        plan_key = f"recovery_{name}"
        if plan_key not in self.plans:
            plan_file = self.plans_path / f"recovery_{name}_plan.json"
            if plan_file.exists():
                with open(plan_file) as f:
                    plan_data = json.load(f)

                plan = RecoveryPlan(
                    name=plan_data["name"],
                    backup_source=plan_data["backup_source"],
                    recovery_targets=plan_data["recovery_targets"],
                    recovery_point_objective=plan_data["recovery_point_objective"],
                    recovery_time_objective=plan_data["recovery_time_objective"],
                    priority=plan_data["priority"],
                    notification_emails=plan_data["notification_emails"],
                )
                self.plans[plan_key] = plan
                return plan
        return self.plans.get(plan_key)


# Global instances
_backup_manager = None
_dr_plan_manager = None
_recovery_manager = None


def get_backup_manager() -> BackupManager:
    """Get the global backup manager instance"""
    global _backup_manager
    if _backup_manager is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        backup_path = config.get("paths", {}).get("backup_root", "backups")
        config_path = config.get("paths", {}).get("dr_config_root", "config/disaster_recovery")
        _backup_manager = BackupManager(backup_path, config_path)
    return _backup_manager


def get_dr_plan_manager() -> DRPlanManager:
    """Get the global DR plan manager instance"""
    global _dr_plan_manager
    if _dr_plan_manager is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        plans_path = config.get("paths", {}).get("dr_plans_root", "config/dr_plans")
        _dr_plan_manager = DRPlanManager(plans_path)
    return _dr_plan_manager


def get_recovery_manager() -> RecoveryManager:
    """Get the global recovery manager instance"""
    global _recovery_manager
    if _recovery_manager is None:
        backup_mgr = get_backup_manager()
        _recovery_manager = RecoveryManager(backup_mgr)
    return _recovery_manager


def create_backup_plan(
    name: str,
    schedule: str,
    source_paths: list[str],
    destination_path: str,
    retention_days: int,
    backup_type: str = "full",
    encryption_enabled: bool = True,
    compression_enabled: bool = True,
    notification_emails: list[str] = None,
) -> BackupPlan:
    """Create a backup plan"""
    dr_mgr = get_dr_plan_manager()
    return dr_mgr.create_backup_plan(
        name,
        schedule,
        source_paths,
        destination_path,
        retention_days,
        backup_type,
        encryption_enabled,
        compression_enabled,
        notification_emails,
    )


def create_recovery_plan(
    name: str,
    backup_source: str,
    recovery_targets: list[str],
    recovery_point_objective: int = 24,
    recovery_time_objective: int = 4,
    priority: str = "medium",
    notification_emails: list[str] = None,
) -> RecoveryPlan:
    """Create a disaster recovery plan"""
    dr_mgr = get_dr_plan_manager()
    return dr_mgr.create_recovery_plan(
        name,
        backup_source,
        recovery_targets,
        recovery_point_objective,
        recovery_time_objective,
        priority,
        notification_emails,
    )


def execute_recovery(plan_name: str) -> dict[str, Any]:
    """Execute a disaster recovery plan"""
    dr_mgr = get_dr_plan_manager()
    recovery_plan = dr_mgr.get_recovery_plan(plan_name)
    if not recovery_plan:
        raise ValueError(f"Recovery plan not found: {plan_name}")

    recovery_mgr = get_recovery_manager()
    return recovery_mgr.execute_recovery(recovery_plan)


def validate_recovery_plan(plan_name: str) -> dict[str, Any]:
    """Validate a recovery plan"""
    dr_mgr = get_dr_plan_manager()
    recovery_plan = dr_mgr.get_recovery_plan(plan_name)
    if not recovery_plan:
        raise ValueError(f"Recovery plan not found: {plan_name}")

    recovery_mgr = get_recovery_manager()
    return recovery_mgr.validate_recovery_plan(recovery_plan)


def create_backup(plan_name: str, backup_name: str = None) -> str:
    """Create a backup using a plan"""
    dr_mgr = get_dr_plan_manager()
    backup_plan = dr_mgr.get_backup_plan(plan_name)
    if not backup_plan:
        raise ValueError(f"Backup plan not found: {plan_name}")

    backup_mgr = get_backup_manager()
    return backup_mgr.create_backup(backup_plan, backup_name)


def restore_backup(backup_path: str, restore_path: str) -> bool:
    """Restore a backup"""
    backup_mgr = get_backup_manager()
    return backup_mgr.restore_backup(backup_path, restore_path)
