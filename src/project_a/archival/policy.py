"""
Data Archival and Retention Policy System for Project_A

Manages data lifecycle, archival policies, and retention periods.
"""

import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class RetentionPolicy:
    """Defines a retention policy for a dataset"""

    name: str
    dataset_path: str
    retention_period_days: int
    archive_after_days: int
    delete_after_days: int
    backup_enabled: bool
    compression_enabled: bool
    notification_emails: list[str]


class ArchiveManager:
    """Manages data archival operations"""

    def __init__(
        self, archive_base_path: str = "data/archive", policies_path: str = "data/policies"
    ):
        self.archive_base_path = Path(archive_base_path)
        self.policies_path = Path(policies_path)
        self.archive_base_path.mkdir(parents=True, exist_ok=True)
        self.policies_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def archive_dataset(
        self, dataset_path: str, policy_name: str, archive_reason: str = "retention_policy"
    ) -> str:
        """Archive a dataset according to retention policy"""
        dataset_path_obj = Path(dataset_path)

        if not dataset_path_obj.exists():
            raise FileNotFoundError(f"Dataset path does not exist: {dataset_path}")

        # Create archive path with timestamp
        archive_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{dataset_path_obj.name}_{archive_timestamp}"
        archive_path = self.archive_base_path / policy_name / archive_name

        # Create policy subdirectory
        archive_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy dataset to archive location
        if dataset_path_obj.is_file():
            shutil.copy2(dataset_path_obj, archive_path)
        else:
            shutil.copytree(dataset_path_obj, archive_path)

        # Create metadata file
        metadata = {
            "original_path": str(dataset_path_obj.absolute()),
            "archived_at": datetime.utcnow().isoformat(),
            "policy_name": policy_name,
            "archive_reason": archive_reason,
            "size_bytes": self._get_directory_size(archive_path),
        }

        metadata_file = archive_path.parent / f"{archive_name}_metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        self.logger.info(f"Dataset archived: {dataset_path} -> {archive_path}")
        return str(archive_path)

    def _get_directory_size(self, path: Path) -> int:
        """Get size of directory in bytes"""
        total_size = 0
        for dirpath, _dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
        return total_size

    def get_archive_info(self, archive_path: str) -> dict[str, Any]:
        """Get information about an archived dataset"""
        archive_path_obj = Path(archive_path)
        metadata_file = archive_path_obj.parent / f"{archive_path_obj.name}_metadata.json"

        if metadata_file.exists():
            with open(metadata_file) as f:
                metadata = json.load(f)
        else:
            metadata = {}

        return {
            "archive_path": str(archive_path_obj.absolute()),
            "exists": archive_path_obj.exists(),
            "size_bytes": self._get_directory_size(archive_path_obj)
            if archive_path_obj.exists()
            else 0,
            "metadata": metadata,
        }

    def restore_from_archive(self, archive_path: str, restore_path: str) -> bool:
        """Restore dataset from archive"""
        archive_path_obj = Path(archive_path)
        restore_path_obj = Path(restore_path)

        if not archive_path_obj.exists():
            self.logger.error(f"Archive path does not exist: {archive_path}")
            return False

        # Create destination directory
        restore_path_obj.parent.mkdir(parents=True, exist_ok=True)

        # Copy from archive to restore location
        if archive_path_obj.is_file():
            shutil.copy2(archive_path_obj, restore_path_obj)
        else:
            if restore_path_obj.exists():
                shutil.rmtree(restore_path_obj)
            shutil.copytree(archive_path_obj, restore_path_obj)

        self.logger.info(f"Dataset restored: {archive_path} -> {restore_path}")
        return True


class RetentionPolicyManager:
    """Manages retention policies"""

    def __init__(self, policies_path: str = "data/policies"):
        self.policies_path = Path(policies_path)
        self.policies_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.policies = {}

    def create_policy(
        self,
        name: str,
        dataset_path: str,
        retention_period_days: int,
        archive_after_days: int,
        delete_after_days: int,
        backup_enabled: bool = True,
        compression_enabled: bool = True,
        notification_emails: list[str] = None,
    ) -> RetentionPolicy:
        """Create a new retention policy"""
        if notification_emails is None:
            notification_emails = []

        policy = RetentionPolicy(
            name=name,
            dataset_path=dataset_path,
            retention_period_days=retention_period_days,
            archive_after_days=archive_after_days,
            delete_after_days=delete_after_days,
            backup_enabled=backup_enabled,
            compression_enabled=compression_enabled,
            notification_emails=notification_emails,
        )

        self.save_policy(policy)
        self.policies[name] = policy

        self.logger.info(f"Retention policy created: {name}")
        return policy

    def save_policy(self, policy: RetentionPolicy):
        """Save policy to file"""
        policy_file = self.policies_path / f"{policy.name}_policy.json"

        with open(policy_file, "w") as f:
            policy_dict = {
                "name": policy.name,
                "dataset_path": policy.dataset_path,
                "retention_period_days": policy.retention_period_days,
                "archive_after_days": policy.archive_after_days,
                "delete_after_days": policy.delete_after_days,
                "backup_enabled": policy.backup_enabled,
                "compression_enabled": policy.compression_enabled,
                "notification_emails": policy.notification_emails,
            }
            json.dump(policy_dict, f, indent=2)

    def load_policy(self, name: str) -> RetentionPolicy | None:
        """Load policy from file"""
        policy_file = self.policies_path / f"{name}_policy.json"

        if not policy_file.exists():
            return None

        with open(policy_file) as f:
            policy_data = json.load(f)

        policy = RetentionPolicy(
            name=policy_data["name"],
            dataset_path=policy_data["dataset_path"],
            retention_period_days=policy_data["retention_period_days"],
            archive_after_days=policy_data["archive_after_days"],
            delete_after_days=policy_data["delete_after_days"],
            backup_enabled=policy_data["backup_enabled"],
            compression_enabled=policy_data["compression_enabled"],
            notification_emails=policy_data["notification_emails"],
        )

        self.policies[name] = policy
        return policy

    def get_policy(self, name: str) -> RetentionPolicy | None:
        """Get policy by name"""
        if name not in self.policies:
            return self.load_policy(name)
        return self.policies.get(name)

    def list_policies(self) -> list[str]:
        """List all policy names"""
        return list(self.policies.keys())


class LifecycleManager:
    """Manages the complete data lifecycle: archive, delete, and retention"""

    def __init__(self, archive_manager: ArchiveManager, policy_manager: RetentionPolicyManager):
        self.archive_manager = archive_manager
        self.policy_manager = policy_manager
        self.logger = logging.getLogger(__name__)

    def evaluate_lifecycle(self, dataset_path: str) -> dict[str, Any]:
        """Evaluate lifecycle status of a dataset"""
        # Find applicable policy
        policy = self._find_applicable_policy(dataset_path)
        if not policy:
            return {
                "dataset_path": dataset_path,
                "policy_applied": False,
                "action_needed": "no_policy",
                "message": "No retention policy found for this dataset",
            }

        # Calculate age of dataset
        dataset_path_obj = Path(dataset_path)
        if dataset_path_obj.is_file():
            modified_time = datetime.fromtimestamp(dataset_path_obj.stat().st_mtime)
        else:
            # Use the newest file in the directory
            files = [f for f in dataset_path_obj.rglob("*") if f.is_file()]
            if files:
                modified_time = datetime.fromtimestamp(max(f.stat().st_mtime for f in files))
            else:
                modified_time = datetime.fromtimestamp(dataset_path_obj.stat().st_mtime)

        age_days = (datetime.utcnow() - modified_time).days

        # Determine action based on policy
        if age_days >= policy.delete_after_days:
            action = "delete"
            message = f"Dataset is {age_days} days old, exceeding delete threshold of {policy.delete_after_days} days"
        elif age_days >= policy.archive_after_days:
            action = "archive"
            message = f"Dataset is {age_days} days old, exceeding archive threshold of {policy.archive_after_days} days"
        else:
            action = "keep"
            message = f"Dataset is {age_days} days old, within retention period of {policy.retention_period_days} days"

        return {
            "dataset_path": dataset_path,
            "policy_applied": True,
            "policy_name": policy.name,
            "age_days": age_days,
            "action_needed": action,
            "message": message,
            "policy_details": {
                "archive_after_days": policy.archive_after_days,
                "delete_after_days": policy.delete_after_days,
            },
        }

    def execute_lifecycle_action(self, dataset_path: str) -> dict[str, Any]:
        """Execute the appropriate lifecycle action for a dataset"""
        evaluation = self.evaluate_lifecycle(dataset_path)

        if evaluation["action_needed"] == "archive":
            try:
                policy = self.policy_manager.get_policy(evaluation["policy_name"])
                archive_path = self.archive_manager.archive_dataset(
                    dataset_path, policy.name, "retention_policy_archive"
                )
                return {
                    "status": "archived",
                    "archive_path": archive_path,
                    "original_path": dataset_path,
                    "message": f"Dataset archived successfully to {archive_path}",
                }
            except Exception as e:
                self.logger.error(f"Failed to archive dataset {dataset_path}: {e}")
                return {
                    "status": "error",
                    "error": str(e),
                    "message": f"Failed to archive dataset: {e}",
                }

        elif evaluation["action_needed"] == "delete":
            try:
                # Actually delete the dataset
                dataset_path_obj = Path(dataset_path)
                if dataset_path_obj.is_file():
                    dataset_path_obj.unlink()
                else:
                    shutil.rmtree(dataset_path_obj)

                return {
                    "status": "deleted",
                    "deleted_path": dataset_path,
                    "message": f"Dataset deleted successfully: {dataset_path}",
                }
            except Exception as e:
                self.logger.error(f"Failed to delete dataset {dataset_path}: {e}")
                return {
                    "status": "error",
                    "error": str(e),
                    "message": f"Failed to delete dataset: {e}",
                }

        elif evaluation["action_needed"] == "keep":
            return {
                "status": "kept",
                "dataset_path": dataset_path,
                "message": evaluation["message"],
            }

        else:  # no policy
            return {
                "status": "no_action",
                "dataset_path": dataset_path,
                "message": evaluation["message"],
            }

    def _find_applicable_policy(self, dataset_path: str) -> RetentionPolicy | None:
        """Find the most appropriate policy for a dataset path"""
        # Find policy that matches the dataset path
        for policy_name in self.policy_manager.list_policies():
            policy = self.policy_manager.get_policy(policy_name)
            if policy and str(Path(dataset_path).resolve()).startswith(
                str(Path(policy.dataset_path).resolve())
            ):
                return policy
        return None

    def run_lifecycle_management(self, base_path: str = "data") -> list[dict[str, Any]]:
        """Run lifecycle management for all datasets in a base path"""
        results = []

        base_path_obj = Path(base_path)
        # Find all potential dataset directories/files
        for item in base_path_obj.rglob("*"):
            if item.is_file() and item.suffix in [".csv", ".parquet", ".json", ".delta"]:
                result = self.execute_lifecycle_action(str(item))
                results.append(result)
            elif item.is_dir() and any(
                f.suffix in [".csv", ".parquet", ".json", ".delta"] for f in item.rglob("*")
            ):
                # Directory containing data files
                result = self.execute_lifecycle_action(str(item))
                results.append(result)

        return results


# Global instances
_archive_manager = None
_policy_manager = None
_lifecycle_manager = None


def get_archive_manager() -> ArchiveManager:
    """Get the global archive manager instance"""
    global _archive_manager
    if _archive_manager is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        archive_path = config.get("paths", {}).get("archive_root", "data/archive")
        policies_path = config.get("paths", {}).get("policies_root", "data/policies")
        _archive_manager = ArchiveManager(archive_path, policies_path)
    return _archive_manager


def get_policy_manager() -> RetentionPolicyManager:
    """Get the global policy manager instance"""
    global _policy_manager
    if _policy_manager is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        policies_path = config.get("paths", {}).get("policies_root", "data/policies")
        _policy_manager = RetentionPolicyManager(policies_path)
    return _policy_manager


def get_lifecycle_manager() -> LifecycleManager:
    """Get the global lifecycle manager instance"""
    global _lifecycle_manager
    if _lifecycle_manager is None:
        archive_mgr = get_archive_manager()
        policy_mgr = get_policy_manager()
        _lifecycle_manager = LifecycleManager(archive_mgr, policy_mgr)
    return _lifecycle_manager


def create_retention_policy(
    name: str,
    dataset_path: str,
    retention_period_days: int,
    archive_after_days: int,
    delete_after_days: int,
    backup_enabled: bool = True,
    compression_enabled: bool = True,
    notification_emails: list[str] = None,
) -> RetentionPolicy:
    """Create a retention policy"""
    policy_mgr = get_policy_manager()
    return policy_mgr.create_policy(
        name,
        dataset_path,
        retention_period_days,
        archive_after_days,
        delete_after_days,
        backup_enabled,
        compression_enabled,
        notification_emails,
    )


def archive_dataset(
    dataset_path: str, policy_name: str, archive_reason: str = "retention_policy"
) -> str:
    """Archive a dataset"""
    archive_mgr = get_archive_manager()
    return archive_mgr.archive_dataset(dataset_path, policy_name, archive_reason)


def evaluate_lifecycle(dataset_path: str) -> dict[str, Any]:
    """Evaluate lifecycle status of a dataset"""
    lifecycle_mgr = get_lifecycle_manager()
    return lifecycle_mgr.evaluate_lifecycle(dataset_path)


def execute_lifecycle_action(dataset_path: str) -> dict[str, Any]:
    """Execute lifecycle action for a dataset"""
    lifecycle_mgr = get_lifecycle_manager()
    return lifecycle_mgr.execute_lifecycle_action(dataset_path)


def run_lifecycle_management(base_path: str = "data") -> list[dict[str, Any]]:
    """Run lifecycle management for all datasets in a base path"""
    lifecycle_mgr = get_lifecycle_manager()
    return lifecycle_mgr.run_lifecycle_management(base_path)
