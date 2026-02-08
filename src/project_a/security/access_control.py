"""
Security and Access Control System for Project_A

Implements row/column level security, data masking, and access controls.
"""

import hashlib
import json
import logging
import re
import secrets
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd


class Permission(Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"


class DataSensitivity(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class User:
    """Represents a system user"""

    user_id: str
    username: str
    email: str
    roles: list[str]
    groups: list[str]
    department: str
    created_at: datetime
    last_login: datetime | None = None
    is_active: bool = True


@dataclass
class DatasetAccessRule:
    """Defines access rules for a dataset"""

    dataset_name: str
    allowed_users: set[str]
    allowed_groups: set[str]
    allowed_roles: set[str]
    permissions: set[Permission]
    row_level_filter: str | None = None  # SQL-like filter expression
    column_masking: dict[str, str] | None = None  # column -> masking method
    sensitivity: DataSensitivity = DataSensitivity.INTERNAL


class UserManager:
    """Manages users and their authentication"""

    def __init__(self, users_path: str = "data/users"):
        self.users_path = Path(users_path)
        self.users_path.mkdir(parents=True, exist_ok=True)
        self.users_file = self.users_path / "users.json"
        self.logger = logging.getLogger(__name__)
        self.users = self._load_users()

    def _load_users(self) -> dict[str, User]:
        """Load users from file"""
        users = {}
        if self.users_file.exists():
            with open(self.users_file) as f:
                data = json.load(f)
                for user_data in data:
                    user = User(
                        user_id=user_data["user_id"],
                        username=user_data["username"],
                        email=user_data["email"],
                        roles=user_data["roles"],
                        groups=user_data["groups"],
                        department=user_data["department"],
                        created_at=datetime.fromisoformat(user_data["created_at"]),
                        last_login=datetime.fromisoformat(user_data["last_login"])
                        if user_data["last_login"]
                        else None,
                        is_active=user_data["is_active"],
                    )
                    users[user.user_id] = user
        return users

    def save_users(self):
        """Save users to file"""
        users_list = []
        for user in self.users.values():
            user_data = {
                "user_id": user.user_id,
                "username": user.username,
                "email": user.email,
                "roles": user.roles,
                "groups": user.groups,
                "department": user.department,
                "created_at": user.created_at.isoformat(),
                "last_login": user.last_login.isoformat() if user.last_login else None,
                "is_active": user.is_active,
            }
            users_list.append(user_data)

        with open(self.users_file, "w") as f:
            json.dump(users_list, f, indent=2)

    def create_user(
        self, username: str, email: str, roles: list[str], groups: list[str], department: str
    ) -> User:
        """Create a new user"""
        user_id = f"user_{secrets.token_hex(8)}"
        user = User(
            user_id=user_id,
            username=username,
            email=email,
            roles=roles,
            groups=groups,
            department=department,
            created_at=datetime.utcnow(),
            is_active=True,
        )

        self.users[user_id] = user
        self.save_users()

        self.logger.info(f"User created: {username} ({user_id})")
        return user

    def authenticate_user(self, username: str, password: str) -> User | None:
        """Authenticate a user (simplified - in real system would verify password hash)"""
        # In a real system, this would verify password hash
        for user in self.users.values():
            if user.username == username and user.is_active:
                user.last_login = datetime.utcnow()
                self.save_users()
                return user
        return None

    def get_user(self, user_id: str) -> User | None:
        """Get user by ID"""
        return self.users.get(user_id)


class AccessControlManager:
    """Manages access controls and permissions"""

    def __init__(self, rules_path: str = "data/access_rules"):
        self.rules_path = Path(rules_path)
        self.rules_path.mkdir(parents=True, exist_ok=True)
        self.rules_file = self.rules_path / "access_rules.json"
        self.logger = logging.getLogger(__name__)
        self.rules = self._load_rules()

    def _load_rules(self) -> dict[str, DatasetAccessRule]:
        """Load access rules from file"""
        rules = {}
        if self.rules_file.exists():
            with open(self.rules_file) as f:
                data = json.load(f)
                for rule_data in data:
                    rule = DatasetAccessRule(
                        dataset_name=rule_data["dataset_name"],
                        allowed_users=set(rule_data["allowed_users"]),
                        allowed_groups=set(rule_data["allowed_groups"]),
                        allowed_roles=set(rule_data["allowed_roles"]),
                        permissions={Permission(p) for p in rule_data["permissions"]},
                        row_level_filter=rule_data.get("row_level_filter"),
                        column_masking=rule_data.get("column_masking"),
                        sensitivity=DataSensitivity(rule_data.get("sensitivity", "internal")),
                    )
                    rules[rule.dataset_name] = rule
        return rules

    def save_rules(self):
        """Save rules to file"""
        rules_list = []
        for rule in self.rules.values():
            rule_data = {
                "dataset_name": rule.dataset_name,
                "allowed_users": list(rule.allowed_users),
                "allowed_groups": list(rule.allowed_groups),
                "allowed_roles": list(rule.allowed_roles),
                "permissions": [p.value for p in rule.permissions],
                "row_level_filter": rule.row_level_filter,
                "column_masking": rule.column_masking,
                "sensitivity": rule.sensitivity.value,
            }
            rules_list.append(rule_data)

        with open(self.rules_file, "w") as f:
            json.dump(rules_list, f, indent=2)

    def set_dataset_access_rule(
        self,
        dataset_name: str,
        allowed_users: list[str] = None,
        allowed_groups: list[str] = None,
        allowed_roles: list[str] = None,
        permissions: list[Permission] = None,
        row_level_filter: str = None,
        column_masking: dict[str, str] = None,
        sensitivity: DataSensitivity = DataSensitivity.INTERNAL,
    ):
        """Set access rules for a dataset"""
        rule = DatasetAccessRule(
            dataset_name=dataset_name,
            allowed_users=set(allowed_users or []),
            allowed_groups=set(allowed_groups or []),
            allowed_roles=set(allowed_roles or []),
            permissions=set(permissions or [Permission.READ]),
            row_level_filter=row_level_filter,
            column_masking=column_masking,
            sensitivity=sensitivity,
        )

        self.rules[dataset_name] = rule
        self.save_rules()

        self.logger.info(f"Access rule set for dataset: {dataset_name}")
        return rule

    def check_permission(self, user: User, dataset_name: str, permission: Permission) -> bool:
        """Check if user has permission for dataset"""
        if dataset_name not in self.rules:
            self.logger.warning(f"No access rule found for dataset: {dataset_name}")
            return False

        rule = self.rules[dataset_name]

        # Check if user has direct access
        if user.user_id in rule.allowed_users:
            return permission in rule.permissions

        # Check if user belongs to allowed groups
        if any(group in rule.allowed_groups for group in user.groups):
            return permission in rule.permissions

        # Check if user has allowed roles
        if any(role in rule.allowed_roles for role in user.roles):
            return permission in rule.permissions

        return False

    def get_row_level_filter(self, user: User, dataset_name: str) -> str | None:
        """Get row level filter for user and dataset"""
        if dataset_name not in self.rules:
            return None

        rule = self.rules[dataset_name]

        # Check if user has access first
        if (
            user.user_id not in rule.allowed_users
            and not any(group in rule.allowed_groups for group in user.groups)
            and not any(role in rule.allowed_roles for role in user.roles)
        ):
            return None  # No access

        return rule.row_level_filter

    def get_column_masking(self, user: User, dataset_name: str) -> dict[str, str] | None:
        """Get column masking rules for user and dataset"""
        if dataset_name not in self.rules:
            return None

        rule = self.rules[dataset_name]

        # Check if user has access first
        if (
            user.user_id not in rule.allowed_users
            and not any(group in rule.allowed_groups for group in user.groups)
            and not any(role in rule.allowed_roles for role in user.roles)
        ):
            return None  # No access

        return rule.column_masking


class DataMasker:
    """Applies data masking to sensitive columns"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.masking_methods = {
            "email": self._mask_email,
            "phone": self._mask_phone,
            "ssn": self._mask_ssn,
            "credit_card": self._mask_credit_card,
            "partial": self._mask_partial,
            "hash": self._mask_hash,
            "nullify": self._mask_nullify,
        }

    def mask_dataframe(self, df: pd.DataFrame, masking_rules: dict[str, str]) -> pd.DataFrame:
        """Apply masking rules to DataFrame"""
        masked_df = df.copy()

        for column, method in masking_rules.items():
            if column in masked_df.columns:
                if method in self.masking_methods:
                    masked_df[column] = masked_df[column].apply(
                        lambda x, mask_method=method: self.masking_methods[mask_method](x)
                        if pd.notna(x)
                        else x
                    )
                else:
                    self.logger.warning(f"Unknown masking method: {method}")

        return masked_df

    def _mask_email(self, value: str) -> str:
        """Mask email addresses"""
        if pd.isna(value) or not isinstance(value, str):
            return value
        try:
            parts = value.split("@")
            if len(parts) == 2:
                username, domain = parts
                masked_username = (
                    username[0] + "*" * max(0, len(username) - 2) + username[-1]
                    if len(username) > 2
                    else "*"
                )
                return f"{masked_username}@{domain}"
        except Exception:
            pass
        return value

    def _mask_phone(self, value: str) -> str:
        """Mask phone numbers"""
        if pd.isna(value) or not isinstance(value, str):
            return value
        # Keep only last 4 digits
        digits_only = re.sub(r"\D", "", value)
        if len(digits_only) >= 4:
            return "*" * (len(digits_only) - 4) + digits_only[-4:]
        return "*" * len(digits_only)

    def _mask_ssn(self, value: str) -> str:
        """Mask SSN"""
        if pd.isna(value) or not isinstance(value, str):
            return value
        # Format is XXX-XX-XXXX, mask first 5 digits
        ssn_parts = value.split("-")
        if len(ssn_parts) == 3 and len(ssn_parts[0]) == 3 and len(ssn_parts[1]) == 2:
            return f"***-**-{ssn_parts[2]}"
        return value

    def _mask_credit_card(self, value: str) -> str:
        """Mask credit card numbers"""
        if pd.isna(value) or not isinstance(value, str):
            return value
        # Keep only last 4 digits
        digits_only = re.sub(r"\D", "", value)
        if len(digits_only) >= 4:
            return "*" * (len(digits_only) - 4) + digits_only[-4:]
        return "*" * len(digits_only)

    def _mask_partial(self, value: str) -> str:
        """Partially mask values (mask middle portion)"""
        if pd.isna(value) or not isinstance(value, str):
            return value
        length = len(value)
        if length <= 2:
            return "*" * length
        visible_start = max(1, length // 4)
        visible_end = max(1, length // 4)
        hidden_length = length - visible_start - visible_end
        return value[:visible_start] + "*" * hidden_length + value[-visible_end:]

    def _mask_hash(self, value: str) -> str:
        """Hash values"""
        if pd.isna(value):
            return value
        value_str = str(value)
        return hashlib.sha256(value_str.encode()).hexdigest()[:16]

    def _mask_nullify(self, value: Any) -> Any:
        """Replace with null/NaN"""
        return pd.NA


class SecureDataFrame:
    """Wrapper for secure DataFrame operations"""

    def __init__(
        self,
        df: pd.DataFrame,
        user: User,
        dataset_name: str,
        access_control: AccessControlManager,
        data_masker: DataMasker,
    ):
        self.df = df
        self.user = user
        self.dataset_name = dataset_name
        self.access_control = access_control
        self.data_masker = data_masker
        self.logger = logging.getLogger(__name__)

    def check_access(self, permission: Permission = Permission.READ) -> bool:
        """Check if user has access to this dataset"""
        return self.access_control.check_permission(self.user, self.dataset_name, permission)

    def get_secure_dataframe(self) -> pd.DataFrame:
        """Get the secured DataFrame with filters and masking applied"""
        if not self.check_access():
            raise PermissionError(
                f"User {self.user.username} does not have access to {self.dataset_name}"
            )

        secured_df = self.df.copy()

        # Apply row-level filter if exists
        row_filter = self.access_control.get_row_level_filter(self.user, self.dataset_name)
        if row_filter:
            # Simple filter application (in real system would need proper SQL parsing)
            try:
                # This is a simplified implementation - in real system would need
                # proper parsing of the filter expression
                secured_df = self._apply_row_filter(secured_df, row_filter)
            except Exception as e:
                self.logger.error(f"Error applying row filter: {e}")

        # Apply column masking if exists
        column_masking = self.access_control.get_column_masking(self.user, self.dataset_name)
        if column_masking:
            secured_df = self.data_masker.mask_dataframe(secured_df, column_masking)

        return secured_df

    def _apply_row_filter(self, df: pd.DataFrame, filter_expr: str) -> pd.DataFrame:
        """Apply row-level filter to DataFrame (simplified implementation)"""
        # This is a simplified implementation
        # In a real system, this would need proper SQL-like expression parsing
        if filter_expr.lower().startswith("department"):
            # Example: "department = 'sales'"
            if "=" in filter_expr:
                parts = filter_expr.split("=")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = parts[1].strip().strip("'\"")
                    if col in df.columns:
                        return df[df[col] == val]
        elif "user_department" in filter_expr.lower():
            # Example: "department = user.department"
            # In real system would substitute user attributes
            pass

        # For now, return original dataframe
        return df


# Global instances
_user_manager = None
_access_control = None
_data_masker = None


def get_user_manager() -> UserManager:
    """Get the global user manager instance"""
    global _user_manager
    if _user_manager is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        users_path = config.get("paths", {}).get("users_root", "data/users")
        _user_manager = UserManager(users_path)
    return _user_manager


def get_access_control_manager() -> AccessControlManager:
    """Get the global access control manager instance"""
    global _access_control
    if _access_control is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        rules_path = config.get("paths", {}).get("access_rules_root", "data/access_rules")
        _access_control = AccessControlManager(rules_path)
    return _access_control


def get_data_masker() -> DataMasker:
    """Get the global data masker instance"""
    global _data_masker
    if _data_masker is None:
        _data_masker = DataMasker()
    return _data_masker


def create_user(
    username: str, email: str, roles: list[str], groups: list[str], department: str
) -> User:
    """Create a new user"""
    user_mgr = get_user_manager()
    return user_mgr.create_user(username, email, roles, groups, department)


def authenticate_user(username: str, password: str) -> User | None:
    """Authenticate a user"""
    user_mgr = get_user_manager()
    return user_mgr.authenticate_user(username, password)


def set_dataset_access_rule(
    dataset_name: str,
    allowed_users: list[str] = None,
    allowed_groups: list[str] = None,
    allowed_roles: list[str] = None,
    permissions: list[Permission] = None,
    row_level_filter: str = None,
    column_masking: dict[str, str] = None,
    sensitivity: DataSensitivity = DataSensitivity.INTERNAL,
):
    """Set access rules for a dataset"""
    access_ctrl = get_access_control_manager()
    return access_ctrl.set_dataset_access_rule(
        dataset_name,
        allowed_users,
        allowed_groups,
        allowed_roles,
        permissions,
        row_level_filter,
        column_masking,
        sensitivity,
    )


def check_permission(user: User, dataset_name: str, permission: Permission) -> bool:
    """Check if user has permission for dataset"""
    access_ctrl = get_access_control_manager()
    return access_ctrl.check_permission(user, dataset_name, permission)


def get_secure_dataframe(df: pd.DataFrame, user: User, dataset_name: str) -> pd.DataFrame:
    """Get a secure DataFrame with filters and masking applied"""
    access_ctrl = get_access_control_manager()
    data_masker = get_data_masker()
    secure_df = SecureDataFrame(df, user, dataset_name, access_ctrl, data_masker)
    return secure_df.get_secure_dataframe()
