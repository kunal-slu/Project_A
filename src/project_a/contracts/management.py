"""
Data Contract Management System for Project_A

Manages schema contracts, validation, and contract lifecycle.
"""

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd


class ContractStatus(Enum):
    DRAFT = "draft"
    REVIEW = "review"
    APPROVED = "approved"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


@dataclass
class SchemaField:
    """Represents a single field in a schema"""

    name: str
    type: str
    required: bool = True
    description: str = ""
    example: str | None = None
    constraints: dict[str, Any] | None = None


@dataclass
class DataContract:
    """Represents a data contract"""

    contract_id: str
    name: str
    version: str
    description: str
    schema: list[SchemaField]
    owners: list[str]
    consumers: list[str]
    status: ContractStatus
    created_at: datetime
    updated_at: datetime
    deprecated_at: datetime | None = None
    changelog: list[dict[str, Any]] = None
    tags: list[str] = None


class SchemaRegistry:
    """Manages schema versions and compatibility"""

    def __init__(self, registry_path: str = "data/contracts"):
        self.registry_path = Path(registry_path)
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.contracts_path = self.registry_path / "contracts"
        self.contracts_path.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def register_contract(self, contract: DataContract) -> str:
        """Register a new data contract"""
        # Validate contract
        self._validate_contract(contract)

        # Save contract
        contract_filename = (
            f"{contract.name.replace('/', '_').replace('.', '_')}_{contract.version}.json"
        )
        contract_path = self.contracts_path / contract_filename

        with open(contract_path, "w") as f:
            contract_dict = {
                "contract_id": contract.contract_id,
                "name": contract.name,
                "version": contract.version,
                "description": contract.description,
                "schema": [
                    {
                        "name": field.name,
                        "type": field.type,
                        "required": field.required,
                        "description": field.description,
                        "example": field.example,
                        "constraints": field.constraints,
                    }
                    for field in contract.schema
                ],
                "owners": contract.owners,
                "consumers": contract.consumers,
                "status": contract.status.value,
                "created_at": contract.created_at.isoformat(),
                "updated_at": contract.updated_at.isoformat(),
                "deprecated_at": contract.deprecated_at.isoformat()
                if contract.deprecated_at
                else None,
                "changelog": contract.changelog or [],
                "tags": contract.tags or [],
            }
            json.dump(contract_dict, f, indent=2)

        self.logger.info(f"Contract registered: {contract.name} v{contract.version}")
        return str(contract_path)

    def get_contract(self, name: str, version: str = None) -> DataContract | None:
        """Get a data contract by name and version"""
        if version:
            filename = f"{name.replace('/', '_').replace('.', '_')}_{version}.json"
            path = self.contracts_path / filename
        else:
            # Get latest version
            files = list(
                self.contracts_path.glob(f"{name.replace('/', '_').replace('.', '_')}_*.json")
            )
            if not files:
                return None
            # Sort by version (assuming semantic versioning)
            files.sort(key=lambda x: [int(i) for i in x.stem.split("_")[-1].split(".")])
            path = files[-1]

        if not path.exists():
            return None

        with open(path) as f:
            data = json.load(f)

        schema = [
            SchemaField(
                name=field["name"],
                type=field["type"],
                required=field.get("required", True),
                description=field.get("description", ""),
                example=field.get("example"),
                constraints=field.get("constraints"),
            )
            for field in data["schema"]
        ]

        return DataContract(
            contract_id=data["contract_id"],
            name=data["name"],
            version=data["version"],
            description=data["description"],
            schema=schema,
            owners=data["owners"],
            consumers=data["consumers"],
            status=ContractStatus(data["status"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            deprecated_at=datetime.fromisoformat(data["deprecated_at"])
            if data.get("deprecated_at")
            else None,
            changelog=data.get("changelog", []),
            tags=data.get("tags", []),
        )

    def get_all_versions(self, name: str) -> list[str]:
        """Get all versions of a contract"""
        files = self.contracts_path.glob(f"{name.replace('/', '_').replace('.', '_')}_*.json")
        versions = []
        for file in files:
            version = file.stem.split("_")[-1]  # Extract version from filename
            versions.append(version)
        return sorted(versions)

    def _validate_contract(self, contract: DataContract):
        """Validate contract structure"""
        if not contract.name:
            raise ValueError("Contract name is required")

        if not re.match(r"^\d+\.\d+\.\d+$", contract.version):
            raise ValueError("Version must follow semantic versioning (X.Y.Z)")

        # Check for duplicate field names
        field_names = [field.name for field in contract.schema]
        if len(field_names) != len(set(field_names)):
            raise ValueError("Duplicate field names in schema")

        # Validate field types
        valid_types = {
            "string",
            "integer",
            "float",
            "boolean",
            "timestamp",
            "array",
            "object",
            "decimal",
        }
        for field in contract.schema:
            if ":" in field.type:  # Handle complex types like array<string>
                base_type = field.type.split("<")[0]
                if base_type not in valid_types and f"{base_type}s" not in valid_types:
                    raise ValueError(f"Invalid field type: {field.type}")
            elif field.type not in valid_types:
                raise ValueError(f"Invalid field type: {field.type}")

    def check_compatibility(
        self, old_contract: DataContract, new_contract: DataContract
    ) -> dict[str, Any]:
        """Check compatibility between two contract versions"""
        result = {
            "compatible": True,
            "breaking_changes": [],
            "non_breaking_changes": [],
            "semantic_version_change": "",  # major, minor, patch
        }

        # Compare schemas
        old_fields = {field.name: field for field in old_contract.schema}
        new_fields = {field.name: field for field in new_contract.schema}

        # Check for removed fields (breaking change)
        for field_name in old_fields:
            if field_name not in new_fields:
                result["compatible"] = False
                result["breaking_changes"].append(f"Field '{field_name}' was removed")

        # Check for changed field types (breaking change)
        for field_name, old_field in old_fields.items():
            if field_name in new_fields:
                new_field = new_fields[field_name]
                if old_field.type != new_field.type:
                    # Allow string to more specific type (non-breaking)
                    # But specific type to string is breaking
                    if old_field.type != "string" or new_field.type == "string":
                        result["compatible"] = False
                        result["breaking_changes"].append(
                            f"Field '{field_name}' type changed from '{old_field.type}' to '{new_field.type}'"
                        )

                # Check if required flag changed from false to true (breaking)
                if not old_field.required and new_field.required:
                    result["compatible"] = False
                    result["breaking_changes"].append(
                        f"Field '{field_name}' changed from optional to required"
                    )

        # Check for added required fields (potentially breaking)
        for field_name, new_field in new_fields.items():
            if field_name not in old_fields and new_field.required:
                result["breaking_changes"].append(f"Required field '{field_name}' was added")
                result["compatible"] = False
            elif field_name not in old_fields and not new_field.required:
                result["non_breaking_changes"].append(f"Optional field '{field_name}' was added")

        # Determine semantic version change
        if result["breaking_changes"]:
            result["semantic_version_change"] = "major"
        elif result["non_breaking_changes"]:
            result["semantic_version_change"] = "minor"
        else:
            result["semantic_version_change"] = "patch"

        return result


class ContractValidator:
    """Validates data against contracts"""

    def __init__(self, registry: SchemaRegistry):
        self.registry = registry
        self.logger = logging.getLogger(__name__)

    def validate_data(
        self, data: pd.DataFrame | dict | list, contract_name: str, version: str = None
    ) -> dict[str, Any]:
        """Validate data against a contract"""
        contract = self.registry.get_contract(contract_name, version)
        if not contract:
            raise ValueError(f"Contract not found: {contract_name} v{version or 'latest'}")

        validation_result = {
            "contract_name": contract_name,
            "version": version or contract.version,
            "valid": True,
            "errors": [],
            "warnings": [],
            "validated_at": datetime.utcnow().isoformat(),
        }

        # Convert to DataFrame if needed
        if isinstance(data, dict):
            data = pd.DataFrame([data])
        elif isinstance(data, list):
            data = pd.DataFrame(data)

        # Check required fields
        for field in contract.schema:
            if field.required and field.name not in data.columns:
                validation_result["valid"] = False
                validation_result["errors"].append(f"Required field '{field.name}' is missing")

        if not validation_result["valid"]:
            return validation_result

        # Validate each field
        for field in contract.schema:
            if field.name in data.columns:
                validation_result.update(
                    self._validate_field(data[field.name], field, validation_result)
                )

        return validation_result

    def _validate_field(
        self, series: pd.Series, field: SchemaField, result: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate a single field"""
        # Check data type
        type_errors = self._check_type_compatibility(series, field.type)
        if type_errors:
            result["valid"] = False
            result["errors"].extend(type_errors)

        # Check constraints
        constraint_errors = self._check_constraints(series, field.constraints)
        if constraint_errors:
            result["valid"] = False
            result["errors"].extend(constraint_errors)

        return result

    def _check_type_compatibility(self, series: pd.Series, expected_type: str) -> list[str]:
        """Check if series data matches expected type"""
        errors = []

        # Handle complex types
        if "<" in expected_type:  # e.g., array<string>, struct<...>
            base_type = expected_type.split("<")[0]
            if base_type == "array":
                # Check if all values are arrays/lists
                for idx, value in enumerate(series):
                    if pd.notna(value) and not isinstance(
                        value, list | str
                    ):  # String arrays are common
                        errors.append(f"Value at index {idx} is not an array/list")
            # Add more complex type validations as needed
        else:
            # Map expected types to pandas types
            type_mapping = {
                "string": "object",
                "integer": "int",
                "float": "float",
                "boolean": "bool",
                "timestamp": "datetime64",
                "decimal": "float",  # Treat decimals as floats for validation
            }

            expected_pandas_type = type_mapping.get(expected_type)
            if expected_pandas_type:
                if not str(series.dtype).startswith(expected_pandas_type):
                    errors.append(f"Expected type '{expected_type}', got '{series.dtype}'")

        return errors

    def _check_constraints(
        self, series: pd.Series, constraints: dict[str, Any] | None
    ) -> list[str]:
        """Check field constraints"""
        if not constraints:
            return []

        errors = []

        # Check for null values if field is required
        null_count = series.isna().sum()
        if null_count > 0:
            errors.append(f"Found {null_count} null values in field")

        # Check min/max for numeric fields
        if pd.api.types.is_numeric_dtype(series):
            if "min" in constraints and series.min() < constraints["min"]:
                errors.append(f"Value below minimum: {series.min()} < {constraints['min']}")
            if "max" in constraints and series.max() > constraints["max"]:
                errors.append(f"Value above maximum: {series.max()} > {constraints['max']}")

        # Check regex for string fields
        if pd.api.types.is_object_dtype(series) and "pattern" in constraints:
            pattern = constraints["pattern"]
            invalid_values = series[not series.astype(str).str.match(pattern)]
            if len(invalid_values) > 0:
                errors.append(f"Values don't match pattern '{pattern}': {invalid_values.tolist()}")

        # Check enum values
        if "enum" in constraints:
            allowed_values = set(constraints["enum"])
            invalid_values = set(series.unique()) - allowed_values
            if invalid_values:
                errors.append(f"Invalid enum values: {list(invalid_values)}")

        return errors


class ContractManager:
    """Manages the contract lifecycle"""

    def __init__(self, registry: SchemaRegistry, validator: ContractValidator):
        self.registry = registry
        self.validator = validator
        self.logger = logging.getLogger(__name__)

    def create_contract(
        self,
        name: str,
        version: str,
        description: str,
        schema: list[SchemaField],
        owners: list[str],
        consumers: list[str],
        tags: list[str] = None,
    ) -> DataContract:
        """Create a new data contract"""
        contract = DataContract(
            contract_id=hashlib.sha256(
                f"{name}_{version}_{datetime.utcnow().isoformat()}".encode()
            ).hexdigest()[:16],
            name=name,
            version=version,
            description=description,
            schema=schema,
            owners=owners,
            consumers=consumers,
            status=ContractStatus.DRAFT,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            changelog=[
                {
                    "version": version,
                    "description": "Initial version",
                    "date": datetime.utcnow().isoformat(),
                }
            ],
            tags=tags or [],
        )

        self.registry.register_contract(contract)
        return contract

    def update_contract(
        self,
        name: str,
        new_version: str,
        description: str = None,
        schema: list[SchemaField] = None,
        status: ContractStatus = None,
        changelog_entry: str = None,
    ) -> DataContract | None:
        """Update an existing contract"""
        current_contract = self.registry.get_contract(name)
        if not current_contract:
            return None

        # Check compatibility
        if schema:
            compatibility = self.registry.check_compatibility(
                current_contract,
                DataContract(
                    contract_id=current_contract.contract_id,
                    name=current_contract.name,
                    version=new_version,
                    description=description or current_contract.description,
                    schema=schema,
                    owners=current_contract.owners,
                    consumers=current_contract.consumers,
                    status=status or current_contract.status,
                    created_at=current_contract.created_at,
                    updated_at=datetime.utcnow(),
                    changelog=current_contract.changelog,
                ),
            )

            if not compatibility["compatible"] and new_version <= current_contract.version:
                self.logger.error(
                    f"Incompatible changes require a higher version number: {compatibility['breaking_changes']}"
                )
                return None

        # Create updated contract
        updated_contract = DataContract(
            contract_id=current_contract.contract_id,
            name=name,
            version=new_version,
            description=description or current_contract.description,
            schema=schema or current_contract.schema,
            owners=current_contract.owners,
            consumers=current_contract.consumers,
            status=status or current_contract.status,
            created_at=current_contract.created_at,
            updated_at=datetime.utcnow(),
            deprecated_at=current_contract.deprecated_at,
            changelog=current_contract.changelog
            + [
                {
                    "version": new_version,
                    "description": changelog_entry or "Updated contract",
                    "date": datetime.utcnow().isoformat(),
                }
            ]
            if changelog_entry
            else current_contract.changelog,
            tags=current_contract.tags,
        )

        self.registry.register_contract(updated_contract)
        return updated_contract

    def deprecate_contract(self, name: str, version: str = None):
        """Deprecate a contract"""
        contract = self.registry.get_contract(name, version)
        if not contract:
            raise ValueError(f"Contract not found: {name}")

        # Update status to deprecated
        updated_contract = DataContract(
            contract_id=contract.contract_id,
            name=contract.name,
            version=contract.version,
            description=contract.description,
            schema=contract.schema,
            owners=contract.owners,
            consumers=contract.consumers,
            status=ContractStatus.DEPRECATED,
            created_at=contract.created_at,
            updated_at=datetime.utcnow(),
            deprecated_at=datetime.utcnow(),
            changelog=contract.changelog
            + [
                {
                    "version": contract.version,
                    "description": "Contract deprecated",
                    "date": datetime.utcnow().isoformat(),
                }
            ],
            tags=contract.tags,
        )

        self.registry.register_contract(updated_contract)


# Global instances
_registry = None
_validator = None
_contract_manager = None


def get_schema_registry() -> SchemaRegistry:
    """Get the global schema registry instance"""
    global _registry
    if _registry is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        registry_path = config.get("paths", {}).get("contracts_root", "data/contracts")
        _registry = SchemaRegistry(registry_path)
    return _registry


def get_contract_validator() -> ContractValidator:
    """Get the global contract validator instance"""
    global _validator
    if _validator is None:
        registry = get_schema_registry()
        _validator = ContractValidator(registry)
    return _validator


def get_contract_manager() -> ContractManager:
    """Get the global contract manager instance"""
    global _contract_manager
    if _contract_manager is None:
        registry = get_schema_registry()
        validator = get_contract_validator()
        _contract_manager = ContractManager(registry, validator)
    return _contract_manager


def create_contract(
    name: str,
    version: str,
    description: str,
    schema: list[SchemaField],
    owners: list[str],
    consumers: list[str],
    tags: list[str] = None,
) -> DataContract:
    """Create a new data contract"""
    manager = get_contract_manager()
    return manager.create_contract(name, version, description, schema, owners, consumers, tags)


def get_contract(name: str, version: str = None) -> DataContract | None:
    """Get a data contract"""
    registry = get_schema_registry()
    return registry.get_contract(name, version)


def validate_data(
    data: pd.DataFrame | dict | list, contract_name: str, version: str = None
) -> dict[str, Any]:
    """Validate data against a contract"""
    validator = get_contract_validator()
    return validator.validate_data(data, contract_name, version)


def update_contract(
    name: str,
    new_version: str,
    description: str = None,
    schema: list[SchemaField] = None,
    status: ContractStatus = None,
    changelog_entry: str = None,
) -> DataContract | None:
    """Update an existing contract"""
    manager = get_contract_manager()
    return manager.update_contract(name, new_version, description, schema, status, changelog_entry)


def deprecate_contract(name: str, version: str = None):
    """Deprecate a contract"""
    manager = get_contract_manager()
    manager.deprecate_contract(name, version)


def check_compatibility(old_contract_name: str, new_contract: DataContract) -> dict[str, Any]:
    """Check compatibility between contracts"""
    registry = get_schema_registry()
    old_contract = registry.get_contract(old_contract_name)
    if not old_contract:
        raise ValueError(f"Old contract not found: {old_contract_name}")
    return registry.check_compatibility(old_contract, new_contract)
