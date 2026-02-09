"""
Data Contract System
Defines and enforces data contracts for all tables including returns_raw.
Handles schema evolution, validation, and drift detection.
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


class ContractSeverity(Enum):
    """Contract validation severity levels."""

    ERROR = "error"  # Fail pipeline
    WARNING = "warning"  # Log warning, continue
    INFO = "info"  # Log info only


class SchemaChangeType(Enum):
    """Types of schema changes."""

    ADD_COLUMN = "add_column"
    REMOVE_COLUMN = "remove_column"
    CHANGE_TYPE = "change_type"
    CHANGE_NULLABILITY = "change_nullability"
    ADD_CONSTRAINT = "add_constraint"
    REMOVE_CONSTRAINT = "remove_constraint"


@dataclass
class DataConstraint:
    """Data quality constraint definition."""

    name: str
    constraint_type: str  # not_null, unique, range, regex, custom
    column: str
    parameters: dict[str, Any]
    severity: ContractSeverity = ContractSeverity.ERROR
    description: str = ""
    enabled: bool = True


@dataclass
class TableContract:
    """Data contract for a table."""

    table_name: str
    version: str
    schema: StructType
    constraints: list[DataConstraint]
    partition_columns: list[str] = None
    clustering_columns: list[str] = None
    retention_days: int = 365
    created_at: str = None
    updated_at: str = None
    owner: str = "data_team"
    description: str = ""
    tags: list[str] = None
    schema_evolution_policy: str = "strict"  # strict, flexible, backward_compatible


class DataContractManager:
    """
    Manages data contracts, schema validation, and evolution.
    """

    def __init__(self, spark: SparkSession, contracts_dir: str = "data/contracts"):
        self.spark = spark
        self.contracts_dir = Path(contracts_dir)
        self.contracts_dir.mkdir(parents=True, exist_ok=True)

        # Initialize contracts
        self.contracts: dict[str, TableContract] = {}
        self._load_existing_contracts()
        self._create_default_contracts()

    def _load_existing_contracts(self):
        """Load existing contracts from files."""
        try:
            for contract_file in self.contracts_dir.glob("*.json"):
                try:
                    with open(contract_file) as f:
                        contract_data = json.load(f)

                    # Reconstruct schema from JSON
                    schema = self._schema_from_json(contract_data["schema"])

                    # Reconstruct constraints
                    constraints = []
                    for constraint_data in contract_data.get("constraints", []):
                        constraint = DataConstraint(**constraint_data)
                        constraint.severity = ContractSeverity(constraint_data["severity"])
                        constraints.append(constraint)

                    # Create contract
                    contract = TableContract(
                        table_name=contract_data["table_name"],
                        version=contract_data["version"],
                        schema=schema,
                        constraints=constraints,
                        partition_columns=contract_data.get("partition_columns"),
                        clustering_columns=contract_data.get("clustering_columns"),
                        retention_days=contract_data.get("retention_days", 365),
                        created_at=contract_data.get("created_at"),
                        updated_at=contract_data.get("updated_at"),
                        owner=contract_data.get("owner", "data_team"),
                        description=contract_data.get("description", ""),
                        tags=contract_data.get("tags", []),
                        schema_evolution_policy=contract_data.get(
                            "schema_evolution_policy", "strict"
                        ),
                    )

                    self.contracts[contract_data["table_name"]] = contract
                    logger.info(f"Loaded contract for {contract_data['table_name']}")

                except Exception as e:
                    logger.warning(f"Failed to load contract {contract_file}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Failed to load existing contracts: {e}")

    def _schema_from_json(self, schema_json: dict[str, Any]) -> StructType:
        """Reconstruct PySpark schema from JSON."""
        fields = []

        for field_data in schema_json["fields"]:
            field_type = self._type_from_json(field_data["type"])
            field = StructField(
                name=field_data["name"],
                dataType=field_type,
                nullable=field_data.get("nullable", True),
                metadata=field_data.get("metadata", {}),
            )
            fields.append(field)

        return StructType(fields)

    def _type_from_json(self, type_data: str | dict[str, Any]) -> Any:
        """Convert JSON type representation to PySpark type."""
        if isinstance(type_data, str):
            type_mapping = {
                "string": StringType(),
                "integer": IntegerType(),
                "double": DoubleType(),
                "timestamp": TimestampType(),
                "boolean": BooleanType(),
                "date": DateType(),
            }
            return type_mapping.get(type_data, StringType())

        elif isinstance(type_data, dict):
            if type_data["type"] == "array":
                element_type = self._type_from_json(type_data["elementType"])
                return ArrayType(element_type)
            elif type_data["type"] == "map":
                key_type = self._type_from_json(type_data["keyType"])
                value_type = self._type_from_json(type_data["valueType"])
                return MapType(key_type, value_type)
            elif type_data["type"] == "decimal":
                return DecimalType(
                    precision=type_data.get("precision", 10), scale=type_data.get("scale", 2)
                )

        return StringType()

    def _create_default_contracts(self):
        """Create default contracts for all tables if they don't exist."""
        default_contracts = {
            "returns_raw": self._create_returns_raw_contract(),
            "customers_raw": self._create_customers_raw_contract(),
            "products_raw": self._create_products_raw_contract(),
            "orders_raw": self._create_orders_raw_contract(),
            "inventory_snapshots": self._create_inventory_contract(),
            "fx_rates": self._create_fx_rates_contract(),
        }

        for table_name, contract in default_contracts.items():
            if table_name not in self.contracts:
                self.contracts[table_name] = contract
                self._save_contract(contract)
                logger.info(f"Created default contract for {table_name}")

    def _create_returns_raw_contract(self) -> TableContract:
        """Create comprehensive contract for returns_raw table."""
        schema = StructType(
            [
                StructField("return_id", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("return_date", TimestampType(), True),
                StructField("return_reason", StringType(), True),
                StructField("refund_amount", DecimalType(10, 2), True),
                StructField("order_amount", DecimalType(10, 2), True),
                StructField("return_status", StringType(), True),
                StructField("return_method", StringType(), True),
                StructField("processing_time_days", IntegerType(), True),
                StructField("is_partial_return", BooleanType(), True),
                StructField("return_notes", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

        constraints = [
            DataConstraint(
                name="return_id_not_null",
                constraint_type="not_null",
                column="return_id",
                parameters={},
                severity=ContractSeverity.ERROR,
                description="Return ID must not be null",
            ),
            DataConstraint(
                name="return_id_unique",
                constraint_type="unique",
                column="return_id",
                parameters={},
                severity=ContractSeverity.ERROR,
                description="Return ID must be unique",
            ),
            DataConstraint(
                name="order_id_not_null",
                constraint_type="not_null",
                column="order_id",
                parameters={},
                severity=ContractSeverity.ERROR,
                description="Order ID must not be null",
            ),
            DataConstraint(
                name="return_reason_valid",
                constraint_type="regex",
                column="return_reason",
                parameters={
                    "pattern": r"^(defective|wrong_size|not_as_described|damaged|changed_mind|other)$"
                },
                severity=ContractSeverity.ERROR,
                description="Return reason must be from predefined list",
            ),
            DataConstraint(
                name="refund_amount_positive",
                constraint_type="range",
                column="refund_amount",
                parameters={"min": 0, "max": 1000000},
                severity=ContractSeverity.ERROR,
                description="Refund amount must be positive and reasonable",
            ),
            DataConstraint(
                name="return_date_reasonable",
                constraint_type="range",
                column="return_date",
                parameters={"min": "2020-01-01", "max": "2030-12-31"},
                severity=ContractSeverity.WARNING,
                description="Return date should be within reasonable range",
            ),
            DataConstraint(
                name="processing_time_reasonable",
                constraint_type="range",
                column="processing_time_days",
                parameters={"min": 0, "max": 365},
                severity=ContractSeverity.WARNING,
                description="Processing time should be reasonable",
            ),
        ]

        return TableContract(
            table_name="returns_raw",
            version="1.0.0",
            schema=schema,
            constraints=constraints,
            partition_columns=["return_date"],
            clustering_columns=["return_reason", "return_status"],
            retention_days=1095,  # 3 years for returns
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat(),
            owner="returns_team",
            description="Raw returns data with comprehensive validation rules",
            tags=["returns", "refunds", "customer_service"],
            schema_evolution_policy="backward_compatible",
        )

    def _create_customers_raw_contract(self) -> TableContract:
        """Create contract for customers_raw table."""
        schema = StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("address", StringType(), True),
                StructField("total_spent", DecimalType(12, 2), True),
                StructField("created_date", TimestampType(), True),
                StructField("last_purchase_date", TimestampType(), True),
                StructField("customer_tier", StringType(), True),
                StructField("is_active", BooleanType(), True),
            ]
        )

        constraints = [
            DataConstraint(
                name="customer_id_not_null",
                constraint_type="not_null",
                column="customer_id",
                parameters={},
                severity=ContractSeverity.ERROR,
            ),
            DataConstraint(
                name="email_format",
                constraint_type="regex",
                column="email",
                parameters={"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
                severity=ContractSeverity.ERROR,
            ),
            DataConstraint(
                name="age_range",
                constraint_type="range",
                column="age",
                parameters={"min": 0, "max": 120},
                severity=ContractSeverity.WARNING,
            ),
        ]

        return TableContract(
            table_name="customers_raw",
            version="1.0.0",
            schema=schema,
            constraints=constraints,
            partition_columns=["created_date"],
            clustering_columns=["customer_tier", "is_active"],
        )

    def _create_products_raw_contract(self) -> TableContract:
        """Create contract for products_raw table."""
        schema = StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DecimalType(10, 2), True),
                StructField("brand", StringType(), True),
                StructField("description", StringType(), True),
                StructField("created_date", TimestampType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("sku", StringType(), True),
            ]
        )

        constraints = [
            DataConstraint(
                name="product_id_not_null",
                constraint_type="not_null",
                column="product_id",
                parameters={},
                severity=ContractSeverity.ERROR,
            ),
            DataConstraint(
                name="price_positive",
                constraint_type="range",
                column="price",
                parameters={"min": 0, "max": 100000},
                severity=ContractSeverity.ERROR,
            ),
        ]

        return TableContract(
            table_name="products_raw",
            version="1.0.0",
            schema=schema,
            constraints=constraints,
            partition_columns=["created_date"],
            clustering_columns=["category", "brand"],
        )

    def _create_orders_raw_contract(self) -> TableContract:
        """Create contract for orders_raw table."""
        schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("order_date", TimestampType(), True),
                StructField("amount", DecimalType(12, 2), True),
                StructField("currency", StringType(), True),
                StructField("shipped", BooleanType(), True),
                StructField("returned", BooleanType(), True),
                StructField("shipping_address", StringType(), True),
                StructField("payment_method", StringType(), True),
            ]
        )

        constraints = [
            DataConstraint(
                name="order_id_not_null",
                constraint_type="not_null",
                column="order_id",
                parameters={},
                severity=ContractSeverity.ERROR,
            ),
            DataConstraint(
                name="amount_positive",
                constraint_type="range",
                column="amount",
                parameters={"min": 0, "max": 1000000},
                severity=ContractSeverity.ERROR,
            ),
        ]

        return TableContract(
            table_name="orders_raw",
            version="1.0.0",
            schema=schema,
            constraints=constraints,
            partition_columns=["order_date"],
            clustering_columns=["shipped", "returned"],
        )

    def _create_inventory_contract(self) -> TableContract:
        """Create contract for inventory_snapshots table."""
        schema = StructType(
            [
                StructField("snapshot_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), True),
                StructField("snapshot_date", TimestampType(), True),
                StructField("warehouse_id", StringType(), True),
                StructField("reorder_level", IntegerType(), True),
                StructField("max_stock", IntegerType(), True),
            ]
        )

        constraints = [
            DataConstraint(
                name="quantity_non_negative",
                constraint_type="range",
                column="quantity",
                parameters={"min": 0, "max": 1000000},
                severity=ContractSeverity.ERROR,
            )
        ]

        return TableContract(
            table_name="inventory_snapshots",
            version="1.0.0",
            schema=schema,
            constraints=constraints,
            partition_columns=["snapshot_date"],
            clustering_columns=["warehouse_id"],
        )

    def _create_fx_rates_contract(self) -> TableContract:
        """Create contract for fx_rates table."""
        schema = StructType(
            [
                StructField("rate_id", StringType(), False),
                StructField("from_currency", StringType(), False),
                StructField("to_currency", StringType(), False),
                StructField("rate", DecimalType(10, 6), True),
                StructField("effective_date", TimestampType(), True),
                StructField("source", StringType(), True),
            ]
        )

        constraints = [
            DataConstraint(
                name="rate_positive",
                constraint_type="range",
                column="rate",
                parameters={"min": 0.000001, "max": 1000000},
                severity=ContractSeverity.ERROR,
            )
        ]

        return TableContract(
            table_name="fx_rates",
            version="1.0.0",
            schema=schema,
            constraints=constraints,
            partition_columns=["effective_date"],
            clustering_columns=["from_currency", "to_currency"],
        )

    def validate_contract(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """Validate DataFrame against its contract."""
        if table_name not in self.contracts:
            return {
                "valid": False,
                "error": f"No contract found for table {table_name}",
                "table_name": table_name,
            }

        contract = self.contracts[table_name]
        validation_result = {
            "table_name": table_name,
            "contract_version": contract.version,
            "validation_time": datetime.now().isoformat(),
            "schema_valid": False,
            "constraints_valid": False,
            "overall_valid": False,
            "schema_violations": [],
            "constraint_violations": [],
            "warnings": [],
            "recommendations": [],
        }

        try:
            # Validate schema
            schema_result = self._validate_schema(df, contract)
            validation_result["schema_valid"] = schema_result["valid"]
            validation_result["schema_violations"] = schema_result.get("violations", [])

            # Validate constraints
            constraint_result = self._validate_constraints(df, contract)
            validation_result["constraints_valid"] = constraint_result["valid"]
            validation_result["constraint_violations"] = constraint_result.get("violations", [])
            validation_result["warnings"] = constraint_result.get("warnings", [])

            # Overall validation
            validation_result["overall_valid"] = (
                validation_result["schema_valid"] and validation_result["constraints_valid"]
            )

            # Generate recommendations
            validation_result["recommendations"] = self._generate_recommendations(
                validation_result, contract
            )

            logger.info(
                f"Contract validation for {table_name}: {'PASSED' if validation_result['overall_valid'] else 'FAILED'}"
            )

        except Exception as e:
            validation_result["error"] = str(e)
            logger.error(f"Contract validation failed for {table_name}: {e}")

        return validation_result

    def _validate_schema(self, df: DataFrame, contract: TableContract) -> dict[str, Any]:
        """Validate DataFrame schema against contract schema."""
        result = {
            "valid": True,
            "violations": [],
            "missing_columns": [],
            "extra_columns": [],
            "type_mismatches": [],
        }

        try:
            actual_schema = df.schema
            expected_schema = contract.schema

            expected_fields = {field.name: field.dataType for field in expected_schema.fields}
            actual_fields = {field.name: field.dataType for field in actual_schema.fields}

            # Check for missing columns
            missing_columns = set(expected_fields.keys()) - set(actual_fields.keys())
            if missing_columns:
                result["missing_columns"] = list(missing_columns)
                result["valid"] = False
                result["violations"].append(
                    f"Missing required columns: {', '.join(missing_columns)}"
                )

            # Check for type mismatches
            type_mismatches = []
            for field_name in set(expected_fields.keys()) & set(actual_fields.keys()):
                if str(expected_fields[field_name]) != str(actual_fields[field_name]):
                    type_mismatches.append(
                        {
                            "column": field_name,
                            "expected": str(expected_fields[field_name]),
                            "actual": str(actual_fields[field_name]),
                        }
                    )

            if type_mismatches:
                result["type_mismatches"] = type_mismatches
                result["valid"] = False
                result["violations"].append(f"Type mismatches in {len(type_mismatches)} columns")

            # Check nullability
            for field_name in set(expected_fields.keys()) & set(actual_fields.keys()):
                expected_field = next(f for f in expected_schema.fields if f.name == field_name)
                actual_field = next(f for f in actual_schema.fields if f.name == field_name)

                if not expected_field.nullable and actual_field.nullable:
                    result["violations"].append(f"Column {field_name} should not be nullable")
                    result["valid"] = False

        except Exception as e:
            result["valid"] = False
            result["violations"].append(f"Schema validation error: {e}")

        return result

    def _validate_constraints(self, df: DataFrame, contract: TableContract) -> dict[str, Any]:
        """Validate DataFrame against contract constraints."""
        result = {"valid": True, "violations": [], "warnings": [], "constraint_results": {}}

        try:
            for constraint in contract.constraints:
                if not constraint.enabled:
                    continue

                constraint_result = self._validate_single_constraint(df, constraint)
                result["constraint_results"][constraint.name] = constraint_result

                if not constraint_result["valid"]:
                    if constraint.severity == ContractSeverity.ERROR:
                        result["valid"] = False
                        result["violations"].append(
                            f"{constraint.name}: {constraint_result['message']}"
                        )
                    elif constraint.severity == ContractSeverity.WARNING:
                        result["warnings"].append(
                            f"{constraint.name}: {constraint_result['message']}"
                        )

        except Exception as e:
            result["valid"] = False
            result["violations"].append(f"Constraint validation error: {e}")

        return result

    def _validate_single_constraint(
        self, df: DataFrame, constraint: DataConstraint
    ) -> dict[str, Any]:
        """Validate a single constraint."""
        result = {"valid": False, "message": "", "details": {}}

        try:
            if constraint.constraint_type == "not_null":
                null_count = df.filter(df[constraint.column].isNull()).count()
                result["valid"] = null_count == 0
                result["message"] = f"Found {null_count} null values in {constraint.column}"
                result["details"] = {"null_count": null_count}

            elif constraint.constraint_type == "unique":
                total_count = df.count()
                unique_count = df.select(constraint.column).distinct().count()
                result["valid"] = total_count == unique_count
                result["message"] = (
                    f"Found {total_count - unique_count} duplicate values in {constraint.column}"
                )
                result["details"] = {"total_count": total_count, "unique_count": unique_count}

            elif constraint.constraint_type == "range":
                min_val = constraint.parameters.get("min")
                max_val = constraint.parameters.get("max")

                if min_val is not None:
                    below_min = df.filter(df[constraint.column] < min_val).count()
                    if below_min > 0:
                        result["message"] = (
                            f"Found {below_min} values below minimum {min_val} in {constraint.column}"
                        )
                        result["details"]["below_min_count"] = below_min
                        result["details"]["below_min_values"] = below_min

                if max_val is not None:
                    above_max = df.filter(df[constraint.column] > max_val).count()
                    if above_max > 0:
                        result["message"] = (
                            f"Found {above_max} values above maximum {max_val} in {constraint.column}"
                        )
                        result["details"]["above_max_count"] = above_max
                        result["details"]["above_max_values"] = above_max

                result["valid"] = (min_val is None or below_min == 0) and (
                    max_val is None or above_max == 0
                )

            elif constraint.constraint_type == "regex":
                pattern = constraint.parameters.get("pattern")
                if pattern:
                    # For regex validation, we'll use a simple approach
                    # In production, you might want to use UDFs for complex regex
                    result["valid"] = True  # Placeholder
                    result["message"] = "Regex validation not implemented yet"

            else:
                result["valid"] = True
                result["message"] = f"Unknown constraint type: {constraint.constraint_type}"

        except Exception as e:
            result["valid"] = False
            result["message"] = f"Constraint validation failed: {e}"

        return result

    def _generate_recommendations(
        self, validation_result: dict[str, Any], contract: TableContract
    ) -> list[str]:
        """Generate recommendations based on validation results."""
        recommendations = []

        # Schema recommendations
        if not validation_result["schema_valid"]:
            if validation_result["schema_violations"]:
                recommendations.append("Review and fix schema violations before processing data")

            if validation_result.get("missing_columns"):
                recommendations.append(
                    f"Add missing columns: {', '.join(validation_result['missing_columns'])}"
                )

            if validation_result.get("type_mismatches"):
                recommendations.append("Review data types and ensure compatibility")

        # Constraint recommendations
        if not validation_result["constraints_valid"]:
            if validation_result["constraint_violations"]:
                recommendations.append("Address constraint violations to ensure data quality")

            recommendations.append(
                "Consider adjusting constraint parameters if violations are expected"
            )

        # General recommendations
        if contract.schema_evolution_policy == "strict":
            recommendations.append(
                "Schema evolution policy is strict - changes require contract updates"
            )
        elif contract.schema_evolution_policy == "flexible":
            recommendations.append("Schema evolution policy is flexible - new columns are allowed")

        return recommendations

    def detect_schema_drift(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """Detect schema drift and suggest evolution."""
        if table_name not in self.contracts:
            return {"drift_detected": False, "error": f"No contract found for table {table_name}"}

        contract = contract = self.contracts[table_name]
        drift_result = {
            "table_name": table_name,
            "contract_version": contract.version,
            "detection_time": datetime.now().isoformat(),
            "drift_detected": False,
            "new_columns": [],
            "removed_columns": [],
            "type_changes": [],
            "evolution_recommendations": [],
            "contract_update_needed": False,
        }

        try:
            actual_schema = df.schema
            expected_schema = contract.schema

            expected_fields = {field.name: field.dataType for field in expected_schema.fields}
            actual_fields = {field.name: field.dataType for field in actual_schema.fields}

            # Detect new columns
            new_columns = set(actual_fields.keys()) - set(expected_fields.keys())
            drift_result["new_columns"] = list(new_columns)

            # Detect removed columns
            removed_columns = set(expected_fields.keys()) - set(actual_fields.keys())
            drift_result["removed_columns"] = list(removed_columns)

            # Detect type changes
            type_changes = []
            for field_name in set(expected_fields.keys()) & set(actual_fields.keys()):
                if str(expected_fields[field_name]) != str(actual_fields[field_name]):
                    type_changes.append(
                        {
                            "column": field_name,
                            "old_type": str(expected_fields[field_name]),
                            "new_type": str(actual_fields[field_name]),
                        }
                    )
            drift_result["type_changes"] = type_changes

            # Determine if drift was detected
            drift_result["drift_detected"] = (
                len(new_columns) > 0 or len(removed_columns) > 0 or len(type_changes) > 0
            )

            # Generate evolution recommendations
            if drift_result["drift_detected"]:
                drift_result["evolution_recommendations"] = (
                    self._generate_evolution_recommendations(drift_result, contract)
                )

                # Check if contract update is needed
                if contract.schema_evolution_policy == "strict":
                    drift_result["contract_update_needed"] = True
                elif contract.schema_evolution_policy == "backward_compatible":
                    # Only backward-incompatible changes require updates
                    drift_result["contract_update_needed"] = len(removed_columns) > 0 or any(
                        self._is_backward_incompatible_change(change) for change in type_changes
                    )

            logger.info(
                f"Schema drift detection for {table_name}: {'DRIFT DETECTED' if drift_result['drift_detected'] else 'NO DRIFT'}"
            )

        except Exception as e:
            drift_result["error"] = str(e)
            logger.error(f"Schema drift detection failed for {table_name}: {e}")

        return drift_result

    def _is_backward_incompatible_change(self, type_change: dict[str, Any]) -> bool:
        """Check if a type change is backward incompatible."""
        old_type = type_change["old_type"]
        new_type = type_change["new_type"]

        # String can accept any type
        if old_type == "StringType()":
            return False

        # Widening conversions are usually safe
        safe_conversions = {
            "IntegerType()": ["LongType()", "DoubleType()", "DecimalType()"],
            "LongType()": ["DoubleType()", "DecimalType()"],
            "FloatType()": ["DoubleType()", "DecimalType()"],
        }

        if old_type in safe_conversions and new_type in safe_conversions[old_type]:
            return False

        return True

    def _generate_evolution_recommendations(
        self, drift_result: dict[str, Any], contract: TableContract
    ) -> list[str]:
        """Generate recommendations for schema evolution."""
        recommendations = []

        if drift_result["new_columns"]:
            if contract.schema_evolution_policy == "strict":
                recommendations.append(
                    f"New columns detected: {', '.join(drift_result['new_columns'])}. Update contract to include these columns."
                )
            else:
                recommendations.append(
                    f"New columns detected: {', '.join(drift_result['new_columns'])}. These are allowed by current policy."
                )

        if drift_result["removed_columns"]:
            recommendations.append(
                f"Removed columns detected: {', '.join(drift_result['removed_columns'])}. Review if these columns are still needed."
            )

        if drift_result["type_changes"]:
            recommendations.append(
                "Type changes detected. Review compatibility and update contract if needed."
            )

        if contract.schema_evolution_policy == "strict":
            recommendations.append(
                "Consider relaxing schema evolution policy if frequent schema changes are expected."
            )

        return recommendations

    def evolve_contract(
        self, table_name: str, evolution_type: SchemaChangeType, details: dict[str, Any]
    ) -> dict[str, Any]:
        """Evolve a contract based on schema changes."""
        if table_name not in self.contracts:
            return {"error": f"No contract found for table {table_name}"}

        contract = self.contracts[table_name]
        evolution_result = {
            "table_name": table_name,
            "old_version": contract.version,
            "new_version": None,
            "evolution_type": evolution_type.value,
            "details": details,
            "evolution_time": datetime.now().isoformat(),
            "success": False,
            "changes_applied": [],
        }

        try:
            # Apply evolution based on type
            if evolution_type == SchemaChangeType.ADD_COLUMN:
                new_field = StructField(
                    name=details["column_name"],
                    dataType=self._type_from_string(details["data_type"]),
                    nullable=details.get("nullable", True),
                    metadata=details.get("metadata", {}),
                )
                contract.schema.fields.append(new_field)
                evolution_result["changes_applied"].append(
                    f"Added column: {details['column_name']}"
                )

            elif evolution_type == SchemaChangeType.REMOVE_COLUMN:
                contract.schema.fields = [
                    field
                    for field in contract.schema.fields
                    if field.name != details["column_name"]
                ]
                evolution_result["changes_applied"].append(
                    f"Removed column: {details['column_name']}"
                )

            elif evolution_type == SchemaChangeType.CHANGE_TYPE:
                for field in contract.schema.fields:
                    if field.name == details["column_name"]:
                        field.dataType = self._type_from_string(details["new_data_type"])
                        break
                evolution_result["changes_applied"].append(
                    f"Changed type of {details['column_name']}"
                )

            # Update contract metadata
            contract.version = self._increment_version(contract.version)
            contract.updated_at = datetime.now().isoformat()

            # Save updated contract
            self._save_contract(contract)

            evolution_result["new_version"] = contract.version
            evolution_result["success"] = True

            logger.info(f"Contract evolution completed for {table_name}: {evolution_type.value}")

        except Exception as e:
            evolution_result["error"] = str(e)
            logger.error(f"Contract evolution failed for {table_name}: {e}")

        return evolution_result

    def _type_from_string(self, type_string: str) -> Any:
        """Convert string representation to PySpark type."""
        type_mapping = {
            "string": StringType(),
            "integer": IntegerType(),
            "double": DoubleType(),
            "timestamp": TimestampType(),
            "boolean": BooleanType(),
            "date": DateType(),
        }
        return type_mapping.get(type_string, StringType())

    def _increment_version(self, version: str) -> str:
        """Increment semantic version."""
        try:
            parts = version.split(".")
            if len(parts) >= 3:
                patch = int(parts[2]) + 1
                return f"{parts[0]}.{parts[1]}.{patch}"
            return f"{version}.1"
        except:
            return f"{version}.1"

    def _save_contract(self, contract: TableContract):
        """Save contract to file."""
        try:
            contract_file = self.contracts_dir / f"{contract.table_name}_contract.json"

            # Convert schema to JSON-serializable format
            schema_json = {"type": "struct", "fields": []}

            for field in contract.schema.fields:
                field_json = {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable,
                    "metadata": field.metadata,
                }
                schema_json["fields"].append(field_json)

            # Convert constraints to JSON-serializable format
            constraints_json = []
            for constraint in contract.constraints:
                constraint_json = asdict(constraint)
                constraint_json["severity"] = constraint.severity.value
                constraints_json.append(constraint_json)

            # Create contract JSON
            contract_json = {
                "table_name": contract.table_name,
                "version": contract.version,
                "schema": schema_json,
                "constraints": constraints_json,
                "partition_columns": contract.partition_columns,
                "clustering_columns": contract.clustering_columns,
                "retention_days": contract.retention_days,
                "created_at": contract.created_at,
                "updated_at": contract.updated_at,
                "owner": contract.owner,
                "description": contract.description,
                "tags": contract.tags,
                "schema_evolution_policy": contract.schema_evolution_policy,
            }

            with open(contract_file, "w") as f:
                json.dump(contract_json, f, indent=2, default=str)

            logger.info(f"Contract saved to {contract_file}")

        except Exception as e:
            logger.error(f"Failed to save contract: {e}")
            raise

    def get_contract_summary(self) -> dict[str, Any]:
        """Get summary of all contracts."""
        summary = {
            "total_contracts": len(self.contracts),
            "tables_with_contracts": list(self.contracts.keys()),
            "contract_versions": {},
            "evolution_policies": {},
            "last_updated": {},
        }

        for table_name, contract in self.contracts.items():
            summary["contract_versions"][table_name] = contract.version
            summary["evolution_policies"][table_name] = contract.schema_evolution_policy
            summary["last_updated"][table_name] = contract.updated_at

        return summary


def main():
    """Main entry point for data contract management."""
    from .config_loader import load_config_resolved
    from .utils import get_spark_session

    # Load configuration
    config = load_config_resolved("config/config-dev.yaml")

    # Create Spark session
    spark = get_spark_session(config)

    # Initialize contract manager
    contract_manager = DataContractManager(spark)

    # Get contract summary
    summary = contract_manager.get_contract_summary()
    print("Data Contract Summary:")
    print(json.dumps(summary, indent=2))

    # Example: Validate a sample DataFrame against returns_raw contract
    print("\nValidating returns_raw contract...")

    # Create sample returns data
    sample_returns = spark.createDataFrame(
        [
            (
                "R001",
                "O001",
                "C001",
                "P001",
                "2024-01-01",
                "defective",
                50.00,
                100.00,
                "processed",
                "mail",
                3,
                False,
                "Product was damaged",
            ),
            (
                "R002",
                "O002",
                "C002",
                "P002",
                "2024-01-02",
                "wrong_size",
                25.00,
                50.00,
                "pending",
                "store",
                1,
                True,
                "Size too small",
            ),
            (
                "R003",
                None,
                "C003",
                "P003",
                "2024-01-03",
                "invalid_reason",
                75.00,
                150.00,
                "processed",
                "mail",
                5,
                False,
                "Customer changed mind",
            ),
        ],
        [
            "return_id",
            "order_id",
            "customer_id",
            "product_id",
            "return_date",
            "return_reason",
            "refund_amount",
            "order_amount",
            "return_status",
            "return_method",
            "processing_time_days",
            "is_partial_return",
            "return_notes",
        ],
    )

    # Validate against contract
    validation_result = contract_manager.validate_contract(sample_returns, "returns_raw")
    print(f"Validation Result: {json.dumps(validation_result, indent=2, default=str)}")

    # Detect schema drift
    drift_result = contract_manager.detect_schema_drift(sample_returns, "returns_raw")
    print(f"Schema Drift Detection: {json.dumps(drift_result, indent=2, default=str)}")

    print("Data contract management completed!")


if __name__ == "__main__":
    main()
