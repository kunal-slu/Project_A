"""Schema evolution gate for curated layers.

This module enforces schema evolution rules and blocks breaking changes.
Baseline schemas are stored on disk and compared on every run.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StructType,
)

logger = logging.getLogger(__name__)


SAFE_TYPE_CHANGES: tuple[tuple[type[DataType], type[DataType]], ...] = (
    (IntegerType, LongType),
    (IntegerType, DecimalType),
    (LongType, DecimalType),
    (FloatType, DoubleType),
)


def _is_safe_type_change(old_type: DataType, new_type: DataType) -> bool:
    if type(old_type) is type(new_type):
        if isinstance(old_type, DecimalType):
            return (
                new_type.precision >= old_type.precision
                and new_type.scale >= old_type.scale
            )
        return True

    for old_cls, new_cls in SAFE_TYPE_CHANGES:
        if isinstance(old_type, old_cls) and isinstance(new_type, new_cls):
            if isinstance(new_type, DecimalType):
                return new_type.precision >= 10 and new_type.scale >= 2
            return True

    return False


def _load_baseline(path: Path) -> StructType | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text())
    return StructType.fromJson(payload)


def _save_baseline(path: Path, schema: StructType) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema.jsonValue(), indent=2))


def _compare_schema(
    actual: StructType, baseline: StructType, policy: str
) -> dict[str, Any]:
    actual_fields = {f.name: f for f in actual.fields}
    baseline_fields = {f.name: f for f in baseline.fields}

    result = {
        "policy": policy,
        "missing_columns": [],
        "new_columns": [],
        "type_changes": [],
        "nullability_changes": [],
        "breaking_changes": False,
    }

    # Missing columns are always breaking
    for name, field in baseline_fields.items():
        if name not in actual_fields:
            result["missing_columns"].append(
                {
                    "column": name,
                    "expected_type": str(field.dataType),
                    "expected_nullable": field.nullable,
                }
            )
            result["breaking_changes"] = True

    # New columns
    for name, field in actual_fields.items():
        if name not in baseline_fields:
            result["new_columns"].append(
                {
                    "column": name,
                    "actual_type": str(field.dataType),
                    "actual_nullable": field.nullable,
                }
            )
            if policy == "strict":
                result["breaking_changes"] = True
            elif policy == "backward_compatible" and field.nullable is False:
                result["breaking_changes"] = True

    # Type and nullability changes
    for name in set(actual_fields.keys()) & set(baseline_fields.keys()):
        actual_field = actual_fields[name]
        baseline_field = baseline_fields[name]

        if not _is_safe_type_change(baseline_field.dataType, actual_field.dataType):
            result["type_changes"].append(
                {
                    "column": name,
                    "expected": str(baseline_field.dataType),
                    "actual": str(actual_field.dataType),
                }
            )
            result["breaking_changes"] = True

        if baseline_field.nullable is False and actual_field.nullable is True:
            result["nullability_changes"].append(
                {
                    "column": name,
                    "expected_nullable": baseline_field.nullable,
                    "actual_nullable": actual_field.nullable,
                }
            )
            result["breaking_changes"] = True

    return result


def enforce_schema_evolution(
    df: DataFrame,
    table_name: str,
    layer: str,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = config or {}
    if not config.get("enabled", False):
        return {"enabled": False, "table": table_name, "layer": layer}

    baseline_root = Path(config.get("baseline_path", "artifacts/schema_baselines"))
    policy_map = config.get("policies", {}) or {}
    policy = str(policy_map.get(layer, config.get("policy", "strict"))).lower()
    update_baseline = bool(config.get("update_baseline", False))

    baseline_path = baseline_root / layer / f"{table_name}.json"
    baseline_schema = _load_baseline(baseline_path)

    if baseline_schema is None:
        _save_baseline(baseline_path, df.schema)
        logger.info("Schema baseline created for %s.%s at %s", layer, table_name, baseline_path)
        return {
            "enabled": True,
            "baseline_created": True,
            "table": table_name,
            "layer": layer,
            "policy": policy,
            "baseline_path": str(baseline_path),
        }

    result = _compare_schema(df.schema, baseline_schema, policy)
    result.update(
        {
            "enabled": True,
            "table": table_name,
            "layer": layer,
            "baseline_path": str(baseline_path),
        }
    )

    if result["breaking_changes"]:
        logger.error(
            "Schema evolution gate failed for %s.%s: %s",
            layer,
            table_name,
            result,
        )
        raise ValueError(f"Schema evolution breaking change detected for {layer}.{table_name}")

    if update_baseline and (result["new_columns"] or result["type_changes"]):
        _save_baseline(baseline_path, df.schema)
        result["baseline_updated"] = True
        logger.info("Schema baseline updated for %s.%s", layer, table_name)

    return result
