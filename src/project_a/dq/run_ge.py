"""
Great Expectations checkpoint runner for enterprise data quality.
"""

import json
import logging
from pathlib import Path
from typing import Any

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context import get_context
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def run_checkpoint(checkpoint_name: str, context_path: str | None = None) -> dict[str, Any]:
    """
    Run Great Expectations checkpoint.

    Args:
        checkpoint_name: Name of the checkpoint to run
        context_path: Optional path to GE context

    Returns:
        dict: Checkpoint results
    """
    try:
        if context_path:
            ctx = get_context(context_root_dir=context_path)
        else:
            ctx = get_context()

        checkpoint = SimpleCheckpoint(name=checkpoint_name, data_context=ctx)
        result = checkpoint.run()

        if not result["success"]:
            logger.error(f"GE checkpoint failed: {checkpoint_name}")
            logger.error(f"Validation results: {result}")
            raise SystemExit(f"Data quality check failed: {checkpoint_name}")

        logger.info(f"GE checkpoint passed: {checkpoint_name}")
        return result

    except Exception as e:
        logger.error(f"GE checkpoint execution failed: {e}")
        raise


def run_contract_validation(contract_path: str, data_path: str) -> dict[str, Any]:
    """
    Run contract-based validation using Great Expectations.

    Args:
        contract_path: Path to contract YAML file
        data_path: Path to data to validate

    Returns:
        dict: Validation results
    """
    contract_file = Path(contract_path)
    if not contract_file.exists():
        raise FileNotFoundError(f"Contract file not found: {contract_path}")

    spark = SparkSession.builder.appName("contract_validation").master("local[*]").getOrCreate()
    try:
        try:
            df = spark.read.format("delta").load(data_path)
            source_format = "delta"
        except Exception:
            df = spark.read.parquet(data_path)
            source_format = "parquet"

        errors: list[str] = []
        checked_rules: list[str] = []

        if contract_file.suffix.lower() == ".yaml":
            from project_a.contracts.runtime_contracts import load_table_contracts, validate_contract

            tables = load_table_contracts(contract_path)
            table_name = Path(data_path.rstrip("/")).name
            contract = tables.get(table_name)
            if not contract:
                # Fallback to single-table YAML contracts by taking the first entry
                contract = next(iter(tables.values()), None)
                table_name = next(iter(tables.keys()), table_name)
            if not contract:
                raise ValueError(f"No contracts found in {contract_path}")

            try:
                validate_contract(df, table_name, contract)
            except Exception as exc:
                errors.append(str(exc))

            checked_rules.extend(
                [
                    "required_columns",
                    "required_non_null",
                    "primary_key_uniqueness",
                    "ranges",
                    "relationships",
                ]
            )
        else:
            payload = json.loads(contract_file.read_text(encoding="utf-8"))
            schema = payload.get("schema", {}) if isinstance(payload, dict) else {}
            required_cols = schema.get("required", []) if isinstance(schema, dict) else []
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                errors.append(f"Missing required columns: {missing_cols}")
            for col_name in required_cols:
                if col_name in df.columns:
                    null_count = df.filter(F.col(col_name).isNull()).count()
                    if null_count > 0:
                        errors.append(f"Null violations in {col_name}: {null_count}")
            unique_keys = ((payload.get("constraints") or {}).get("unique_keys") or [])
            for key in unique_keys:
                if key in df.columns:
                    dup_count = df.groupBy(key).count().filter(F.col("count") > 1).count()
                    if dup_count > 0:
                        errors.append(f"Duplicate key violations for {key}: {dup_count}")

            checked_rules.extend(["required_columns", "required_non_null", "unique_keys"])

        success = len(errors) == 0
        result = {
            "success": success,
            "contract_path": contract_path,
            "data_path": data_path,
            "source_format": source_format,
            "checked_rules": checked_rules,
            "errors": errors,
            "row_count": df.count(),
        }
        if success:
            logger.info("Contract validation passed for %s", data_path)
        else:
            logger.error("Contract validation failed for %s: %s", data_path, errors)
        return result
    finally:
        spark.stop()
