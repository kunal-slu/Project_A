"""
Schema contract validation and enforcement.

Provides production-ready schema alignment, validation, and error handling.
"""

import json
import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


def load_schema_contract(contract_path: str) -> dict[str, Any]:
    """
    Load a schema contract from JSON file.

    Args:
        contract_path: Path to schema contract JSON file

    Returns:
        Schema contract dictionary
    """
    try:
        with open(contract_path) as f:
            contract = json.load(f)
        logger.info(f"✅ Loaded schema contract: {contract_path}")
        return contract
    except Exception as e:
        logger.error(f"❌ Failed to load schema contract {contract_path}: {e}")
        raise


def contract_to_struct_type(contract: dict[str, Any]) -> T.StructType:
    """
    Convert schema contract to PySpark StructType.

    Args:
        contract: Schema contract dictionary

    Returns:
        PySpark StructType
    """
    fields = []

    for col_name, col_def in contract.get("columns", {}).items():
        py_type = _py_type_mapping(col_def.get("type", "string"))
        nullable = col_def.get("nullable", True)

        fields.append(T.StructField(col_name, py_type, nullable))

    return T.StructType(fields)


def align_to_schema(
    df: DataFrame,
    struct: T.StructType,
    required_cols: list[str],
    extra_cols_to_metadata: bool = True,
) -> tuple[DataFrame, dict[str, Any]]:
    """
    Align DataFrame to schema contract with validation.

    This is the core production function for schema enforcement:
    - Adds missing columns as nulls
    - Casts columns to correct types
    - Moves extra columns to metadata
    - Validates required columns are not null

    Args:
        df: Input DataFrame
        struct: Expected schema (StructType)
        required_cols: List of required columns (must not be null)
        extra_cols_to_metadata: If True, move extra columns to metadata_extra JSON field

    Returns:
        Tuple of (aligned DataFrame, DQ dictionary with null counts)
    """
    logger.info("Aligning DataFrame to schema contract...")
    logger.info(f"  Input columns: {df.columns}")
    logger.info(f"  Expected columns: {struct.fieldNames()}")

    # Track DQ issues
    dq = {"columns_added": 0, "columns_removed": 0, "required_null_counts": {}, "extra_columns": []}

    # 1. Add missing columns as nulls with correct type
    for field in struct.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
            dq["columns_added"] += 1
            logger.debug(f"  + Added missing column: {field.name} as {field.dataType}")

    # 2. Handle extra columns
    extra_cols = set(df.columns) - set(struct.fieldNames())
    if extra_cols and extra_cols_to_metadata:
        # Move extra columns to metadata_extra JSON field
        logger.warning(f"  Found extra columns: {extra_cols} - moving to metadata_extra")
        dq["extra_columns"] = list(extra_cols)

        # Create struct of extra columns
        extra_struct_cols = [F.col(c).alias(c) for c in extra_cols]
        df = df.withColumn("metadata_extra", F.to_json(F.struct(*extra_struct_cols)))

        # Drop original extra columns
        df = df.drop(*extra_cols)
        dq["columns_removed"] = len(extra_cols)

    # 3. Cast all columns to correct types
    for field in struct.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            logger.debug(f"  ✓ Cast {field.name} to {field.dataType}")

    # 4. Validate required columns are not null
    for col_name in required_cols:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            dq["required_null_counts"][col_name] = null_count
            logger.warning(f"  ⚠️  {col_name} has {null_count} null values (required)")

    # 5. Reorder columns to match schema
    ordered_cols = [field.name for field in struct.fields]
    if "metadata_extra" not in ordered_cols and "metadata_extra" in df.columns:
        ordered_cols.append("metadata_extra")
    df = df.select(*ordered_cols)

    logger.info("✅ Schema alignment complete")
    logger.info(f"  Columns added: {dq['columns_added']}")
    logger.info(f"  Columns removed: {dq['columns_removed']}")
    logger.info(f"  Required null counts: {sum(dq['required_null_counts'].values())}")

    return df, dq


def validate_and_quarantine(
    spark: SparkSession,
    df: DataFrame,
    contract: dict[str, Any],
    error_lane_path: str | None = None,
) -> tuple[DataFrame, DataFrame, dict[str, Any]]:
    """
    Validate DataFrame against contract and quarantine bad rows.

    This implements the P0 error lane pattern:
    - Validates against schema
    - Identifies bad rows
    - Writes bad rows to error lane
    - Returns clean DataFrame

    Args:
        spark: SparkSession
        df: Input DataFrame
        contract: Schema contract dictionary
        error_lane_path: Path to write error rows (s3://bucket/_errors/table/dd=YYYY-MM-DD)

    Returns:
        Tuple of (clean DataFrame, quarantined DataFrame, validation results)
    """
    logger.info("Starting validation and quarantine process...")

    # Get schema and required columns
    struct = contract_to_struct_type(contract)
    required_cols = contract.get("required_columns", [])

    # Align to schema
    aligned_df, dq = align_to_schema(df, struct, required_cols)

    # Identify bad rows (rows with nulls in required columns)
    bad_rows_mask = None
    for col_name in required_cols:
        col_mask = F.col(col_name).isNull()
        if bad_rows_mask is None:
            bad_rows_mask = col_mask
        else:
            bad_rows_mask = bad_rows_mask | col_mask

    # Split into clean and bad
    if bad_rows_mask is not None:
        clean_df = aligned_df.filter(~bad_rows_mask)
        quarantined_df = aligned_df.filter(bad_rows_mask)
    else:
        clean_df = aligned_df
        quarantined_df = spark.createDataFrame([], aligned_df.schema)

    quarantine_count = quarantined_df.count()

    # Write quarantined rows to error lane if path provided
    if quarantine_count > 0 and error_lane_path:
        logger.warning(f"⚠️  Quarantining {quarantine_count} bad rows to {error_lane_path}")
        quarantined_df.write.format("json").mode("append").option(
            "timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'"
        ).save(error_lane_path)
        logger.info("✅ Quarantined rows written to error lane")

    validation_results = {
        "total_rows": df.count(),
        "clean_rows": clean_df.count(),
        "quarantined_rows": quarantine_count,
        "pass_rate": (clean_df.count() / df.count()) if df.count() > 0 else 0.0,
        "dq_issues": dq,
    }

    logger.info(
        f"✅ Validation complete: {validation_results['clean_rows']}/{validation_results['total_rows']} rows passed"
    )

    return clean_df, quarantined_df, validation_results


def _py_type_mapping(type_str: str) -> T.DataType:
    """
    Map string type to PySpark DataType.

    Args:
        type_str: Type string (e.g., "string", "decimal(10,2)")

    Returns:
        PySpark DataType
    """
    type_lower = type_str.lower().strip()

    # Handle parameterized types
    if type_lower.startswith("decimal") or type_lower.startswith("numeric"):
        # Extract precision and scale
        import re

        match = re.search(r"decimal\((\d+),(\d+)\)", type_lower)
        if match:
            precision, scale = int(match.group(1)), int(match.group(2))
            return T.DecimalType(precision, scale)
        return T.DecimalType(10, 2)  # Default

    if type_lower.startswith("varchar"):
        import re

        match = re.search(r"varchar\((\d+)\)", type_lower)
        if match:
            # Spark doesn't support varchar length, use string
            return T.StringType()
        return T.StringType()

    # Simple type mappings
    type_map = {
        "string": T.StringType(),
        "int": T.IntegerType(),
        "integer": T.IntegerType(),
        "bigint": T.LongType(),
        "long": T.LongType(),
        "float": T.FloatType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "bool": T.BooleanType(),
        "date": T.DateType(),
        "timestamp": T.TimestampType(),
        "array": T.ArrayType(T.StringType()),
        "map": T.MapType(T.StringType(), T.StringType()),
        "struct": T.StructType(),
    }

    return type_map.get(type_lower, T.StringType())


def add_metadata_columns(
    df: DataFrame, batch_id: str, source_system: str, run_date: str
) -> DataFrame:
    """
    Add standard run metadata columns to DataFrame.

    Args:
        df: Input DataFrame
        batch_id: Unique batch identifier (UUID)
        source_system: Source system name (e.g., 'snowflake', 'redshift')
        run_date: Run date YYYY-MM-DD format

    Returns:
        DataFrame with metadata columns added
    """
    # Check if columns already exist
    existing_cols = set(df.columns)
    needs_ingest_ts = "_ingest_ts" not in existing_cols
    needs_batch_id = "_batch_id" not in existing_cols
    needs_source = "_source_system" not in existing_cols
    needs_run_date = "_run_date" not in existing_cols

    # Add metadata columns
    if needs_ingest_ts:
        df = df.withColumn("_ingest_ts", F.current_timestamp())
    if needs_batch_id:
        df = df.withColumn("_batch_id", F.lit(batch_id))
    if needs_source:
        df = df.withColumn("_source_system", F.lit(source_system))
    if needs_run_date:
        df = df.withColumn("_run_date", F.lit(run_date))

    return df
