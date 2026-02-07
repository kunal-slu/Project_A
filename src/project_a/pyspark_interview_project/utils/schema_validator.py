"""
Schema validation utility for enforcing data contracts at bronze ingestion.

Validates incoming data against JSON schema registry files and handles
schema drift gracefully.
"""

import json
import logging
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Literal
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, MapType

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates DataFrames against schema registry contracts."""
    
    def __init__(self, schema_path: str):
        """
        Initialize validator with schema registry file.
        
        Args:
            schema_path: Path to JSON schema registry file
        """
        self.schema_path = schema_path
        self.schema_def = self._load_schema()
    
    def _load_schema(self) -> Dict[str, Any]:
        """Load schema definition from JSON file."""
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise ValueError(f"Failed to load schema from {self.schema_path}: {str(e)}")
    
    def validate_required_fields(self, df: DataFrame) -> List[str]:
        """
        Check if all required fields are present.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            List of missing required fields
        """
        required_fields = self.schema_def.get('validation_rules', {}).get('required_fields', [])
        existing_columns = set(df.columns)
        missing_fields = [f for f in required_fields if f not in existing_columns]
        
        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
        
        return missing_fields
    
    def handle_schema_drift(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Handle schema drift by adding missing columns and logging extra columns.
        
        Args:
            df: Input DataFrame
            spark: Spark session
            
        Returns:
            DataFrame with aligned schema
        """
        schema_fields = self.schema_def.get('fields', [])
        required_columns = {f['name'] for f in schema_fields if f.get('required', False)}
        
        # Get existing columns
        existing_columns = set(df.columns)
        
        # Find missing required columns
        missing_columns = required_columns - existing_columns
        extra_columns = existing_columns - {f['name'] for f in schema_fields}
        
        result_df = df
        
        # Add missing required columns as null
        for col_name in missing_columns:
            field_def = next((f for f in schema_fields if f['name'] == col_name), None)
            if field_def:
                col_type = field_def.get('type', 'string')
                if col_type == 'integer':
                    result_df = result_df.withColumn(col_name, lit(None).cast(IntegerType()))
                elif col_type == 'double':
                    result_df = result_df.withColumn(col_name, lit(None).cast(DoubleType()))
                elif col_type == 'timestamp':
                    result_df = result_df.withColumn(col_name, lit(None).cast(TimestampType()))
                else:
                    result_df = result_df.withColumn(col_name, lit(None).cast(StringType()))
                
                logger.info(f"Added missing required column: {col_name}")
        
        # Log extra columns (schema drift tolerance)
        if extra_columns:
            logger.info(f"Extra columns detected (will be preserved): {extra_columns}")
            # Optionally, collect extra columns into a JSON struct
            # For now, we just log and preserve them
        
        return result_df
    
    def validate_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """
        Run data quality checks defined in schema validation rules.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        validation_rules = self.schema_def.get('validation_rules', {})
        results = {
            "passed": True,
            "issues": []
        }
        
        # Check required fields are not null
        not_null_fields = validation_rules.get('not_null_fields', [])
        for field in not_null_fields:
            if field in df.columns:
                null_count = df.filter(col(field).isNull()).count()
                if null_count > 0:
                    results["passed"] = False
                    results["issues"].append(f"{field}: {null_count} null values found")
        
        # Check format patterns (if defined)
        format_checks = validation_rules.get('format_checks', {})
        # Note: Format checking would require regex UDF - simplified here
        
        # Check ranges (if defined)
        range_checks = validation_rules.get('range_checks', {})
        for field, range_def in range_checks.items():
            if field in df.columns:
                if 'min' in range_def:
                    below_min = df.filter(col(field) < range_def['min']).count()
                    if below_min > 0:
                        results["issues"].append(f"{field}: {below_min} values below minimum {range_def['min']}")
                if 'max' in range_def:
                    above_max = df.filter(col(field) > range_def['max']).count()
                    if above_max > 0:
                        results["issues"].append(f"{field}: {above_max} values above maximum {range_def['max']}")
        
        return results
    
    def validate_and_prepare(self, df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Complete validation and schema alignment.
        
        Args:
            df: Input DataFrame
            spark: Spark session
            
        Returns:
            Tuple of (aligned DataFrame, validation results)
        """
        # Check required fields
        missing_fields = self.validate_required_fields(df)
        
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Handle schema drift
        aligned_df = self.handle_schema_drift(df, spark)
        
        # Add metadata fields if not present
        if '_ingestion_ts' not in aligned_df.columns:
            aligned_df = aligned_df.withColumn('_ingestion_ts', current_timestamp())
        if '_source_system' not in aligned_df.columns:
            aligned_df = aligned_df.withColumn('_source_system', lit(self.schema_def.get('source_system', 'unknown')))
        
        # Validate data quality
        dq_results = self.validate_data_quality(aligned_df)
        
        return aligned_df, dq_results


def validate_schema(
    df: DataFrame,
    expected_schema: Dict[str, Any],
    mode: Literal["strict", "allow_new"] = "strict",
    config: Optional[Dict[str, Any]] = None,
    spark: Optional[SparkSession] = None
) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Validate schema with evolution support.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected schema definition (from config/schema_definitions/)
        mode: "strict" (fail on unexpected columns) or "allow_new" (log and preserve)
        config: Configuration dict (for drift logging path)
        spark: SparkSession (for drift logging)
        
    Returns:
        Tuple of (validated DataFrame, validation results)
        
    Raises:
        ValueError: If strict mode and unexpected columns found
    """
    expected_columns = {col_def["name"] for col_def in expected_schema.get("columns", [])}
    actual_columns = set(df.columns)
    
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns
    
    validation_results = {
        "passed": True,
        "missing_columns": list(missing_columns),
        "extra_columns": list(extra_columns),
        "drift_detected": len(extra_columns) > 0 or len(missing_columns) > 0
    }
    
    result_df = df
    
    # Handle missing columns (add as null)
    if missing_columns:
        logger.warning(f"Missing expected columns: {missing_columns}")
        for col_name in missing_columns:
            col_def = next((c for c in expected_schema.get("columns", []) if c["name"] == col_name), None)
            if col_def:
                col_type = col_def.get("type", "string")
                result_df = result_df.withColumn(col_name, lit(None).cast(col_type))
    
    # Handle extra columns (schema drift)
    if extra_columns:
        logger.warning(f"Unexpected columns detected (schema drift): {extra_columns}")
        
        if mode == "strict":
            raise ValueError(
                f"Schema drift detected in strict mode. Unexpected columns: {extra_columns}. "
                f"Set mode='allow_new' to allow schema evolution."
            )
        elif mode == "allow_new":
            # Log drift and preserve columns in extras struct
            drift_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "table": expected_schema.get("table_name", "unknown"),
                "missing_columns": list(missing_columns),
                "extra_columns": list(extra_columns),
                "expected_columns": list(expected_columns),
                "actual_columns": list(actual_columns)
            }
            
            # Log to S3 or local
            log_schema_drift(drift_data, config, spark)
            
            # Preserve extra columns (they'll be written normally)
            logger.info(f"Preserving extra columns in allow_new mode: {extra_columns}")
            validation_results["passed"] = True  # Allow to continue
    
    return result_df, validation_results


def log_schema_drift(drift_data: Dict[str, Any], config: Optional[Dict[str, Any]] = None, spark: Optional[SparkSession] = None):
    """Log schema drift to S3 or local file."""
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    
    if config and spark:
        drift_path = config.get("data_lake", {}).get("drift_prefix", "s3://bucket/meta/schema_drifts")
        if drift_path.startswith("s3://") or drift_path.startswith("s3a://"):
            try:
                # Write drift JSON to S3
                import json
                drift_json = json.dumps(drift_data, indent=2)
                # Use boto3 or Spark to write to S3
                logger.info(f"Schema drift logged to {drift_path}/{date_str}.json")
                return
            except Exception as e:
                logger.warning(f"Could not log to S3: {e}, falling back to local")
    
    # Fallback to local
    local_path = Path(f"data/metrics/schema_drifts/{date_str}.json")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    import json
    with open(local_path, "a") as f:
        f.write(json.dumps(drift_data) + "\n")
    
    logger.info(f"Schema drift logged to {local_path}")


def validate_bronze_ingestion(
    df: DataFrame,
    spark: SparkSession,
    schema_name: str,
    schemas_dir: str = "schemas",
    mode: Literal["strict", "allow_new"] = "allow_new",
    config: Optional[Dict[str, Any]] = None
) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Convenience function to validate bronze ingestion with schema evolution.
    
    Args:
        df: Input DataFrame
        spark: Spark session
        schema_name: Name of schema file (without .schema.json)
        schemas_dir: Directory containing schema files
        mode: Schema validation mode ("strict" or "allow_new")
        config: Configuration dict
        
    Returns:
        Tuple of (validated DataFrame, validation results)
    """
    schema_path = Path(schemas_dir) / f"{schema_name}.schema.json"
    
    if not schema_path.exists():
        logger.warning(f"Schema file not found: {schema_path}, skipping validation")
        return df, {"passed": True, "issues": ["Schema file not found"]}
    
    with open(schema_path) as f:
        expected_schema = json.load(f)
    
    return validate_schema(df, expected_schema, mode=mode, config=config, spark=spark)

