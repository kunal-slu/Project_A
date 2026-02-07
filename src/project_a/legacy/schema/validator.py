"""
Schema validation for bronze/silver/gold evolution and contracts.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, DataType

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Schema validator for data lakehouse layers."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def validate_bronze_schema(self, df: DataFrame, table_name: str) -> bool:
        """
        Bronze layer: permissive validation (log only).
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table
            
        Returns:
            Always True (permissive)
        """
        logger.info(f"Bronze schema validation for {table_name} (permissive)")
        logger.info(f"Schema: {df.schema}")
        return True
    
    def validate_silver_schema(self, df: DataFrame, table_name: str, existing_schema_path: Optional[str] = None) -> bool:
        """
        Silver layer: must match previous schema (allow additive nullable columns).
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table
            existing_schema_path: Path to existing schema file
            
        Returns:
            True if schema is compatible, False otherwise
        """
        logger.info(f"Silver schema validation for {table_name}")
        
        if not existing_schema_path:
            logger.info("No existing schema found, accepting new schema")
            return True
        
        try:
            # Load existing schema
            with open(existing_schema_path, 'r') as f:
                existing_schema_dict = json.load(f)
            
            existing_schema = StructType.fromJson(existing_schema_dict)
            current_schema = df.schema
            
            # Check compatibility
            compatible = self._is_schema_compatible(existing_schema, current_schema)
            
            if compatible:
                logger.info("Schema is compatible")
            else:
                logger.error("Schema is not compatible - breaking changes detected")
            
            return compatible
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False
    
    def validate_gold_schema(self, df: DataFrame, table_name: str, contract_path: str) -> bool:
        """
        Gold layer: enforce strict contract from JSON schema.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table
            contract_path: Path to schema contract JSON
            
        Returns:
            True if schema matches contract, False otherwise
        """
        logger.info(f"Gold schema validation for {table_name}")
        
        try:
            # Load contract schema
            with open(contract_path, 'r') as f:
                contract_schema_dict = json.load(f)
            
            contract_schema = StructType.fromJson(contract_schema_dict)
            current_schema = df.schema
            
            # Strict validation - schemas must match exactly
            matches = self._schemas_match_exactly(contract_schema, current_schema)
            
            if matches:
                logger.info("Schema matches contract")
            else:
                logger.error("Schema does not match contract")
                self._log_schema_differences(contract_schema, current_schema)
            
            return matches
            
        except Exception as e:
            logger.error(f"Contract validation failed: {e}")
            return False
    
    def _is_schema_compatible(self, existing: StructType, current: StructType) -> bool:
        """Check if current schema is compatible with existing (additive changes only)."""
        existing_fields = {f.name: f for f in existing.fields}
        current_fields = {f.name: f for f in current.fields}
        
        # Check for removed fields
        removed_fields = set(existing_fields.keys()) - set(current_fields.keys())
        if removed_fields:
            logger.error(f"Removed fields detected: {removed_fields}")
            return False
        
        # Check for type changes
        for field_name in existing_fields:
            if field_name in current_fields:
                existing_field = existing_fields[field_name]
                current_field = current_fields[field_name]
                
                if existing_field.dataType != current_field.dataType:
                    logger.error(f"Type change detected for {field_name}: "
                               f"{existing_field.dataType} -> {current_field.dataType}")
                    return False
        
        # New fields are allowed if they are nullable
        new_fields = set(current_fields.keys()) - set(existing_fields.keys())
        for field_name in new_fields:
            field = current_fields[field_name]
            if field.nullable:
                logger.info(f"New nullable field added: {field_name}")
            else:
                logger.error(f"New non-nullable field added: {field_name}")
                return False
        
        return True
    
    def _schemas_match_exactly(self, contract: StructType, current: StructType) -> bool:
        """Check if schemas match exactly."""
        return contract == current
    
    def _log_schema_differences(self, contract: StructType, current: StructType) -> None:
        """Log differences between contract and current schema."""
        logger.error("Schema differences:")
        logger.error(f"Contract: {contract}")
        logger.error(f"Current: {current}")
