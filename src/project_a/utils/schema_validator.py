"""
Schema Validator - Enforce Expected Schemas

Provides schema validation utilities to prevent schema drift and ensure
data contracts are maintained across ETL pipeline layers.
"""
import logging
from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, \
    DecimalType, DateType, TimestampType, LongType

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validates DataFrames against expected schemas.
    
    Provides:
    - Column existence checks
    - Type enforcement
    - Nullability validation
    - Schema drift detection
    """
    
    # Standard schema definitions for key tables
    SCHEMAS = {
        "orders_silver": StructType([
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=True),
            StructField("order_date", DateType(), nullable=False),
            StructField("total_amount", DecimalType(10, 2), nullable=False),
            StructField("quantity", IntegerType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True)
        ]),
        "customers_silver": StructType([
            StructField("customer_id", StringType(), nullable=False),
            StructField("first_name", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
            StructField("registration_date", DateType(), nullable=True)
        ]),
        "products_silver": StructType([
            StructField("product_id", StringType(), nullable=False),
            StructField("product_name", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("price_usd", DecimalType(10, 2), nullable=True),
            StructField("cost_usd", DecimalType(10, 2), nullable=True),
            StructField("supplier_id", StringType(), nullable=True)
        ])
    }
    
    @staticmethod
    def validate_columns(df: DataFrame, expected_columns: List[str], 
                        table_name: str = "table") -> None:
        """
        Validate that DataFrame contains expected columns.
        
        Args:
            df: DataFrame to validate
            expected_columns: List of required column names
            table_name: Name for logging
            
        Raises:
            ValueError: If columns are missing
        """
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)
        
        missing = expected_set - actual_columns
        if missing:
            raise ValueError(
                f"{table_name}: Missing required columns: {sorted(missing)}"
            )
        
        extra = actual_columns - expected_set
        if extra:
            logger.warning(
                f"{table_name}: Found extra columns (not in contract): {sorted(extra)}"
            )
    
    @staticmethod
    def validate_schema(df: DataFrame, expected_schema: StructType, 
                       table_name: str = "table", strict: bool = False) -> None:
        """
        Validate DataFrame schema matches expected schema.
        
        Args:
            df: DataFrame to validate
            expected_schema: Expected StructType
            table_name: Name for logging
            strict: If True, fail on any mismatch; if False, warn only
            
        Raises:
            ValueError: If schema validation fails (strict=True)
        """
        actual_schema = df.schema
        expected_fields = {f.name: f for f in expected_schema.fields}
        actual_fields = {f.name: f for f in actual_schema.fields}
        
        issues = []
        
        # Check for missing columns
        missing = set(expected_fields.keys()) - set(actual_fields.keys())
        if missing:
            issues.append(f"Missing columns: {sorted(missing)}")
        
        # Check column types
        for col_name in set(expected_fields.keys()) & set(actual_fields.keys()):
            expected_field = expected_fields[col_name]
            actual_field = actual_fields[col_name]
            
            if type(expected_field.dataType) != type(actual_field.dataType):
                issues.append(
                    f"Column '{col_name}': Expected {expected_field.dataType}, "
                    f"got {actual_field.dataType}"
                )
            
            # Check nullability
            if not expected_field.nullable and actual_field.nullable:
                issues.append(
                    f"Column '{col_name}': Expected non-nullable, got nullable"
                )
        
        if issues:
            error_msg = f"{table_name} schema validation failed:\n" + "\n".join(issues)
            if strict:
                raise ValueError(error_msg)
            else:
                logger.warning(error_msg)
    
    @staticmethod
    def validate_primary_key(df: DataFrame, pk_column: str, 
                            table_name: str = "table") -> None:
        """
        Validate primary key constraints.
        
        Args:
            df: DataFrame to validate
            pk_column: Primary key column name
            table_name: Name for logging
            
        Raises:
            ValueError: If PK validation fails
        """
        from pyspark.sql.functions import col
        
        # Check for nulls
        null_count = df.filter(col(pk_column).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"{table_name}: Found {null_count} null values in primary key '{pk_column}'"
            )
        
        # Check for duplicates
        total_count = df.count()
        distinct_count = df.select(pk_column).distinct().count()
        
        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            raise ValueError(
                f"{table_name}: Found {duplicate_count} duplicate values in "
                f"primary key '{pk_column}'"
            )
    
    @staticmethod
    def get_schema(table_name: str) -> Optional[StructType]:
        """
        Get standard schema for a table.
        
        Args:
            table_name: Table name (e.g., 'orders_silver')
            
        Returns:
            StructType if defined, else None
        """
        return SchemaValidator.SCHEMAS.get(table_name)
    
    @staticmethod
    def apply_schema(df: DataFrame, table_name: str) -> DataFrame:
        """
        Apply and enforce standard schema on DataFrame.
        
        Args:
            df: DataFrame to apply schema to
            table_name: Table name (e.g., 'orders_silver')
            
        Returns:
            DataFrame with enforced schema
            
        Raises:
            ValueError: If schema not defined or cannot be applied
        """
        schema = SchemaValidator.get_schema(table_name)
        if schema is None:
            raise ValueError(f"No standard schema defined for '{table_name}'")
        
        # Select and cast columns according to schema
        from pyspark.sql.functions import col
        
        select_exprs = []
        for field in schema.fields:
            if field.name in df.columns:
                select_exprs.append(col(field.name).cast(field.dataType).alias(field.name))
            elif not field.nullable:
                raise ValueError(f"Required column '{field.name}' missing from DataFrame")
            else:
                # Add null column for optional missing fields
                from pyspark.sql.functions import lit
                select_exprs.append(lit(None).cast(field.dataType).alias(field.name))
        
        return df.select(*select_exprs)
