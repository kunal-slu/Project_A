#!/usr/bin/env python3
"""
Apply data masking and PII protection for Gold layer tables.

This job ensures compliance with data governance policies by:
- Hashing or masking PII columns (email, phone, SSN, etc.)
- Creating safe-for-analytics Gold tables
- Maintaining audit trail of masking operations
- Supporting different masking strategies per data classification level
"""

import sys
import logging
import hashlib
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, hash, regexp_replace, 
    current_timestamp, concat, substring
)
from pyspark.sql.types import StringType

# Add project root to path
sys.path.append('/opt/airflow/dags/src')

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.io import read_delta, write_delta
from pyspark_interview_project.utils.config import load_config

logger = logging.getLogger(__name__)


class DataMaskingEngine:
    """Handles PII masking and data protection for Gold layer."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.masking_config = config.get('security', {}).get('data_masking', {})
        self.pii_columns = config.get('security', {}).get('data_classification', {}).get('pii_columns', [])
        self.sensitive_columns = config.get('security', {}).get('data_classification', {}).get('sensitive_columns', [])
        
    def apply_masking_strategies(self, df: DataFrame, table_name: str) -> DataFrame:
        """Apply appropriate masking strategies based on column classification."""
        
        logger.info(f"Applying data masking for table: {table_name}")
        
        # Track masking operations for audit
        masking_audit = []
        
        for column in df.columns:
            if column.lower() in self.pii_columns:
                masking_strategy = self._get_masking_strategy(column, 'pii')
                df = self._apply_column_masking(df, column, masking_strategy)
                masking_audit.append({
                    'column': column,
                    'strategy': masking_strategy,
                    'classification': 'pii',
                    'timestamp': datetime.utcnow().isoformat()
                })
                
            elif column.lower() in self.sensitive_columns:
                masking_strategy = self._get_masking_strategy(column, 'sensitive')
                df = self._apply_column_masking(df, column, masking_strategy)
                masking_audit.append({
                    'column': column,
                    'strategy': masking_strategy,
                    'classification': 'sensitive',
                    'timestamp': datetime.utcnow().isoformat()
                })
        
        # Add masking metadata columns
        df = df.withColumn("data_masking_applied", lit(True)) \
               .withColumn("masking_timestamp", current_timestamp()) \
               .withColumn("safe_for_analytics", lit(True))
        
        logger.info(f"Applied masking to {len(masking_audit)} columns in {table_name}")
        return df
    
    def _get_masking_strategy(self, column: str, classification: str) -> str:
        """Determine masking strategy based on column name and classification."""
        
        column_lower = column.lower()
        
        # Email masking
        if 'email' in column_lower:
            return 'email_hash'
        
        # Phone number masking
        elif 'phone' in column_lower or 'mobile' in column_lower:
            return 'phone_mask'
        
        # SSN masking
        elif 'ssn' in column_lower or 'social' in column_lower:
            return 'ssn_mask'
        
        # Credit card masking
        elif 'credit' in column_lower or 'card' in column_lower:
            return 'credit_card_mask'
        
        # Address masking
        elif 'address' in column_lower or 'street' in column_lower:
            return 'address_hash'
        
        # Name masking
        elif 'name' in column_lower and classification == 'pii':
            return 'name_hash'
        
        # Financial data masking
        elif classification == 'sensitive' and any(fin_term in column_lower for fin_term in ['salary', 'revenue', 'profit']):
            return 'financial_mask'
        
        # Default strategy
        else:
            return 'hash'
    
    def _apply_column_masking(self, df: DataFrame, column: str, strategy: str) -> DataFrame:
        """Apply specific masking strategy to a column."""
        
        if strategy == 'email_hash':
            # Hash email domain, mask local part
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     concat(
                         lit("user_"),
                         hash(col(column)).cast(StringType()),
                         lit("@masked.com")
                     )
                ).otherwise(col(column))
            )
            
        elif strategy == 'phone_mask':
            # Mask phone numbers: +1-XXX-XXX-XXXX
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     regexp_replace(col(column), r'\d', 'X')
                ).otherwise(col(column))
            )
            
        elif strategy == 'ssn_mask':
            # Mask SSN: XXX-XX-1234
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     regexp_replace(col(column), r'^\d{3}-\d{2}-', 'XXX-XX-')
                ).otherwise(col(column))
            )
            
        elif strategy == 'credit_card_mask':
            # Mask credit card: XXXX-XXXX-XXXX-1234
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     regexp_replace(col(column), r'^\d{4}-\d{4}-\d{4}-', 'XXXX-XXXX-XXXX-')
                ).otherwise(col(column))
            )
            
        elif strategy == 'address_hash':
            # Hash full address
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     concat(lit("addr_"), hash(col(column)).cast(StringType()))
                ).otherwise(col(column))
            )
            
        elif strategy == 'name_hash':
            # Hash names
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     concat(lit("name_"), hash(col(column)).cast(StringType()))
                ).otherwise(col(column))
            )
            
        elif strategy == 'financial_mask':
            # Round financial data to nearest thousand
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     (col(column) / 1000).cast("int") * 1000
                ).otherwise(col(column))
            )
            
        else:  # Default hash strategy
            df = df.withColumn(
                column,
                when(col(column).isNotNull(),
                     hash(col(column)).cast(StringType())
                ).otherwise(col(column))
            )
        
        return df
    
    def create_masking_audit_table(self, masking_audit: List[Dict]) -> DataFrame:
        """Create audit table for masking operations."""
        
        audit_data = []
        for audit_record in masking_audit:
            audit_data.append((
                audit_record['column'],
                audit_record['strategy'],
                audit_record['classification'],
                audit_record['timestamp'],
                str(uuid.uuid4())  # audit_id
            ))
        
        audit_schema = "audit_id STRING, column_name STRING, masking_strategy STRING, classification STRING, timestamp STRING"
        audit_df = self.spark.createDataFrame(audit_data, audit_schema)
        
        return audit_df


def mask_silver_to_gold_table(spark: SparkSession, config: Dict[str, Any], 
                             source_table: str, target_table: str) -> None:
    """Apply masking to transform Silver table to safe Gold table."""
    
    logger.info(f"Starting masking process: {source_table} -> {target_table}")
    
    try:
        # Read Silver table
        silver_df = read_delta(spark, f"{config['lake']['silver']}/{source_table}")
        
        # Initialize masking engine
        masking_engine = DataMaskingEngine(spark, config)
        
        # Apply masking
        gold_df = masking_engine.apply_masking_strategies(silver_df, target_table)
        
        # Write to Gold layer
        gold_path = f"{config['lake']['gold']}/{target_table}"
        write_delta(gold_df, gold_path, mode="overwrite")
        
        # Create audit record
        audit_df = masking_engine.create_masking_audit_table([])
        audit_path = f"{config['lake']['gold']}/audit/masking_audit"
        write_delta(audit_df, audit_path, mode="append")
        
        logger.info(f"Successfully created masked Gold table: {target_table}")
        
        # Log metrics
        row_count = gold_df.count()
        logger.info(f"Masked table {target_table} contains {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to mask table {source_table}: {str(e)}")
        raise


def main():
    """Main entry point for data masking job."""
    
    # Initialize Spark
    spark = build_spark("DataMaskingJob")
    
    # Load configuration
    config = load_config("aws/config/config-prod.yaml")
    
    # Tables to mask (Silver -> Gold)
    tables_to_mask = [
        ("customers", "customers_safe"),
        ("orders", "orders_safe"),
        ("payments", "payments_safe"),
        ("analytics_events", "analytics_events_safe"),
        ("support_tickets", "support_tickets_safe")
    ]
    
    try:
        for source_table, target_table in tables_to_mask:
            mask_silver_to_gold_table(spark, config, source_table, target_table)
        
        logger.info("Data masking job completed successfully")
        
    except Exception as e:
        logger.error(f"Data masking job failed: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
