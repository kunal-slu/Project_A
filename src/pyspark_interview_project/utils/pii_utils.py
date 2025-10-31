"""
PII (Personally Identifiable Information) masking utilities.

Provides functions to mask/hash sensitive data for compliance.
"""

import os
import hashlib
import re
import logging
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, sha2, lit, when, concat
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


def get_salt() -> str:
    """Get salt from environment or default."""
    return os.getenv("PII_MASKING_SALT", "default_salt_change_in_production")


def mask_email(df: DataFrame, email_col: str, output_col: Optional[str] = None) -> DataFrame:
    """
    Mask email addresses: user@domain.com -> u***@d***.com
    
    Args:
        df: Input DataFrame
        email_col: Column name containing email
        output_col: Output column name (defaults to email_col)
        
    Returns:
        DataFrame with masked email
    """
    output_col = output_col or email_col
    
    # Pattern: keep first char, mask rest before @, keep domain structure
    masked_expr = regexp_replace(
        col(email_col),
        r'^([a-zA-Z0-9._%+-])[^@]*@([a-zA-Z0-9.-]*)\.([a-zA-Z]{2,})$',
        r'\1***@\2***.\3'
    )
    
    return df.withColumn(output_col, masked_expr)


def mask_phone(df: DataFrame, phone_col: str, output_col: Optional[str] = None) -> DataFrame:
    """
    Mask phone numbers: +1234567890 -> +1***7890
    
    Args:
        df: Input DataFrame
        phone_col: Column name containing phone
        output_col: Output column name (defaults to phone_col)
        
    Returns:
        DataFrame with masked phone
    """
    output_col = output_col or phone_col
    
    # Pattern: keep country code and last 4 digits, mask middle
    masked_expr = regexp_replace(
        col(phone_col),
        r'^(\+?\d{1,2})?(\d{1,3})?(\d{4})$',
        r'\1***\3'
    )
    
    # If pattern doesn't match, just mask all but last 4
    masked_expr = when(
        col(phone_col).rlike(r'^\d+$'),
        regexp_replace(col(phone_col), r'^(.+?)(\d{4})$', r'***\2')
    ).otherwise(col(phone_col))
    
    return df.withColumn(output_col, masked_expr)


def hash_value(df: DataFrame, col_name: str, output_col: Optional[str] = None, salt_from_env: bool = True) -> DataFrame:
    """
    Hash a column value using SHA-256.
    
    Args:
        df: Input DataFrame
        col_name: Column name to hash
        output_col: Output column name (defaults to col_name)
        salt_from_env: Use salt from environment
        
    Returns:
        DataFrame with hashed column
    """
    output_col = output_col or col_name
    
    # Create salted value
    salt = get_salt() if salt_from_env else ""
    
    # Hash using SHA-256
    # Note: In real implementation, would concatenate salt with value
    hashed_expr = sha2(
        when(col(col_name).isNotNull(), concat(lit(salt), col(col_name))),
        256
    )
    
    return df.withColumn(output_col, hashed_expr)


def mask_name(df: DataFrame, name_col: str, output_col: Optional[str] = None) -> DataFrame:
    """
    Mask name: "John Doe" -> "J*** D***"
    
    Args:
        df: Input DataFrame
        name_col: Column name containing name
        output_col: Output column name (defaults to name_col)
        
    Returns:
        DataFrame with masked name
    """
    output_col = output_col or name_col
    
    # Mask first name: keep first char
    # Mask last name: keep first char
    # Split by space and mask each part
    masked_expr = regexp_replace(
        col(name_col),
        r'(\w)\w*',
        r'\1***'
    )
    
    return df.withColumn(output_col, masked_expr)


def apply_pii_masking(
    df: DataFrame,
    pii_config: dict,
    layer: str = "gold"
) -> DataFrame:
    """
    Apply PII masking based on configuration.
    
    Args:
        df: Input DataFrame
        pii_config: PII configuration dict (from config/dq.yaml)
        layer: Data layer (bronze/silver/gold) - only mask in gold
        
    Returns:
        DataFrame with PII masked
    """
    if layer != "gold":
        # Only mask in gold layer
        logger.info(f"Skipping PII masking in {layer} layer (only applied in gold)")
        return df
    
    logger.info("Applying PII masking to gold layer")
    
    result_df = df
    
    # Get PII columns from config
    pii_fields = pii_config.get("pii_fields", {})
    
    for col_name, pii_info in pii_fields.items():
        if col_name not in df.columns:
            logger.warning(f"PII column {col_name} not found in DataFrame")
            continue
        
        pii_type = pii_info.get("type", "hash")
        mask_mode = pii_info.get("mask_mode", "hash")
        
        if mask_mode == "hash":
            result_df = hash_value(result_df, col_name)
            logger.info(f"✅ Hashed PII column: {col_name}")
        elif mask_mode == "mask_email" and pii_type == "email":
            result_df = mask_email(result_df, col_name)
            logger.info(f"✅ Masked email column: {col_name}")
        elif mask_mode == "mask_phone" and pii_type == "phone":
            result_df = mask_phone(result_df, col_name)
            logger.info(f"✅ Masked phone column: {col_name}")
        elif mask_mode == "mask_name" and pii_type == "name":
            result_df = mask_name(result_df, col_name)
            logger.info(f"✅ Masked name column: {col_name}")
        elif mask_mode == "drop":
            result_df = result_df.drop(col_name)
            logger.info(f"✅ Dropped PII column: {col_name}")
        else:
            # Default: hash
            result_df = hash_value(result_df, col_name)
            logger.info(f"✅ Hashed PII column (default): {col_name}")
    
    return result_df

