"""
PII masking utilities for GDPR/CCPA compliance.

Provides UDFs for masking sensitive data in Silver/Gold layers.
"""
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

logger = logging.getLogger(__name__)


def _mask_email(value: str) -> str:
    """Mask email addresses: keep first char, mask rest before @."""
    if not value or "@" not in value:
        return value
    name, domain = value.split("@", 1)
    if len(name) <= 1:
        return f"*@{domain}"
    name_masked = name[0] + "***"
    return f"{name_masked}@{domain}"


def _mask_phone(value: str) -> str:
    """Mask phone numbers: show only last 4 digits."""
    if not value or len(value) < 4:
        return value
    return "***-***-" + value[-4:]


def _mask_ssn(value: str) -> str:
    """Mask SSN: show only last 4 digits."""
    if not value or len(value) < 4:
        return value
    return "***-**-" + value[-4:]


def _mask_name(value: str) -> str:
    """Mask names: show only first letter."""
    if not value or len(value) == 0:
        return value
    if len(value) == 1:
        return value
    return value[0] + "****"


def _mask_ip(value: str) -> str:
    """Mask IP addresses."""
    if not value:
        return value
    parts = value.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.***.***"
    return "***.***.***.***"


# Create UDFs
mask_email = udf(_mask_email, StringType())
mask_phone = udf(_mask_phone, StringType())
mask_ssn = udf(_mask_ssn, StringType())
mask_name = udf(_mask_name, StringType())
mask_ip = udf(_mask_ip, StringType())


def apply_pii_masking(df, email_col=None, phone_col=None, ssn_col=None, 
                      name_col=None, ip_col=None):
    """
    Apply PII masking to specified columns in DataFrame.
    
    Args:
        df: Input DataFrame
        email_col: Column name for email masking
        phone_col: Column name for phone masking
        ssn_col: Column name for SSN masking
        name_col: Column name for name masking
        ip_col: Column name for IP masking
        
    Returns:
        DataFrame with PII masked
        
    Example:
        masked_df = apply_pii_masking(
            df, 
            email_col="email",
            phone_col="phone_number"
        )
    """
    from pyspark.sql import functions as F
    
    df_masked = df
    
    if email_col and email_col in df.columns:
        logger.info(f"Masking PII: {email_col}")
        df_masked = df_masked.withColumn(email_col, mask_email(F.col(email_col)))
    
    if phone_col and phone_col in df.columns:
        logger.info(f"Masking PII: {phone_col}")
        df_masked = df_masked.withColumn(phone_col, mask_phone(F.col(phone_col)))
    
    if ssn_col and ssn_col in df.columns:
        logger.info(f"Masking PII: {ssn_col}")
        df_masked = df_masked.withColumn(ssn_col, mask_ssn(F.col(ssn_col)))
    
    if name_col and name_col in df.columns:
        logger.info(f"Masking PII: {name_col}")
        df_masked = df_masked.withColumn(name_col, mask_name(F.col(name_col)))
    
    if ip_col and ip_col in df.columns:
        logger.info(f"Masking PII: {ip_col}")
        df_masked = df_masked.withColumn(ip_col, mask_ip(F.col(ip_col)))
    
    return df_masked

