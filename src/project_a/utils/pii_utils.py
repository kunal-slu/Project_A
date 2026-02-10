"""PII masking helpers with string and DataFrame compatibility APIs."""

from __future__ import annotations

import hashlib
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def mask_email(value: str | None) -> str:
    if not value or "@" not in value:
        return ""
    local, domain = value.split("@", 1)
    keep = local[:1]
    return f"{keep}***@{domain}"


def mask_phone(value: str | None) -> str:
    if not value:
        return ""
    digits = re.sub(r"\D", "", value)
    if len(digits) <= 4:
        return "*" * len(digits)
    return f"{'*' * (len(digits) - 4)}{digits[-4:]}"


def hash_value(value: str | None) -> str:
    if value is None:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def mask_name(value: str | None) -> str:
    if not value:
        return ""
    parts = value.split()
    masked = [f"{part[:1]}***" if part else "" for part in parts]
    return " ".join(masked)


def apply_pii_masking(df: DataFrame, pii_config: dict[str, str]) -> DataFrame:
    """Apply masking rules by column name. Supported rules: mask, hash."""
    result = df

    mask_email_udf = udf(mask_email, StringType())
    mask_phone_udf = udf(mask_phone, StringType())
    mask_name_udf = udf(mask_name, StringType())
    hash_udf = udf(hash_value, StringType())

    for col_name, mode in pii_config.items():
        if col_name not in result.columns:
            continue
        if mode == "hash":
            result = result.withColumn(col_name, hash_udf(col_name))
            continue

        # "mask" mode picks sensible default based on column name.
        if "email" in col_name.lower():
            result = result.withColumn(col_name, mask_email_udf(col_name))
        elif "phone" in col_name.lower():
            result = result.withColumn(col_name, mask_phone_udf(col_name))
        else:
            result = result.withColumn(col_name, mask_name_udf(col_name))

    return result
