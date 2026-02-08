"""
Tests for PII utilities.
"""

from project_a.utils.pii_utils import (
    apply_pii_masking,
    hash_value,
    mask_email,
    mask_name,
    mask_phone,
)


def test_mask_email():
    """Test email masking."""
    result = mask_email("john.doe@example.com")

    assert "@" in result
    assert "example.com" in result
    assert "john" not in result.lower() or "***" in result


def test_mask_phone():
    """Test phone masking."""
    result = mask_phone("123-456-7890")

    assert len(result) > 0
    # Should mask most digits
    assert result.count("*") > 0 or len(result) < len("123-456-7890")


def test_hash_value():
    """Test value hashing."""
    result = hash_value("test_value")

    assert result is not None
    assert len(result) > 0
    # Should be deterministic
    assert hash_value("test_value") == result


def test_mask_name():
    """Test name masking."""
    result = mask_name("John Doe")

    assert result is not None
    assert len(result) > 0


def test_apply_pii_masking(spark):
    """Test applying PII masking to DataFrame."""
    from pyspark.sql.types import StringType, StructField, StructType

    data = [
        ("john@example.com", "123-456-7890", "John Doe"),
    ]
    schema = StructType(
        [
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Define PII columns
    pii_config = {"email": "mask", "phone": "mask", "name": "hash"}

    result_df = apply_pii_masking(df, pii_config)

    assert result_df is not None
    assert "email" in result_df.columns
