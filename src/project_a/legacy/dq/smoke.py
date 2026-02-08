"""
Lightweight Data Quality smoke checks.
Fail fast if basic data quality issues are detected.
"""

import logging

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def assert_non_null(df: DataFrame, col: str, threshold: float = 0.1) -> None:
    """Assert that a column has minimal null values."""
    total_count = df.count()
    null_count = df.filter(f"{col} IS NULL").count()
    null_percentage = null_count / total_count if total_count > 0 else 0

    if null_percentage > threshold:
        raise ValueError(
            f"Column '{col}' has {null_percentage:.2%} nulls (threshold: {threshold:.2%})"
        )

    logger.info(f"✓ Column '{col}': {null_percentage:.2%} nulls (within threshold)")


def assert_no_duplicates(df: DataFrame, cols: list[str], threshold: float = 0.05) -> None:
    """Assert that duplicate records are within acceptable limits."""
    total_count = df.count()
    distinct_count = df.select(*cols).distinct().count()
    duplicate_percentage = (total_count - distinct_count) / total_count if total_count > 0 else 0

    if duplicate_percentage > threshold:
        raise ValueError(
            f"Duplicate records: {duplicate_percentage:.2%} (threshold: {threshold:.2%})"
        )

    logger.info(f"✓ Duplicates: {duplicate_percentage:.2%} (within threshold)")


def assert_positive_values(df: DataFrame, col: str) -> None:
    """Assert that a numeric column has only positive values."""
    negative_count = df.selectExpr(f"sum(case when {col} < 0 then 1 else 0 end) as n").collect()[0][
        "n"
    ]

    if negative_count > 0:
        raise ValueError(f"Column '{col}' has {negative_count} negative values")

    logger.info(f"✓ Column '{col}': no negative values")


def assert_valid_dates(df: DataFrame, col: str) -> None:
    """Assert that a date column has valid dates."""
    invalid_count = df.selectExpr(
        f"sum(case when {col} is null or {col} < '1900-01-01' then 1 else 0 end) as n"
    ).collect()[0]["n"]

    if invalid_count > 0:
        raise ValueError(f"Column '{col}' has {invalid_count} invalid dates")

    logger.info(f"✓ Column '{col}': valid dates")


def dq_customers(df: DataFrame) -> None:
    """Data quality checks for customers table."""
    logger.info("Running data quality checks for customers...")

    # Check required columns
    assert_non_null(df, "customer_id", threshold=0.0)
    assert_non_null(df, "name", threshold=0.05)
    assert_non_null(df, "email", threshold=0.05)

    # Check for duplicates
    assert_no_duplicates(df, ["customer_id"], threshold=0.0)

    # Check email format (basic validation)
    invalid_emails = df.filter("email NOT LIKE '%@%'").count()
    if invalid_emails > 0:
        logger.warning(f"Found {invalid_emails} emails without '@' symbol")

    logger.info("✓ Customers data quality checks passed")


def dq_orders(df: DataFrame) -> None:
    """Data quality checks for orders table."""
    logger.info("Running data quality checks for orders...")

    # Check required columns
    assert_non_null(df, "order_id", threshold=0.0)
    assert_non_null(df, "customer_id", threshold=0.0)
    assert_non_null(df, "order_date", threshold=0.05)

    # Check for duplicates
    assert_no_duplicates(df, ["order_id"], threshold=0.0)

    # Check numeric columns
    if "total_amount" in df.columns:
        assert_positive_values(df, "total_amount")
    if "quantity" in df.columns:
        assert_positive_values(df, "quantity")

    # Check date validity
    assert_valid_dates(df, "order_date")

    logger.info("✓ Orders data quality checks passed")


def dq_products(df: DataFrame) -> None:
    """Data quality checks for products table."""
    logger.info("Running data quality checks for products...")

    # Check required columns
    assert_non_null(df, "product_id", threshold=0.0)
    assert_non_null(df, "product_name", threshold=0.05)

    # Check for duplicates
    assert_no_duplicates(df, ["product_id"], threshold=0.0)

    # Check price if exists
    if "price" in df.columns:
        assert_positive_values(df, "price")

    logger.info("✓ Products data quality checks passed")


def run_dq_checks(df: DataFrame, table_name: str) -> None:
    """Run appropriate DQ checks based on table name."""
    if table_name.lower().startswith("customer"):
        dq_customers(df)
    elif table_name.lower().startswith("order"):
        dq_orders(df)
    elif table_name.lower().startswith("product"):
        dq_products(df)
    else:
        logger.warning(f"No specific DQ checks defined for table: {table_name}")
        # Run basic checks
        assert_non_null(df, df.columns[0], threshold=0.0)  # First column should have no nulls
