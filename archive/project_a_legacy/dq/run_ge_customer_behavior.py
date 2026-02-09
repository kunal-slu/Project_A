"""
Great Expectations DQ runner for customer behavior data.

Runs GE checks on customer behavior table with fail-fast on critical violations.
"""

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

try:
    import great_expectations as gx

    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False

logger = logging.getLogger(__name__)


def get_spark():
    """Get or create Spark session."""
    return SparkSession.builder.appName("dq-customer-behavior").getOrCreate()


def main(config: dict[str, Any] | None = None):
    """Run GE DQ checks on customer behavior bronze table."""
    if not GE_AVAILABLE:
        logger.warning("Great Expectations not installed, skipping DQ checks")
        return

    spark = get_spark()

    # 1. Read bronze table (Redshift → S3 → bronze landed data)
    # Use config-based bucket resolution
    bucket = (
        (config or {}).get("buckets", {}).get("lake", "my-etl-lake-demo")
        if config
        else os.getenv("S3_BUCKET", "my-etl-lake-demo")
    )
    df = spark.read.format("parquet").load(f"s3a://{bucket}/bronze/customer_behavior/")

    logger.info(f"Loaded {df.count()} records from bronze/customer_behavior")

    # 2. Convert to pandas for GE (or use SparkDFDataset)
    try:
        pdf = df.toPandas()
    except Exception as e:
        logger.error(f"Failed to convert to pandas: {e}")
        raise

    # Initialize GE context
    context = gx.get_context()

    # 3. Ad-hoc suite (can be loaded from config/dq.yaml)
    context.add_or_update_expectation_suite("customer_behavior_suite")

    # Create validator
    batch = context.sources.pandas_default.read_dataframe(pdf)
    validator = batch.validate(expectation_suite_name="customer_behavior_suite")

    # Core DQ checks
    logger.info("Running DQ expectations...")

    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_in_set(
        "event_name",
        [
            "page_view",
            "product_view",
            "signup",
            "email_open",
            "email_click",
            "purchase",
            "add_to_cart",
            "login",
        ],
    )
    validator.expect_column_values_to_match_regex("session_id", r"^SESS-\d{6}$")

    # Run validation
    result = validator.validate()

    # 4. Fail fast on critical violations
    if not result["success"]:
        error_msg = f"DQ failed for customer_behavior: {result.get('statistics', {})}"
        logger.error(error_msg)

        # In prod: send to SNS/Slack/CloudWatch
        # send_alert("DQ_FAILURE", error_msg)

        raise RuntimeError(error_msg)

    logger.info("✅ DQ passed for customer_behavior")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
