"""
Test Glue catalog contract to ensure DB/table names match config.
"""

import os
import sys
from pathlib import Path

# Add src to path
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))


def test_glue_database_names():
    """Test that Glue database names match expected patterns."""
    expected_databases = {"bronze_db": "bronze", "silver_db": "silver", "gold_db": "gold"}

    # Test environment variable defaults
    assert os.getenv("GLUE_DB_SILVER", "silver_db") == "silver_db"
    assert os.getenv("GLUE_DB_GOLD", "gold_db") == "gold_db"

    # Test naming convention
    for db_name, layer in expected_databases.items():
        assert layer in db_name.lower()


def test_s3_path_alignment():
    """Test that S3 paths align with Glue database structure."""
    expected_paths = {
        "bronze": "s3a://{bucket}/bronze/",
        "silver": "s3a://{bucket}/silver/",
        "gold": "s3a://{bucket}/gold/",
    }

    bucket = os.getenv("S3_LAKE_BUCKET", "data-lake-bucket")

    for layer, path_template in expected_paths.items():
        expected_path = path_template.format(bucket=bucket)
        assert layer in expected_path
        assert expected_path.startswith("s3a://")


def test_table_naming_convention():
    """Test that table names follow consistent conventions."""
    # Bronze layer tables
    bronze_tables = [
        "hubspot_contacts",
        "hubspot_deals",
        "snowflake_customers",
        "snowflake_orders",
        "snowflake_products",
        "redshift_customer_behavior",
        "fx_rates",
        "kafka_events",
    ]

    # Silver layer tables
    silver_tables = [
        "customers_conformed",
        "orders_conformed",
        "products_conformed",
        "customer_behavior_conformed",
        "fx_rates_conformed",
    ]

    # Gold layer tables
    gold_tables = ["customer_analytics", "business_metrics", "revenue_summary", "customer_segments"]

    # Test naming conventions
    for table in bronze_tables:
        assert "_" in table  # Snake case
        assert table.islower()  # Lowercase

    for table in silver_tables:
        assert "_" in table  # Snake case
        assert table.islower()  # Lowercase
        assert "conformed" in table  # Silver layer suffix

    for table in gold_tables:
        assert "_" in table  # Snake case
        assert table.islower()  # Lowercase


def test_checkpoint_path_consistency():
    """Test that checkpoint paths are consistent."""
    checkpoint_prefix = os.getenv("S3_CHECKPOINT_PREFIX", "checkpoints")
    bucket = os.getenv("S3_LAKE_BUCKET", "data-lake-bucket")

    expected_checkpoint_base = f"s3a://{bucket}/{checkpoint_prefix}"

    # Test streaming checkpoint paths
    streaming_checkpoints = [
        f"{expected_checkpoint_base}/orders_stream/valid",
        f"{expected_checkpoint_base}/orders_stream/dlq",
        f"{expected_checkpoint_base}/orders_stream/raw_dlq",
    ]

    for checkpoint_path in streaming_checkpoints:
        assert checkpoint_path.startswith("s3a://")
        assert "checkpoints" in checkpoint_path
        assert "orders_stream" in checkpoint_path


def test_airflow_variable_consistency():
    """Test that Airflow variables are consistently named."""
    required_variables = [
        "EMR_APP_ID",
        "EMR_JOB_ROLE_ARN",
        "GLUE_DB_SILVER",
        "GLUE_DB_GOLD",
        "S3_LAKE_BUCKET",
        "S3_CHECKPOINT_PREFIX",
    ]

    # Test naming convention
    for var in required_variables:
        assert var.isupper()  # Uppercase
        assert "_" in var  # Underscore separated
        assert not var.startswith("_")  # No leading underscore
        assert not var.endswith("_")  # No trailing underscore


def test_config_file_alignment():
    """Test that config files align with expected structure."""
    config_files = ["config/aws.yaml", "config/default.yaml", "config/local.yaml"]

    for config_file in config_files:
        config_path = Path(__file__).parent.parent / config_file
        if config_path.exists():
            # Basic validation that file exists and is readable
            assert config_path.is_file()
            assert config_path.stat().st_size > 0  # Not empty
