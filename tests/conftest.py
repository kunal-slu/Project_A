"""
Pytest configuration and shared fixtures.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Ensure Delta Lake + Avro packages are available for tests that use them.
try:
    import importlib_metadata
    import pyspark

    # Force tests to use the PySpark-bundled Spark distribution (avoids Spark 4.x/Scala mismatch)
    pyspark_home = os.path.dirname(pyspark.__file__)
    os.environ["SPARK_HOME"] = pyspark_home

    delta_version = importlib_metadata.version("delta_spark")
    spark_version = pyspark.__version__
    iceberg_pkg = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
    packages = (
        f"io.delta:delta-spark_2.12:{delta_version},"
        f"org.apache.spark:spark-avro_2.12:{spark_version},"
        f"{iceberg_pkg}"
    )

    if "PYSPARK_SUBMIT_ARGS" not in os.environ:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {packages} "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "pyspark-shell"
        )
except Exception:
    # If delta-spark isn't installed, tests will skip or fall back where possible.
    pass

# Ensure module-scoped Spark fixtures do not stop the shared SparkContext
os.environ.setdefault("PROJECT_A_DISABLE_SPARK_STOP", "1")

# Add src to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    try:
        spark = (
            SparkSession.builder.appName("pytest")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.sql.adaptive.skewJoin.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
    except Exception:
        pytest.skip("Spark unavailable in current environment")

    yield spark
    spark.stop()


@pytest.fixture
def sample_customers_data():
    """Sample customers data for testing."""
    return [
        (
            "1",
            "John",
            "Doe",
            "john@example.com",
            "123 Main St",
            "New York",
            "NY",
            "USA",
            "10001",
            "555-1234",
            "2025-01-01",
            "M",
            30,
        ),
        (
            "2",
            "Jane",
            "Smith",
            "jane@example.com",
            "456 Oak Ave",
            "Los Angeles",
            "CA",
            "USA",
            "90210",
            "555-5678",
            "2025-01-02",
            "F",
            25,
        ),
        (
            "3",
            "Bob",
            "Johnson",
            "bob@example.com",
            "789 Pine Rd",
            "Chicago",
            "IL",
            "USA",
            "60601",
            "555-9012",
            "2025-01-03",
            "M",
            35,
        ),
    ]


@pytest.fixture
def sample_orders_data():
    """Sample orders data for testing."""
    return [
        ("O1", "1", "P1", 2, 25.0, 50.0, "2025-01-01T10:00:00", "Card", "PAID"),
        ("O2", "2", "P2", 1, 100.0, 100.0, "2025-01-02T11:00:00", "Cash", "PAID"),
        ("O3", "3", "P3", 3, 15.0, 45.0, "2025-01-03T12:00:00", "Card", "PENDING"),
    ]


@pytest.fixture
def temp_dir(tmp_path):
    """Temporary directory for test files."""
    return str(tmp_path)


@pytest.fixture(autouse=True)
def setup_logging():
    """Setup logging for tests."""
    import logging

    logging.basicConfig(level=logging.INFO)
