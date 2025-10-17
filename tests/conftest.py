"""
Pytest configuration and shared fixtures.
"""

import pytest
import os
import sys
from pyspark.sql import SparkSession

# Add src to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("pytest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.adaptive.skewJoin.enabled", "false") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_customers_data():
    """Sample customers data for testing."""
    return [
        ("1", "John", "Doe", "john@example.com", "123 Main St", "New York", "NY", "USA", "10001", "555-1234", "2025-01-01", "M", 30),
        ("2", "Jane", "Smith", "jane@example.com", "456 Oak Ave", "Los Angeles", "CA", "USA", "90210", "555-5678", "2025-01-02", "F", 25),
        ("3", "Bob", "Johnson", "bob@example.com", "789 Pine Rd", "Chicago", "IL", "USA", "60601", "555-9012", "2025-01-03", "M", 35),
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