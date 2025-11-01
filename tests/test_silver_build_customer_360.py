"""
Tests for silver_build_customer_360 job.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType

from jobs.silver_build_customer_360 import build_customer_360


@pytest.fixture
def mock_config():
    """Mock configuration dictionary."""
    return {
        "data_lake": {
            "silver_path": "data/lakehouse_delta/silver",
            "gold_path": "data/lakehouse_delta/gold"
        },
        "lineage": {"enabled": False},
        "monitoring": {"metrics_enabled": False}
    }


@pytest.fixture
def sample_contacts_data(spark):
    """Sample contacts data."""
    data = [
        ("C1", "ACC1", "John", "Doe", "john@example.com", "123-456-7890", "2025-01-01", "2025-01-15"),
        ("C2", "ACC2", "Jane", "Smith", "jane@example.com", "123-456-7891", "2025-01-02", "2025-01-16"),
    ]
    schema = StructType([
        StructField("contact_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("last_modified_date", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_accounts_data(spark):
    """Sample accounts data."""
    data = [
        ("ACC1", "Acme Corp", "Enterprise", "Technology", "2025-01-01", "2025-01-15"),
        ("ACC2", "Beta Inc", "SMB", "Retail", "2025-01-02", "2025-01-16"),
    ]
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("last_modified_date", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_orders_data(spark):
    """Sample orders data."""
    data = [
        ("O1", "C1", "P1", "2025-01-01", 100.0, 2, "USD", "COMPLETED"),
        ("O2", "C2", "P2", "2025-01-02", 200.0, 1, "USD", "COMPLETED"),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("total_amount", DecimalType(18, 2), True),
        StructField("quantity", IntegerType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_behavior_data(spark):
    """Sample behavior data."""
    data = [
        ("E1", "C1", "page_view", "2025-01-01 10:00:00"),
        ("E2", "C1", "click", "2025-01-01 10:05:00"),
        ("E3", "C2", "page_view", "2025-01-02 11:00:00"),
    ]
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("event_ts", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@patch('jobs.silver_build_customer_360.emit_metrics')
@patch('jobs.silver_build_customer_360.GreatExpectationsRunner')
def test_build_customer_360_success(
    mock_ge_runner, mock_emit_metrics,
    spark, mock_config, sample_contacts_data, sample_accounts_data,
    sample_orders_data, sample_behavior_data, tmp_path
):
    """Test successful customer_360 Gold table build."""
    # Setup GE mock
    mock_ge_instance = Mock()
    mock_ge_instance.init_context.return_value = None
    mock_ge_instance.run_checkpoint.return_value = {"success": True}
    mock_ge_runner.return_value = mock_ge_instance
    
    # Write sample data to silver paths
    silver_path = tmp_path / "silver"
    (silver_path / "crm" / "contacts").mkdir(parents=True)
    (silver_path / "crm" / "accounts").mkdir(parents=True)
    (silver_path / "snowflake" / "orders").mkdir(parents=True)
    (silver_path / "behavior").mkdir(parents=True)
    
    sample_contacts_data.write.format("delta").mode("overwrite").save(str(silver_path / "crm" / "contacts"))
    sample_accounts_data.write.format("delta").mode("overwrite").save(str(silver_path / "crm" / "accounts"))
    sample_orders_data.write.format("delta").mode("overwrite").save(str(silver_path / "snowflake" / "orders"))
    sample_behavior_data.write.format("delta").mode("overwrite").save(str(silver_path / "behavior"))
    
    mock_config["data_lake"]["silver_path"] = str(silver_path)
    mock_config["data_lake"]["gold_path"] = str(tmp_path / "gold")
    
    # Execute build
    result_df = build_customer_360(spark, mock_config)
    
    # Assertions
    assert result_df is not None
    assert "customer_id" in result_df.columns
    assert "lifetime_value_usd" in result_df.columns or "total_orders" in result_df.columns


@patch('jobs.silver_build_customer_360.emit_metrics')
def test_build_customer_360_handles_missing_data(
    mock_emit_metrics, spark, mock_config, tmp_path
):
    """Test build handles missing Silver data gracefully."""
    mock_config["data_lake"]["silver_path"] = str(tmp_path / "nonexistent" / "silver")
    mock_config["data_lake"]["gold_path"] = str(tmp_path / "gold")
    
    # Should handle missing data gracefully
    result_df = build_customer_360(spark, mock_config)
    
    # Should still return a DataFrame (even if empty)
    assert result_df is not None


