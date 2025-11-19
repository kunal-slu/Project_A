"""
Unit tests for SafeDeltaWriter module.
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from delta.tables import DeltaTable
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from project_a.utils.safe_writer import SafeDeltaWriter


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder
        .appName("TestSafeWriter")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .master("local[2]")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("balance", DoubleType(), True),
        StructField("segment", StringType(), True),
        StructField("ingest_date", DateType(), True),
    ])


@pytest.fixture
def sample_data(spark, sample_schema):
    """Create sample data for testing."""
    data = [
        ("C001", "Alice", 100.0, "Premium", date(2024, 1, 1)),
        ("C002", "Bob", 200.0, "Standard", date(2024, 1, 1)),
        ("C003", "Charlie", 300.0, "Premium", date(2024, 1, 1)),
    ]
    return spark.createDataFrame(data, schema=sample_schema)


class TestSafeDeltaWriter:
    """Test suite for SafeDeltaWriter."""

    def test_writer_initialization(self, spark):
        """Test SafeDeltaWriter initialization."""
        writer = SafeDeltaWriter(spark)
        assert writer.spark == spark

    def test_write_new_table_append(self, spark, temp_dir, sample_data):
        """Test creating a new table with append mode."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        result = writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append"
        )
        
        assert result["success"] is True
        assert result["final_row_count"] == 3
        assert result["initial_row_count"] == 0
        assert result["records_written"] == 3
        
        # Verify table exists
        delta_table = DeltaTable.forPath(spark, target_path)
        assert delta_table.toDF().count() == 3

    def test_write_merge_upsert(self, spark, temp_dir, sample_data, sample_schema):
        """Test MERGE upsert operation."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        # Initial write
        writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append"
        )
        
        # Update data
        updated_data = [
            ("C001", "Alice Updated", 150.0, "Premium", date(2024, 1, 2)),
            ("C004", "David", 400.0, "Standard", date(2024, 1, 2)),
        ]
        df_update = spark.createDataFrame(updated_data, schema=sample_schema)
        
        # Merge
        result = writer.write_with_merge(
            df=df_update,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="merge"
        )
        
        assert result["success"] is True
        assert result["final_row_count"] == 4  # 3 original + 1 new
        
        # Verify merge results
        delta_table = DeltaTable.forPath(spark, target_path)
        result_df = delta_table.toDF()
        
        # Check updated record
        alice = result_df.filter("customer_id = 'C001'").collect()[0]
        assert alice["name"] == "Alice Updated"
        assert alice["balance"] == 150.0
        
        # Check new record
        david = result_df.filter("customer_id = 'C004'").collect()
        assert len(david) == 1

    def test_write_with_replace_where(self, spark, temp_dir, sample_data, sample_schema):
        """Test overwrite with replaceWhere condition."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        # Initial write
        writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append"
        )
        
        # New data for same partition
        new_data = [
            ("C005", "Eve", 500.0, "Premium", date(2024, 1, 1)),
        ]
        df_new = spark.createDataFrame(new_data, schema=sample_schema)
        
        # Overwrite partition
        result = writer.write_with_merge(
            df=df_new,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="overwrite",
            replace_where_condition="ingest_date = '2024-01-01'"
        )
        
        assert result["success"] is True
        
        # Verify partition was replaced
        delta_table = DeltaTable.forPath(spark, target_path)
        result_df = delta_table.toDF()
        
        # Should only have the new record for this partition
        partition_data = result_df.filter("ingest_date = '2024-01-01'").collect()
        assert len(partition_data) == 1
        assert partition_data[0]["customer_id"] == "C005"

    def test_write_without_merge_keys_fails(self, spark, temp_dir, sample_data):
        """Test that merge mode without merge_keys raises ValueError."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        # Initial write
        writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append"
        )
        
        # Try merge without keys
        with pytest.raises(ValueError, match="Merge keys must be provided"):
            writer.write_with_merge(
                df=sample_data,
                target_path=target_path,
                merge_keys=[],
                mode="merge"
            )

    def test_write_overwrite_without_replace_where_fails(self, spark, temp_dir, sample_data):
        """Test that overwrite without replaceWhere on existing table fails."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        # Initial write
        writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append"
        )
        
        # Try unsafe overwrite
        with pytest.raises(ValueError, match="Unsupported write mode"):
            writer.write_with_merge(
                df=sample_data,
                target_path=target_path,
                merge_keys=["customer_id"],
                mode="overwrite",
                replace_where_condition=None
            )

    def test_write_with_pre_hook(self, spark, temp_dir, sample_data):
        """Test write with pre-write hook."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        def pre_hook(df: DataFrame) -> DataFrame:
            """Add a timestamp column."""
            from pyspark.sql.functions import current_timestamp
            return df.withColumn("processed_at", current_timestamp())
        
        result = writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append",
            pre_write_hook=pre_hook
        )
        
        assert result["success"] is True
        
        # Verify hook was applied
        delta_table = DeltaTable.forPath(spark, target_path)
        result_df = delta_table.toDF()
        assert "processed_at" in result_df.columns

    def test_write_with_post_hook(self, spark, temp_dir, sample_data):
        """Test write with post-write hook."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        hook_called = {"count": 0}
        
        def post_hook(df: DataFrame, row_count: int):
            """Verify row count."""
            hook_called["count"] = row_count
        
        result = writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append",
            post_write_hook=post_hook
        )
        
        assert result["success"] is True
        assert hook_called["count"] == 3

    def test_row_count_validation(self, spark, temp_dir, sample_data):
        """Test row count validation."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        result = writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append"
        )
        
        # Verify counts
        assert result["initial_row_count"] == 0
        assert result["final_row_count"] == 3
        assert result["records_written"] == 3

    def test_multiple_merge_operations(self, spark, temp_dir, sample_data, sample_schema):
        """Test multiple sequential merge operations."""
        writer = SafeDeltaWriter(spark)
        target_path = str(Path(temp_dir) / "customers")
        
        # Initial write
        writer.write_with_merge(
            df=sample_data,
            target_path=target_path,
            merge_keys=["customer_id"],
            mode="append"
        )
        
        # Multiple updates
        for i in range(3):
            updated_data = [
                ("C001", f"Alice v{i+1}", 100.0 + (i * 10), "Premium", date(2024, 1, i+2)),
            ]
            df_update = spark.createDataFrame(updated_data, schema=sample_schema)
            
            result = writer.write_with_merge(
                df=df_update,
                target_path=target_path,
                merge_keys=["customer_id"],
                mode="merge"
            )
            
            assert result["success"] is True
        
        # Verify final state
        delta_table = DeltaTable.forPath(spark, target_path)
        alice = delta_table.toDF().filter("customer_id = 'C001'").collect()[0]
        assert alice["name"] == "Alice v3"
        assert alice["balance"] == 120.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

