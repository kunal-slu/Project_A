"""
Comprehensive tests for SCD2 implementation.

This test suite validates the standardized SCD2 helper with golden datasets
that include updates and late-arriving data scenarios.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from project_a.common.scd2 import apply_scd2, SCD2Config, validate_scd2_table


class TestSCD2Standardized:
    """Test the standardized SCD2 implementation."""

    @pytest.fixture
    def spark(self):
        """Create Spark session for testing."""
        return (SparkSession.builder
                .appName("SCD2Test")
                .master("local[2]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())

    @pytest.fixture
    def scd2_config(self):
        """Create standard SCD2 configuration."""
        return SCD2Config(
            business_key="customer_id",
            change_columns=["name", "email", "address"],
            effective_from_column="effective_from",
            effective_to_column="effective_to",
            is_current_column="is_current",
            surrogate_key_column="surrogate_key",
            hash_column="hash_diff",
            updated_at_column="updated_at"
        )

    @pytest.fixture
    def sample_customer_data(self, spark):
        """Create sample customer data for testing."""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])

        # Initial data
        data = [
            ("C001", "John Doe", "john@example.com", "123 Main St", datetime(2024, 1, 1, 10, 0)),
            ("C002", "Jane Smith", "jane@example.com", "456 Oak Ave", datetime(2024, 1, 1, 11, 0)),
            ("C003", "Bob Johnson", "bob@example.com", "789 Pine St", datetime(2024, 1, 1, 12, 0))
        ]

        return spark.createDataFrame(data, schema)

    @pytest.fixture
    def updated_customer_data(self, spark):
        """Create updated customer data for testing changes."""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])

        # Updated data (changes to C001 and C002)
        data = [
            ("C001", "John Doe", "john.doe@newemail.com", "123 Main St", datetime(2024, 1, 15, 10, 0)),  # Email changed
            ("C002", "Jane Smith", "jane@example.com", "456 Oak Ave Apt 2B", datetime(2024, 1, 15, 11, 0)),  # Address changed
            ("C004", "Alice Brown", "alice@example.com", "321 Elm St", datetime(2024, 1, 15, 13, 0))  # New customer
        ]

        return spark.createDataFrame(data, schema)

    @pytest.fixture
    def late_arriving_data(self, spark):
        """Create late-arriving data for testing."""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])

        # Late-arriving data (should be inserted with correct effective_from)
        data = [
            ("C001", "John Doe", "john@example.com", "123 Main St", datetime(2024, 1, 5, 10, 0)),  # Late arrival
            ("C005", "Charlie Davis", "charlie@example.com", "654 Maple Dr", datetime(2024, 1, 5, 14, 0))  # New late arrival
        ]

        return spark.createDataFrame(data, schema)

    def test_scd2_initial_load(self, spark, scd2_config, sample_customer_data, tmp_path):
        """Test initial SCD2 load."""
        target_path = str(tmp_path / "customers_scd2")

        # Apply SCD2
        result = apply_scd2(spark, sample_customer_data, target_path, scd2_config)

        # Verify result
        assert result["success"] is True
        assert result["records_processed"] == 3

        # Load and verify table
        df = spark.read.format("delta").load(target_path)

        # Check required columns exist
        required_cols = [
            "customer_id", "name", "email", "address", "effective_from",
            "effective_to", "is_current", "surrogate_key", "hash_diff", "updated_at"
        ]
        for col in required_cols:
            assert col in df.columns

        # Check all records are current
        current_records = df.filter(F.col("is_current") == True)
        assert current_records.count() == 3

        # Check effective_to is null for current records
        null_effective_to = current_records.filter(F.col("effective_to").isNull())
        assert null_effective_to.count() == 3

    def test_scd2_incremental_update(self, spark, scd2_config, sample_customer_data, updated_customer_data, tmp_path):
        """Test incremental SCD2 update with changes."""
        target_path = str(tmp_path / "customers_scd2")

        # Initial load
        apply_scd2(spark, sample_customer_data, target_path, scd2_config)

        # Apply updates
        result = apply_scd2(spark, updated_customer_data, target_path, scd2_config)

        # Verify result
        assert result["success"] is True
        assert result["records_processed"] == 3  # 2 changes + 1 new

        # Load and verify table
        df = spark.read.format("delta").load(target_path)

        # Check total records (3 original + 3 new/updated)
        assert df.count() == 6

        # Check current records
        current_records = df.filter(F.col("is_current") == True)
        assert current_records.count() == 4  # 3 original + 1 new

        # Verify specific changes
        # C001 should have 2 records (old and new)
        c001_records = df.filter(F.col("customer_id") == "C001").orderBy("effective_from")
        assert c001_records.count() == 2

        # Check old record is closed
        old_c001 = c001_records.first()
        assert old_c001["is_current"] is False
        assert old_c001["effective_to"] is not None

        # Check new record is current
        new_c001 = c001_records.collect()[1]
        assert new_c001["is_current"] is True
        assert new_c001["effective_to"] is None
        assert new_c001["email"] == "john.doe@newemail.com"

    def test_scd2_late_arriving_data(self, spark, scd2_config, sample_customer_data, late_arriving_data, tmp_path):
        """Test SCD2 with late-arriving data."""
        target_path = str(tmp_path / "customers_scd2")

        # Initial load
        apply_scd2(spark, sample_customer_data, target_path, scd2_config)

        # Apply late-arriving data with explicit effective_from
        late_effective_from = datetime(2024, 1, 5, 10, 0)
        result = apply_scd2(spark, late_arriving_data, target_path, scd2_config, effective_from=late_effective_from)

        # Verify result
        assert result["success"] is True
        assert result["records_processed"] == 2

        # Load and verify table
        df = spark.read.format("delta").load(target_path)

        # Check total records
        assert df.count() == 5

        # Check C001 has correct effective_from for late arrival
        c001_late = df.filter(
            (F.col("customer_id") == "C001") &
            (F.col("effective_from") == F.lit(late_effective_from))
        )
        assert c001_late.count() == 1

        # Verify the late record is inserted with correct timestamp
        late_record = c001_late.first()
        assert late_record["effective_from"] == late_effective_from

    def test_scd2_validation(self, spark, scd2_config, sample_customer_data, tmp_path):
        """Test SCD2 table validation."""
        target_path = str(tmp_path / "customers_scd2")

        # Create SCD2 table
        apply_scd2(spark, sample_customer_data, target_path, scd2_config)

        # Validate table
        validation_result = validate_scd2_table(spark, target_path, scd2_config)

        # Verify validation passes
        assert validation_result["valid"] is True
        assert "validation passed" in validation_result["message"]

    def test_scd2_no_changes(self, spark, scd2_config, sample_customer_data, tmp_path):
        """Test SCD2 when no changes are detected."""
        target_path = str(tmp_path / "customers_scd2")

        # Initial load
        apply_scd2(spark, sample_customer_data, target_path, scd2_config)

        # Apply same data again (no changes)
        result = apply_scd2(spark, sample_customer_data, target_path, scd2_config)

        # Verify result
        assert result["success"] is True
        assert result["records_processed"] == 0
        assert "No SCD2 changes detected" in result["message"]

        # Verify table still has same number of records
        df = spark.read.format("delta").load(target_path)
        assert df.count() == 3  # No new records added

    def test_scd2_missing_columns(self, spark, scd2_config, tmp_path):
        """Test SCD2 with missing required columns."""
        target_path = str(tmp_path / "customers_scd2")

        # Create data with missing columns
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            # Missing email and address
        ])

        data = [("C001", "John Doe")]
        df = spark.createDataFrame(data, schema)

        # Should fail due to missing columns
        result = apply_scd2(spark, df, target_path, scd2_config)

        assert result["success"] is False
        assert "Missing required columns" in result["error"]

    def test_scd2_empty_dataframe(self, spark, scd2_config, tmp_path):
        """Test SCD2 with empty DataFrame."""
        target_path = str(tmp_path / "customers_scd2")

        # Create empty DataFrame
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
        ])

        df = spark.createDataFrame([], schema)

        # Should succeed but process no records
        result = apply_scd2(spark, df, target_path, scd2_config)

        assert result["success"] is True
        assert result["records_processed"] == 0
        assert "No source data to process" in result["message"]

    def test_scd2_with_ts_column(self, spark, tmp_path):
        """Test SCD2 with timestamp column for effective_from."""
        target_path = str(tmp_path / "customers_scd2")

        # Config with ts_column
        config = SCD2Config(
            business_key="customer_id",
            change_columns=["name", "email"],
            ts_column="created_at"
        )

        # Create data with timestamp column
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])

        data = [
            ("C001", "John Doe", "john@example.com", datetime(2024, 1, 1, 10, 0)),
            ("C002", "Jane Smith", "jane@example.com", datetime(2024, 1, 1, 11, 0))
        ]

        df = spark.createDataFrame(data, schema)

        # Apply SCD2
        result = apply_scd2(spark, df, target_path, config)

        # Verify result
        assert result["success"] is True
        assert result["records_processed"] == 2

        # Verify effective_from uses created_at timestamp
        result_df = spark.read.format("delta").load(target_path)
        c001_record = result_df.filter(F.col("customer_id") == "C001").first()
        assert c001_record["effective_from"] == datetime(2024, 1, 1, 10, 0)

    def test_scd2_golden_dataset(self, spark, scd2_config, tmp_path):
        """Test SCD2 with golden dataset including complex scenarios."""
        target_path = str(tmp_path / "customers_scd2")

        # Golden dataset with multiple scenarios
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])

        # Scenario 1: Initial load
        initial_data = [
            ("C001", "John Doe", "john@example.com", "123 Main St", datetime(2024, 1, 1, 10, 0)),
            ("C002", "Jane Smith", "jane@example.com", "456 Oak Ave", datetime(2024, 1, 1, 11, 0))
        ]

        df1 = spark.createDataFrame(initial_data, schema)
        result1 = apply_scd2(spark, df1, target_path, scd2_config)
        assert result1["success"] is True
        assert result1["records_processed"] == 2

        # Scenario 2: Updates
        update_data = [
            ("C001", "John Doe", "john.doe@newemail.com", "123 Main St", datetime(2024, 1, 15, 10, 0)),
            ("C003", "Bob Johnson", "bob@example.com", "789 Pine St", datetime(2024, 1, 15, 12, 0))
        ]

        df2 = spark.createDataFrame(update_data, schema)
        result2 = apply_scd2(spark, df2, target_path, scd2_config)
        assert result2["success"] is True
        assert result2["records_processed"] == 2

        # Scenario 3: Late-arriving data
        late_data = [
            ("C001", "John Doe", "john@example.com", "123 Main St", datetime(2024, 1, 5, 10, 0)),
            ("C004", "Alice Brown", "alice@example.com", "321 Elm St", datetime(2024, 1, 5, 13, 0))
        ]

        df3 = spark.createDataFrame(late_data, schema)
        late_effective_from = datetime(2024, 1, 5, 10, 0)
        result3 = apply_scd2(spark, df3, target_path, scd2_config, effective_from=late_effective_from)
        assert result3["success"] is True
        assert result3["records_processed"] == 2

        # Final validation
        final_df = spark.read.format("delta").load(target_path)

        # Should have 7 total records:
        # - C001: 3 records (initial, update, late-arriving)
        # - C002: 1 record (initial)
        # - C003: 1 record (new)
        # - C004: 1 record (late-arriving)
        assert final_df.count() == 7

        # Should have 4 current records
        current_records = final_df.filter(F.col("is_current") == True)
        assert current_records.count() == 4

        # Validate table integrity
        validation_result = validate_scd2_table(spark, target_path, scd2_config)
        assert validation_result["valid"] is True
