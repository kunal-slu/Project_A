"""
SCD2 validation tests to ensure data integrity and proper implementation.
"""

import pytest
from pyspark.sql import SparkSession, functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType


class TestSCD2Validation:
    """Test SCD2 implementation for data integrity."""

    @pytest.fixture
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder \
            .appName("SCD2Validation") \
            .master("local[2]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    @pytest.fixture
    def sample_scd2_data(self, spark):
        """Create sample SCD2 data for testing."""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("address", StringType(), True),
            StructField("effective_from", DateType(), True),
            StructField("effective_to", DateType(), True),
            StructField("is_current", BooleanType(), True)
        ])

        data = [
            ("C001", "123 Main St", "2020-01-01", "2023-06-15", False),
            ("C001", "456 Oak Ave", "2023-06-15", None, True),
            ("C002", "789 Pine St", "2021-03-01", None, True),
            ("C003", "321 Elm St", "2022-01-01", "2023-12-01", False),
            ("C003", "654 Maple Dr", "2023-12-01", None, True)
        ]

        return spark.createDataFrame(data, schema)

    def test_scd2_required_columns(self, sample_scd2_data):
        """Test that SCD2 table has all required columns."""
        required_cols = {"customer_id", "effective_from", "effective_to", "is_current"}
        actual_cols = set(sample_scd2_data.columns)

        assert required_cols.issubset(actual_cols), f"Missing required columns: {required_cols - actual_cols}"

    def test_scd2_single_current_record_per_key(self, sample_scd2_data):
        """Test that each business key has exactly one current record."""
        violations = (sample_scd2_data
                     .filter(F.col("is_current") == True)
                     .groupBy("customer_id")
                     .count()
                     .filter("count != 1"))

        assert violations.count() == 0, "Found business keys with multiple or zero current records"

    def test_scd2_no_date_overlaps(self, sample_scd2_data):
        """Test that there are no overlapping date ranges for the same business key."""
        w = W.partitionBy("customer_id").orderBy("effective_from")

        overlaps = (sample_scd2_data
                   .withColumn("next_from", F.lead("effective_from").over(w))
                   .filter("next_from is not null and next_from <= effective_to"))

        assert overlaps.count() == 0, "Found overlapping date ranges in SCD2 data"

    def test_scd2_current_records_have_null_effective_to(self, sample_scd2_data):
        """Test that current records have null effective_to."""
        violations = sample_scd2_data.filter(
            (F.col("is_current") == True) & (F.col("effective_to").isNotNull())
        )

        assert violations.count() == 0, "Current records should have null effective_to"

    def test_scd2_historical_records_have_effective_to(self, sample_scd2_data):
        """Test that historical records have non-null effective_to."""
        violations = sample_scd2_data.filter(
            (F.col("is_current") == False) & (F.col("effective_to").isNull())
        )

        assert violations.count() == 0, "Historical records should have non-null effective_to"

    def test_scd2_effective_from_before_effective_to(self, sample_scd2_data):
        """Test that effective_from is before effective_to for historical records."""
        violations = sample_scd2_data.filter(
            (F.col("is_current") == False) &
            (F.col("effective_from") >= F.col("effective_to"))
        )

        assert violations.count() == 0, "effective_from should be before effective_to"

    def test_scd2_no_duplicate_records(self, sample_scd2_data):
        """Test that there are no duplicate records for the same business key and effective_from."""
        duplicates = (sample_scd2_data
                     .groupBy("customer_id", "effective_from")
                     .count()
                     .filter("count > 1"))

        assert duplicates.count() == 0, "Found duplicate records for same business key and effective_from"

    def test_scd2_continuous_date_ranges(self, sample_scd2_data):
        """Test that date ranges are continuous (no gaps)."""
        w = W.partitionBy("customer_id").orderBy("effective_from")

        gaps = (sample_scd2_data
                .withColumn("prev_to", F.lag("effective_to").over(w))
                .filter("prev_to is not null and effective_from > prev_to"))

        assert gaps.count() == 0, "Found gaps in date ranges"

    def test_scd2_business_key_not_null(self, sample_scd2_data):
        """Test that business key is never null."""
        violations = sample_scd2_data.filter(F.col("customer_id").isNull())

        assert violations.count() == 0, "Business key should never be null"

    def test_scd2_effective_from_not_null(self, sample_scd2_data):
        """Test that effective_from is never null."""
        violations = sample_scd2_data.filter(F.col("effective_from").isNull())

        assert violations.count() == 0, "effective_from should never be null"

    def test_scd2_is_current_not_null(self, sample_scd2_data):
        """Test that is_current flag is never null."""
        violations = sample_scd2_data.filter(F.col("is_current").isNull())

        assert violations.count() == 0, "is_current flag should never be null"


def validate_scd2_table(spark: SparkSession, table_path: str, business_key: str = "customer_id") -> dict:
    """
    Validate SCD2 table and return validation results.

    Args:
        spark: SparkSession
        table_path: Path to SCD2 table
        business_key: Business key column name

    Returns:
        Dictionary with validation results
    """
    try:
        df = spark.read.format("delta").load(table_path)

        results = {
            "table_exists": True,
            "total_records": df.count(),
            "current_records": df.filter(F.col("is_current") == True).count(),
            "historical_records": df.filter(F.col("is_current") == False).count(),
            "validation_passed": True,
            "errors": []
        }

        # Check required columns
        required_cols = {business_key, "effective_from", "effective_to", "is_current"}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            results["validation_passed"] = False
            results["errors"].append(f"Missing required columns: {missing_cols}")

        # Check single current record per key
        violations = (df.filter(F.col("is_current") == True)
                     .groupBy(business_key)
                     .count()
                     .filter("count != 1"))
        if violations.count() > 0:
            results["validation_passed"] = False
            results["errors"].append("Found business keys with multiple or zero current records")

        # Check no overlaps
        w = W.partitionBy(business_key).orderBy("effective_from")
        overlaps = (df.withColumn("next_from", F.lead("effective_from").over(w))
                   .filter("next_from is not null and next_from <= effective_to"))
        if overlaps.count() > 0:
            results["validation_passed"] = False
            results["errors"].append("Found overlapping date ranges")

        return results

    except Exception as e:
        return {
            "table_exists": False,
            "validation_passed": False,
            "errors": [f"Failed to read table: {str(e)}"]
        }
