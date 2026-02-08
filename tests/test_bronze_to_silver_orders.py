"""
Test bronze to silver transformation for orders.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from project_a.dq.gate import run_dq_gate
from project_a.utils.contracts import enforce_not_null, load_contract


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for tests."""
    try:
        return SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    except Exception:
        pytest.skip("Spark unavailable in current environment")


def test_orders_null_order_id_filtered(spark):
    """Test that null order_id rows are dropped or quarantined."""
    contract = load_contract("snowflake_orders.schema.json")

    # Create DataFrame with null order_id
    df = spark.createDataFrame(
        [
            ("o1", "c1", "p1", "2024-01-01", 100.0, "USD", "completed"),
            (None, "c2", "p2", "2024-01-02", 200.0, "USD", "pending"),  # null order_id
            ("o3", "c3", "p3", "2024-01-03", 300.0, "USD", "completed"),
        ],
        [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "order_amount",
            "currency",
            "status",
        ],
    )

    # Filter nulls in required columns
    df_clean = enforce_not_null(df, contract.required)

    # Should only have 2 rows (o1 and o3)
    assert df_clean.count() == 2

    # Verify no null order_ids remain
    null_count = df_clean.filter(F.col("order_id").isNull()).count()
    assert null_count == 0


def test_orders_dq_gate_passes(spark):
    """Test that DQ gate passes for valid orders data."""
    contract = load_contract("snowflake_orders.schema.json")

    # Create valid DataFrame
    df = spark.createDataFrame(
        [
            ("o1", "c1", "p1", "2024-01-01", 100.0, "USD", "completed"),
            ("o2", "c2", "p2", "2024-01-02", 200.0, "USD", "pending"),
        ],
        [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "order_amount",
            "currency",
            "status",
        ],
    )

    # Run DQ gate
    result = run_dq_gate(
        spark=spark,
        df=df,
        table_name="orders",
        primary_key=contract.primary_key,
        required_cols=contract.required,
        check_uniqueness=True,
    )

    assert result.passed is True
    assert result.total_rows == 2


def test_orders_dq_gate_fails_on_null(spark):
    """Test that DQ gate fails when required columns have nulls."""
    contract = load_contract("snowflake_orders.schema.json")

    # Create DataFrame with nulls in required columns
    df = spark.createDataFrame(
        [
            ("o1", "c1", "p1", "2024-01-01", 100.0, "USD", "completed"),
            (None, "c2", "p2", "2024-01-02", 200.0, "USD", "pending"),  # null order_id
        ],
        [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "order_amount",
            "currency",
            "status",
        ],
    )

    # Run DQ gate - should raise ValueError
    with pytest.raises(ValueError, match="DQ Gate FAILED"):
        run_dq_gate(
            spark=spark,
            df=df,
            table_name="orders",
            primary_key=contract.primary_key,
            required_cols=contract.required,
            check_uniqueness=True,
        )
