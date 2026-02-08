"""
Quality Gate Tests

This test suite ensures that data quality checks meet minimum thresholds.
Failures here should block deployments and DAG execution.
"""

import pytest

from project_a.dq.runner import run_suite
from project_a.utils.config import load_conf
from project_a.utils.spark_session import build_spark


class TestQualityGate:
    """Quality gate tests that must pass for deployment."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for quality gate tests."""
        try:
            config = load_conf("config/local.yaml")
            spark = build_spark(app_name="quality_gate_test", config=config)
        except Exception:
            pytest.skip("Spark unavailable in current environment")
        yield spark

    @pytest.fixture
    def quality_thresholds(self):
        """Load quality thresholds from config."""
        config = load_conf("config/dq.yaml")
        return config.get(
            "quality_gates",
            {"min_pass_rate": 0.95, "critical_checks": ["not_null_keys", "referential_integrity"]},
        )

    def test_silver_orders_quality_gate(self, spark, quality_thresholds):
        """
        Critical: Silver orders must pass all quality checks.
        Fails build if quality below threshold.
        """
        silver_orders_path = "data/lakehouse_delta/silver/orders"

        # Skip if table doesn't exist (e.g., in CI without data)
        try:
            df = spark.read.format("delta").load(silver_orders_path)
            if df.isEmpty():
                pytest.skip("Silver orders table is empty - skipping quality gate")
        except Exception:
            pytest.skip("Silver orders table does not exist - skipping quality gate")

        # Run DQ suite
        result = run_suite("silver_orders_not_null_keys", silver_orders_path, spark)

        # Assert critical checks pass
        assert result.passed, f"Quality gate failed for silver.orders: {result.failures}"

        # Assert pass rate meets threshold
        total_checks = len(result.checks)
        passed_checks = total_checks - len(result.failures)
        pass_rate = passed_checks / total_checks if total_checks > 0 else 0

        min_pass_rate = quality_thresholds.get("min_pass_rate", 0.95)
        assert pass_rate >= min_pass_rate, (
            f"Quality gate pass rate {pass_rate:.2%} below threshold {min_pass_rate:.2%}. "
            f"Failed checks: {result.failures}"
        )

    def test_silver_fx_rates_quality_gate(self, spark, quality_thresholds):
        """
        Critical: Silver FX rates must pass all quality checks.
        """
        silver_fx_path = "data/lakehouse_delta/silver/fx_rates"

        try:
            df = spark.read.format("delta").load(silver_fx_path)
            if df.isEmpty():
                pytest.skip("Silver FX rates table is empty")
        except Exception:
            pytest.skip("Silver FX rates table does not exist")

        result = run_suite("silver_fx_rates_not_null_keys", silver_fx_path, spark)

        assert result.passed, f"Quality gate failed for silver.fx_rates: {result.failures}"

        min_pass_rate = quality_thresholds.get("min_pass_rate", 0.95)
        total_checks = len(result.checks)
        passed_checks = total_checks - len(result.failures)
        pass_rate = passed_checks / total_checks if total_checks > 0 else 0

        assert pass_rate >= min_pass_rate, (
            f"Quality gate pass rate {pass_rate:.2%} below threshold {min_pass_rate:.2%}"
        )

    def test_gold_revenue_quality_gate(self, spark, quality_thresholds):
        """
        Critical: Gold revenue fact table must pass all quality checks.
        """
        gold_revenue_path = "data/lakehouse_delta/gold/fact_revenue"

        try:
            df = spark.read.format("delta").load(gold_revenue_path)
            if df.isEmpty():
                pytest.skip("Gold revenue table is empty")
        except Exception:
            pytest.skip("Gold revenue table does not exist")

        result = run_suite("gold_revenue_not_null_keys", gold_revenue_path, spark)

        assert result.passed, f"Quality gate failed for gold.fact_revenue: {result.failures}"

        min_pass_rate = quality_thresholds.get("min_pass_rate", 0.95)
        total_checks = len(result.checks)
        passed_checks = total_checks - len(result.failures)
        pass_rate = passed_checks / total_checks if total_checks > 0 else 0

        assert pass_rate >= min_pass_rate, (
            f"Quality gate pass rate {pass_rate:.2%} below threshold {min_pass_rate:.2%}"
        )

    def test_critical_checks_defined(self, quality_thresholds):
        """
        Ensure critical checks are defined in config.
        """
        critical_checks = quality_thresholds.get("critical_checks", [])
        assert len(critical_checks) > 0, "No critical checks defined in quality gates config"

        # Verify critical check names are valid
        valid_critical_checks = ["not_null_keys", "referential_integrity", "freshness", "volume"]
        for check in critical_checks:
            assert check in valid_critical_checks, f"Unknown critical check: {check}"
