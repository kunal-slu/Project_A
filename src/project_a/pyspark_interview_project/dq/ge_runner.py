"""
Great Expectations runner with DQ breaker logic.

Runs GE checks and fails pipeline if critical rules are violated.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import great_expectations as ge
    from great_expectations.dataset import SparkDFDataset

    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False
    ge = None

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class GERunner:
    """Great Expectations runner with DQ breaker support."""

    def __init__(self, config: dict[str, Any]):
        """
        Initialize GE runner.

        Args:
            config: Configuration dictionary
        """
        self.config = config

        # Load DQ config from file
        self.dq_config = self._load_dq_config(config)
        self.critical_rules = self.dq_config.get("quality_gates", {}).get("critical_checks", [])
        # Use config-based DQ results path
        self.dq_results_path = config.get("paths", {}).get("dq_results_root") or config.get(
            "data_lake", {}
        ).get("dq_results_path", "s3://my-etl-lake-demo/_dq_results")

    def _load_dq_config(self, config: dict[str, Any]) -> dict[str, Any]:
        """Load DQ configuration from config/dq.yaml."""
        import yaml

        try:
            config_path = Path("config/dq.yaml")
            if config_path.exists():
                with open(config_path) as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Could not load DQ config: {e}")

        # Fallback to inline config
        return config.get("dq", {})

    def run_suite(
        self,
        spark: SparkSession,
        df: DataFrame,
        suite_name: str,
        layer: str,
        execution_date: str = None,
    ) -> dict[str, Any]:
        """
        Run GE expectations suite.

        Args:
            spark: SparkSession
            df: DataFrame to validate
            suite_name: Suite name (e.g., 'bronze_behavior')
            layer: Layer name (bronze, silver, gold)
            execution_date: Execution date

        Returns:
            Results dictionary with pass/fail status
        """
        if not GE_AVAILABLE:
            logger.warning("Great Expectations not available, skipping DQ checks")
            return {"passed": True, "skipped": True}

        if execution_date is None:
            execution_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Running DQ suite: {suite_name} (layer: {layer})")

        # Convert to GE dataset
        ge_df = SparkDFDataset(df)

        # Load suite from config
        suite_config = self._load_suite_config(suite_name, layer)

        if not suite_config:
            logger.warning(f"Suite config not found: {suite_name}")
            return {"passed": True, "skipped": True}

        results = {
            "suite_name": suite_name,
            "layer": layer,
            "execution_date": execution_date,
            "timestamp": datetime.utcnow().isoformat(),
            "expectations": [],
            "passed": True,
            "critical_failures": 0,
            "warning_failures": 0,
        }

        # Run expectations
        for expectation in suite_config.get("expectations", []):
            exp_result = self._run_expectation(ge_df, expectation)
            results["expectations"].append(exp_result)

            if not exp_result.get("passed"):
                results["passed"] = False

                # Check if critical
                if exp_result.get("expectation_type") in self.critical_rules:
                    results["critical_failures"] += 1
                else:
                    results["warning_failures"] += 1

        # Write results
        self._write_results(results, suite_name, execution_date)

        # Emit to CloudWatch
        self._emit_metrics(results, suite_name)

        # DQ Breaker: Fail if critical violations
        if results["critical_failures"] > 0:
            error_msg = (
                f"DQ critical failures: {results['critical_failures']} violations in {suite_name}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(
            f"✅ DQ suite passed: {suite_name} ({len(results['expectations'])} expectations)"
        )

        return results

    def _load_suite_config(self, suite_name: str, layer: str) -> dict[str, Any]:
        """Load suite configuration from dq.yaml."""
        layer_config = self.dq_config.get(layer, {})
        return layer_config.get(suite_name, {})

    def _run_expectation(self, ge_df, expectation: dict[str, Any]) -> dict[str, Any]:
        """Run a single expectation."""
        exp_type = expectation.get("type")
        kwargs = expectation.get("kwargs", {})

        try:
            # Map to GE method
            method = getattr(ge_df, exp_type, None)
            if not method:
                logger.warning(f"Unknown expectation type: {exp_type}")
                return {"passed": True, "skipped": True, "expectation_type": exp_type}

            result = method(**kwargs)

            return {
                "expectation_type": exp_type,
                "passed": result.get("success", False),
                "kwargs": kwargs,
                "result": result,
            }
        except Exception as e:
            logger.error(f"Failed to run expectation {exp_type}: {e}")
            return {"expectation_type": exp_type, "passed": False, "error": str(e)}

    def _write_results(self, results: dict[str, Any], suite_name: str, execution_date: str):
        """Write DQ results to S3."""
        results_json = json.dumps(results, indent=2, default=str)

        # Write to S3 or local
        results_path = f"{self.dq_results_path}/dt={execution_date}/{suite_name}.json"

        try:
            if results_path.startswith("s3://"):
                import boto3

                s3 = boto3.client("s3")
                bucket, key = results_path.replace("s3://", "").split("/", 1)
                s3.put_object(Bucket=bucket, Key=key, Body=results_json.encode("utf-8"))
            else:
                # Local
                Path(results_path).parent.mkdir(parents=True, exist_ok=True)
                with open(results_path, "w") as f:
                    f.write(results_json)

            logger.info(f"DQ results written to: {results_path}")
        except Exception as e:
            logger.warning(f"Failed to write DQ results: {e}")

    def _emit_metrics(self, results: dict[str, Any], suite_name: str):
        """Emit DQ metrics to CloudWatch."""
        try:
            from project_a.monitoring.metrics_collector import emit_rowcount

            emit_rowcount(
                "dq_failed_records",
                results.get("critical_failures", 0),
                {"suite": suite_name, "layer": results.get("layer")},
                self.config,
            )
        except Exception as e:
            logger.warning(f"Failed to emit DQ metrics: {e}")
