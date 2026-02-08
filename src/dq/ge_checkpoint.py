"""
Great Expectations data quality checkpoint runner.
Runs GE suites on silver/gold tables and fails on critical violations.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List

import boto3
from great_expectations import DataContext
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

logger = logging.getLogger(__name__)


class GECheckpointRunner:
    """Great Expectations checkpoint runner for data quality validation."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GE checkpoint runner.

        Args:
            config: Configuration dictionary with GE settings
        """
        self.config = config
        self.dq_config = config.get("dq", {})
        self.s3_client = boto3.client("s3")

        # Initialize GE context
        self.context = self._init_ge_context()

    def _init_ge_context(self) -> DataContext:
        """Initialize Great Expectations context."""
        ge_config_path = Path("ge/great_expectations.yml")
        if not ge_config_path.exists():
            raise FileNotFoundError(f"GE config not found: {ge_config_path}")

        return DataContext(context_root_dir=str(ge_config_path.parent))

    def run_checkpoint(self, table_name: str, layer: str, data_path: str) -> Dict[str, Any]:
        """
        Run GE checkpoint on specified table.

        Args:
            table_name: Name of the table to validate
            layer: Data layer (bronze/silver/gold)
            data_path: S3 path to the data

        Returns:
            Checkpoint result dictionary
        """
        logger.info(f"Running GE checkpoint for {layer}.{table_name}")

        # Create runtime batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="s3_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name=f"{layer}_{table_name}",
            runtime_parameters={"path": data_path},
            batch_identifiers={"default_identifier_name": "default_identifier"},
        )

        # Run checkpoint
        checkpoint_name = f"{layer}_{table_name}_checkpoint"
        checkpoint_result = self.context.run_checkpoint(
            checkpoint_name=checkpoint_name, validations=[{"batch_request": batch_request}]
        )

        # Analyze results
        result = self._analyze_checkpoint_result(checkpoint_result, table_name, layer)

        return result

    def _analyze_checkpoint_result(
        self, checkpoint_result: Any, table_name: str, layer: str
    ) -> Dict[str, Any]:
        """Analyze checkpoint result and determine if critical failures occurred."""
        result = {
            "table_name": table_name,
            "layer": layer,
            "success": checkpoint_result.success,
            "critical_failures": [],
            "warnings": [],
            "summary": {},
        }

        # Extract validation results
        validation_results = checkpoint_result.list_validation_results()

        for validation_result in validation_results:
            validation_id = validation_result.meta.get("run_id", "unknown")
            statistics = validation_result.statistics

            result["summary"][validation_id] = {
                "evaluated_expectations": statistics.get("evaluated_expectations", 0),
                "successful_expectations": statistics.get("successful_expectations", 0),
                "unsuccessful_expectations": statistics.get("unsuccessful_expectations", 0),
                "success_percent": statistics.get("success_percent", 0),
            }

            # Check for critical failures
            for expectation_result in validation_result.results:
                if not expectation_result.success:
                    expectation_type = expectation_result.expectation_config.get(
                        "expectation_type", "unknown"
                    )
                    column = expectation_result.expectation_config.get("kwargs", {}).get(
                        "column", "unknown"
                    )

                    # Determine severity based on expectation type
                    severity = self._get_expectation_severity(expectation_type)

                    failure_info = {
                        "expectation_type": expectation_type,
                        "column": column,
                        "severity": severity,
                        "details": expectation_result.result,
                    }

                    if severity == "critical":
                        result["critical_failures"].append(failure_info)
                    else:
                        result["warnings"].append(failure_info)

        # Determine overall success
        result["success"] = len(result["critical_failures"]) == 0

        return result

    def _get_expectation_severity(self, expectation_type: str) -> str:
        """Determine severity level for expectation type."""
        critical_expectations = [
            "expect_column_to_exist",
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_unique",
            "expect_table_row_count_to_be_between",
        ]

        if expectation_type in critical_expectations:
            return "critical"
        else:
            return "warning"

    def run_silver_quality_checks(self, silver_tables: List[str]) -> Dict[str, Any]:
        """Run quality checks on all silver tables."""
        results = {}
        overall_success = True

        for table in silver_tables:
            data_path = f"{self.config['s3']['silver_path']}/{table}"
            result = self.run_checkpoint(table, "silver", data_path)
            results[table] = result

            if not result["success"]:
                overall_success = False
                logger.error(f"Critical failures in silver.{table}: {result['critical_failures']}")

        return {"layer": "silver", "overall_success": overall_success, "table_results": results}

    def run_gold_quality_checks(self, gold_tables: List[str]) -> Dict[str, Any]:
        """Run quality checks on all gold tables."""
        results = {}
        overall_success = True

        for table in gold_tables:
            data_path = f"{self.config['s3']['gold_path']}/{table}"
            result = self.run_checkpoint(table, "gold", data_path)
            results[table] = result

            if not result["success"]:
                overall_success = False
                logger.error(f"Critical failures in gold.{table}: {result['critical_failures']}")

        return {"layer": "gold", "overall_success": overall_success, "table_results": results}

    def publish_results(self, results: Dict[str, Any]) -> str:
        """Publish GE results to S3 and return Data Docs URL."""
        # Generate Data Docs
        self.context.build_data_docs()

        # Upload to S3
        docs_bucket = self.dq_config.get("great_expectations_bucket", "")
        if docs_bucket:
            self._upload_data_docs_to_s3(docs_bucket)
            docs_url = f"s3://{docs_bucket}/index.html"
        else:
            docs_url = "file://ge/uncommitted/data_docs/local_site/index.html"

        logger.info(f"Data Docs published to: {docs_url}")
        return docs_url

    def _upload_data_docs_to_s3(self, bucket: str):
        """Upload Data Docs to S3."""
        docs_dir = Path("ge/uncommitted/data_docs/local_site")

        for file_path in docs_dir.rglob("*"):
            if file_path.is_file():
                s3_key = f"ge-docs/{file_path.relative_to(docs_dir)}"
                self.s3_client.upload_file(str(file_path), bucket, s3_key)


def main():
    """Main entry point for GE checkpoint runner."""
    import argparse
    from project_a.utils.config import load_conf

    parser = argparse.ArgumentParser(description="Run Great Expectations data quality checks")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument(
        "--layer", choices=["silver", "gold"], required=True, help="Data layer to check"
    )
    parser.add_argument("--tables", nargs="+", required=True, help="Tables to validate")

    args = parser.parse_args()

    # Load configuration
    config = load_conf(args.config)

    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Run quality checks
    runner = GECheckpointRunner(config)

    if args.layer == "silver":
        results = runner.run_silver_quality_checks(args.tables)
    else:
        results = runner.run_gold_quality_checks(args.tables)

    # Publish results
    docs_url = runner.publish_results(results)

    # Print results
    print(json.dumps(results, indent=2))
    print(f"\nData Docs URL: {docs_url}")

    # Exit with error code if critical failures
    if not results["overall_success"]:
        logger.error("Data quality checks failed with critical violations")
        sys.exit(1)
    else:
        logger.info("All data quality checks passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
