#!/usr/bin/env python3
"""
Great Expectations DQ check runner with failure handling.

Runs GE suites and fails pipeline on critical check failures.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

try:
    import great_expectations as ge
    from great_expectations.core.expectation_suite import ExpectationSuite

    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False
    ge = None

logger = logging.getLogger(__name__)


def run_ge_suite(
    suite_name: str, data_asset: str, critical_only: bool = False, config_path: str = None
) -> dict:
    """
    Run Great Expectations suite and return results.

    Args:
        suite_name: Name of GE suite (e.g., 'bronze_salesforce_accounts')
        data_asset: Data asset path or table name
        critical_only: Only run critical expectations
        config_path: Path to GE config

    Returns:
        Dictionary with results
    """
    if not GE_AVAILABLE:
        logger.error("Great Expectations not available")
        return {"passed": False, "error": "GE not installed"}

    try:
        # Initialize GE context
        if config_path:
            context = ge.get_context(project_root=config_path)
        else:
            context = ge.get_context()

        # Load suite
        suite = context.get_expectation_suite(suite_name)

        # Filter critical expectations if needed
        if critical_only:
            suite = _filter_critical_expectations(suite)

        # Create validator
        validator = context.get_validator(data_asset=data_asset, expectation_suite=suite)

        # Run suite
        logger.info(f"Running GE suite: {suite_name}")
        logger.info(f"Data asset: {data_asset}")
        logger.info(f"Expectations: {len(suite.expectations)}")

        results = validator.validate()

        # Process results
        passed = results.success
        stats = results.statistics

        logger.info(f"✅ Suite {'PASSED' if passed else 'FAILED'}")
        logger.info(f"   Expectations: {stats.get('evaluated_expectations', 0)}")
        logger.info(f"   Successful: {stats.get('successful_expectations', 0)}")
        logger.info(f"   Failed: {stats.get('unsuccessful_expectations', 0)}")

        return {
            "passed": passed,
            "suite_name": suite_name,
            "data_asset": data_asset,
            "statistics": stats,
            "results": results.to_json_dict(),
        }

    except Exception as e:
        logger.error(f"Failed to run GE suite: {e}")
        return {"passed": False, "error": str(e)}


def _filter_critical_expectations(suite: ExpectationSuite) -> ExpectationSuite:
    """Filter to only critical expectations."""
    critical_suite = ExpectationSuite(suite_name=suite.expectation_suite_name)

    for expectation in suite.expectations:
        # Check if expectation is marked as critical
        if hasattr(expectation, "meta") and expectation.meta.get("critical", False):
            critical_suite.add_expectation(expectation)

    return critical_suite


def save_dq_results(results: dict, output_path: str):
    """Save DQ results to S3 or local file."""
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if output_path.startswith("s3://"):
        # Use boto3 to write
        logger.info(f"Saving DQ results to S3: {output_path}")
        # Implementation would use boto3.put_object()
    else:
        # Local file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        output_file = Path(output_path) / f"dq_results_{date_str}.json"

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)

        logger.info(f"Saved DQ results to: {output_file}")


def send_dq_alert(results: dict, config: dict):
    """Send alert on DQ failure."""
    if results.get("passed"):
        return

    logger.error("🚨 DQ CHECK FAILED - Sending alert")

    # Get alerting config
    alerts_config = config.get("monitoring", {}).get("alerts", {})

    # Send Slack alert (if configured)
    slack_webhook = alerts_config.get("slack_webhook")
    if slack_webhook:
        try:
            import requests

            message = {
                "text": "❌ Data Quality Check Failed",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Suite:* {results.get('suite_name')}\n*Asset:* {results.get('data_asset')}\n*Failed:* {results.get('statistics', {}).get('unsuccessful_expectations', 0)}",
                        },
                    }
                ],
            }

            requests.post(slack_webhook, json=message)
            logger.info("✅ Sent Slack alert")
        except Exception as e:
            logger.warning(f"Could not send Slack alert: {e}")

    # Send email alert (if configured)
    # Implementation would use boto3 SES


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Great Expectations DQ checks")
    parser.add_argument("--suite-name", required=True, help="GE suite name")
    parser.add_argument("--data-asset", required=True, help="Data asset path or table")
    parser.add_argument(
        "--critical-only", action="store_true", help="Only run critical expectations"
    )
    parser.add_argument(
        "--output", default="data/metrics/dq_results", help="Output path for results"
    )
    parser.add_argument(
        "--fail-on-error", action="store_true", default=True, help="Exit with error code on failure"
    )
    parser.add_argument("--config", help="GE config path")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    # Load config (if needed for alerts)
    config = {}

    # Run suite
    results = run_ge_suite(
        suite_name=args.suite_name,
        data_asset=args.data_asset,
        critical_only=args.critical_only,
        config_path=args.config,
    )

    # Save results
    save_dq_results(results, args.output)

    # Send alert if failed
    if not results.get("passed"):
        send_dq_alert(results, config)

    # Exit with error code if failed
    if args.fail_on_error and not results.get("passed"):
        logger.error("❌ DQ checks failed - exiting with error code")
        sys.exit(1)

    logger.info("✅ DQ checks passed")
