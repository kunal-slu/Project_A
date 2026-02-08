#!/usr/bin/env python3
"""
SLA Breach Alert Publisher

Checks CloudWatch metrics for job runtime and record counts,
then sends alerts to Slack/SNS if SLAs are violated.
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any

sys.path.insert(0, os.path.dirname(__file__).replace("/jobs", "/.."))

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None

logger = logging.getLogger(__name__)


class SLAChecker:
    """Checks SLA compliance and sends alerts."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.cloudwatch = boto3.client("cloudwatch") if boto3 else None
        self.sns = boto3.client("sns") if boto3 else None
        self.slack_webhook = config.get("alerts", {}).get("slack_webhook")
        self.sns_topic_arn = config.get("alerts", {}).get("sns_topic_arn")

        # SLA thresholds from config
        self.sla_config = config.get("slas", {})
        self.default_runtime_minutes = self.sla_config.get("default_runtime_minutes", 60)
        self.min_records_threshold = self.sla_config.get("min_records_threshold", 1)

    def get_job_metrics(self, job_name: str, hours_back: int = 1) -> dict[str, Any]:
        """
        Get CloudWatch metrics for a job.

        Args:
            job_name: Name of the job
            hours_back: How many hours back to look

        Returns:
            Dictionary with metrics
        """
        if not self.cloudwatch:
            logger.warning("CloudWatch not available, returning mock metrics")
            return {"runtime_minutes": 0, "records_processed": 0}

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        try:
            # Get runtime metric
            runtime_response = self.cloudwatch.get_metric_statistics(
                Namespace="ETL/Jobs",
                MetricName="JobRuntime",
                Dimensions=[{"Name": "JobName", "Value": job_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=["Average", "Maximum"],
            )

            # Get records processed metric
            records_response = self.cloudwatch.get_metric_statistics(
                Namespace="ETL/Jobs",
                MetricName="RecordsProcessed",
                Dimensions=[{"Name": "JobName", "Value": job_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=["Sum"],
            )

            runtime_minutes = 0
            if runtime_response.get("Datapoints"):
                max_runtime = max(dp["Maximum"] for dp in runtime_response["Datapoints"])
                runtime_minutes = max_runtime / 60

            records_processed = 0
            if records_response.get("Datapoints"):
                records_processed = sum(dp["Sum"] for dp in records_response["Datapoints"])

            return {"runtime_minutes": runtime_minutes, "records_processed": int(records_processed)}

        except ClientError as e:
            logger.error(f"Failed to get CloudWatch metrics: {str(e)}")
            return {"runtime_minutes": 0, "records_processed": 0}

    def check_sla_breach(self, job_name: str) -> dict[str, Any] | None:
        """
        Check if job breached SLA.

        Args:
            job_name: Name of the job

        Returns:
            Alert dictionary if breach detected, None otherwise
        """
        metrics = self.get_job_metrics(job_name)
        runtime_minutes = metrics.get("runtime_minutes", 0)
        records_processed = metrics.get("records_processed", 0)

        # Get job-specific SLA if available
        job_sla = self.sla_config.get("jobs", {}).get(job_name, {})
        max_runtime_minutes = job_sla.get("runtime_minutes", self.default_runtime_minutes)
        min_records = job_sla.get("min_records", self.min_records_threshold)

        breaches = []

        if runtime_minutes > max_runtime_minutes:
            breaches.append(
                f"Runtime {runtime_minutes:.1f} min exceeds SLA {max_runtime_minutes} min"
            )

        if records_processed < min_records:
            breaches.append(f"Records processed {records_processed} below threshold {min_records}")

        if breaches:
            return {
                "job_name": job_name,
                "breaches": breaches,
                "metrics": metrics,
                "sla": {"max_runtime_minutes": max_runtime_minutes, "min_records": min_records},
                "timestamp": datetime.utcnow().isoformat(),
            }

        return None

    def send_slack_alert(self, alert: dict[str, Any]) -> bool:
        """Send alert to Slack webhook."""
        if not self.slack_webhook:
            logger.warning("Slack webhook not configured")
            return False

        if not boto3:
            logger.warning("boto3 not available, skipping Slack alert")
            return False

        try:
            import urllib.parse
            import urllib.request

            message = {
                "text": f"🚨 SLA Breach Alert: {alert['job_name']}",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"SLA Breach: {alert['job_name']}"},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Breaches:*\n{chr(10).join(alert['breaches'])}",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Metrics:*\nRuntime: {alert['metrics']['runtime_minutes']:.1f} min\nRecords: {alert['metrics']['records_processed']}",
                            },
                        ],
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"*Timestamp:* {alert['timestamp']}"},
                    },
                ],
            }

            req = urllib.request.Request(
                self.slack_webhook,
                data=json.dumps(message).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )

            urllib.request.urlopen(req)
            logger.info(f"Sent Slack alert for {alert['job_name']}")
            return True

        except Exception as e:
            logger.error(f"Failed to send Slack alert: {str(e)}")
            return False

    def send_sns_alert(self, alert: dict[str, Any]) -> bool:
        """Send alert to SNS topic."""
        if not self.sns_topic_arn or not self.sns:
            logger.warning("SNS not configured")
            return False

        try:
            subject = f"SLA Breach: {alert['job_name']}"
            message = json.dumps(alert, indent=2)

            self.sns.publish(TopicArn=self.sns_topic_arn, Subject=subject, Message=message)

            logger.info(f"Sent SNS alert for {alert['job_name']}")
            return True

        except Exception as e:
            logger.error(f"Failed to send SNS alert: {str(e)}")
            return False

    def check_and_alert(self, job_name: str) -> bool:
        """
        Check SLA for job and send alerts if breach detected.

        Args:
            job_name: Name of the job

        Returns:
            True if breach detected, False otherwise
        """
        alert = self.check_sla_breach(job_name)

        if alert:
            logger.warning(f"SLA breach detected for {job_name}: {alert['breaches']}")

            # Send alerts
            slack_sent = self.send_slack_alert(alert)
            sns_sent = self.send_sns_alert(alert)

            if slack_sent or sns_sent:
                return True
            else:
                logger.error("Failed to send any alerts")
                return True

        logger.info(f"No SLA breach for {job_name}")
        return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Check SLA and send alerts")
    parser.add_argument("--config", required=True, help="Configuration file")
    parser.add_argument("--job-name", required=True, help="Job name to check")
    args = parser.parse_args()

    # Load config
    try:
        import yaml

        with open(args.config) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {str(e)}")
        sys.exit(1)

    checker = SLAChecker(config)
    breach_detected = checker.check_and_alert(args.job_name)

    sys.exit(1 if breach_detected else 0)


if __name__ == "__main__":
    main()
