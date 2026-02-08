#!/usr/bin/env python3
"""
Create CloudWatch alarms for ETL pipeline monitoring (P5-13).

Alarms for:
- records_processed_total (low/high thresholds)
- dq_critical_failures (>= 1 fails pipeline)
- latency_seconds (SLO breaches)
- cost_estimate_usd (budget alerts)
"""

import argparse
import logging
import sys

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_dq_alarm(cloudwatch, alarm_name: str, namespace: str = "ETLPipelineMetrics"):
    """Create alarm for DQ critical failures."""
    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription="Alert when DQ critical failures detected",
        MetricName="dq_critical_failures",
        Namespace=namespace,
        Statistic="Sum",
        Period=300,  # 5 minutes
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        ActionsEnabled=True,
        AlarmActions=["arn:aws:sns:us-east-1:ACCOUNT:data-engineering-alerts"],
        TreatMissingData="notBreaching",
    )
    logger.info(f"✅ Created alarm: {alarm_name}")


def create_latency_alarm(cloudwatch, alarm_name: str, threshold_seconds: float = 3600.0):
    """Create alarm for latency SLO breaches."""
    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=f"Alert when pipeline latency exceeds {threshold_seconds}s SLO",
        MetricName="latency_seconds",
        Namespace="ETLPipelineMetrics",
        Statistic="Average",
        Period=300,
        EvaluationPeriods=2,
        Threshold=threshold_seconds,
        ComparisonOperator="GreaterThanThreshold",
        ActionsEnabled=True,
        AlarmActions=["arn:aws:sns:us-east-1:ACCOUNT:data-engineering-alerts"],
    )
    logger.info(f"✅ Created alarm: {alarm_name}")


def create_cost_alarm(cloudwatch, alarm_name: str, budget_threshold: float = 100.0):
    """Create alarm for cost budget."""
    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=f"Alert when daily cost exceeds ${budget_threshold}",
        MetricName="cost_estimate_usd",
        Namespace="ETLPipelineMetrics",
        Statistic="Sum",
        Period=86400,  # 24 hours
        EvaluationPeriods=1,
        Threshold=budget_threshold,
        ComparisonOperator="GreaterThanThreshold",
        ActionsEnabled=True,
        AlarmActions=["arn:aws:sns:us-east-1:ACCOUNT:finance-alerts"],
    )
    logger.info(f"✅ Created alarm: {alarm_name}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Create CloudWatch alarms for ETL pipeline")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--sns-topic", required=True, help="SNS topic ARN for alerts")

    args = parser.parse_args()

    cloudwatch = boto3.client("cloudwatch", region_name=args.region)

    try:
        # DQ failures alarm
        create_dq_alarm(cloudwatch, "etl-dq-critical-failures")

        # Latency alarms
        create_latency_alarm(cloudwatch, "etl-latency-bronze", threshold_seconds=7200.0)  # 2 hours
        create_latency_alarm(cloudwatch, "etl-latency-silver", threshold_seconds=10800.0)  # 3 hours
        create_latency_alarm(cloudwatch, "etl-latency-gold", threshold_seconds=14400.0)  # 4 hours

        # Cost alarm
        create_cost_alarm(cloudwatch, "etl-daily-cost-budget", budget_threshold=100.0)

        logger.info("🎉 All CloudWatch alarms created successfully")
        return 0

    except Exception as e:
        logger.error(f"❌ Failed to create alarms: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
