"""
Alert management for pipeline monitoring (Slack, email, etc.).
"""

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def send_slack_alert(
    message: str,
    severity: str = "info",
    config: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> bool:
    """
    Send alert to Slack via webhook.

    Args:
        message: Alert message
        severity: 'info', 'warning', 'error', 'critical'
        config: Configuration dict with Slack webhook URL
        context: Optional additional context

    Returns:
        True if sent successfully, False otherwise
    """
    if not config:
        logger.warning("No config provided for Slack alert")
        return False

    webhook_url = (
        config.get("monitoring", {}).get("notifications", {}).get("slack", {}).get("webhook_url")
    )
    if not webhook_url:
        logger.debug("Slack webhook URL not configured")
        return False

    color_map = {"info": "#36a64f", "warning": "#ffcc00", "error": "#ff6b6b", "critical": "#cc0000"}

    payload = {
        "attachments": [
            {
                "color": color_map.get(severity, "#36a64f"),
                "title": f"Data Pipeline Alert: {severity.upper()}",
                "text": message,
                "fields": [
                    {
                        "title": "Time",
                        "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "short": True,
                    }
                ],
                "footer": "PySpark Data Engineering Pipeline",
            }
        ]
    }

    if context:
        for key, value in context.items():
            payload["attachments"][0]["fields"].append(
                {"title": key.replace("_", " ").title(), "value": str(value), "short": True}
            )

    try:
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        req = urllib.request.Request(webhook_url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            _ = resp.read()
        logger.info("Slack alert sent: %s - %s...", severity, message[:50])
        return True
    except urllib.error.URLError as exc:
        logger.error("Failed to send Slack alert: %s", exc)
        return False


def send_email_alert(
    message: str, subject: str, recipients: list[str], config: dict[str, Any] | None = None
) -> bool:
    """
    Send alert via email (requires AWS SES or similar).

    Args:
        message: Alert message
        subject: Email subject
        recipients: List of email addresses
        config: Configuration dict

    Returns:
        True if sent successfully, False otherwise
    """
    # This would integrate with AWS SES, SendGrid, etc.
    # For now, just log
    logger.info(f"Email alert (not implemented): {subject} to {recipients}")
    return False


def alert_on_dq_failure(table_name: str, failed_checks: list[str], config: dict[str, Any]) -> bool:
    """
    Send alert when data quality checks fail.

    Args:
        table_name: Name of the table
        failed_checks: List of failed check names
        config: Configuration dict

    Returns:
        True if alert sent successfully
    """
    message = f"Data quality checks failed for table '{table_name}'"
    context = {
        "table": table_name,
        "failed_checks": ", ".join(failed_checks),
        "count": len(failed_checks),
    }

    return send_slack_alert(message, "error", config, context)


def alert_on_sla_breach(
    job_name: str, expected_duration: float, actual_duration: float, config: dict[str, Any]
) -> bool:
    """
    Send alert when SLA is breached.

    Args:
        job_name: Name of the job
        expected_duration: Expected duration in seconds
        actual_duration: Actual duration in seconds
        config: Configuration dict

    Returns:
        True if alert sent successfully
    """
    message = f"Job '{job_name}' exceeded SLA ({actual_duration:.1f}s > {expected_duration:.1f}s)"
    context = {
        "job": job_name,
        "expected_seconds": expected_duration,
        "actual_seconds": actual_duration,
        "overage_seconds": actual_duration - expected_duration,
    }

    return send_slack_alert(message, "warning", config, context)
