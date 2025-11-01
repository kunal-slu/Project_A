"""
Airflow helper utilities for monitoring and orchestration.

Integrates with CloudWatch, DataDog, Slack for observability.
"""
import os
import logging
import requests
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


def send_dd_event(
    title: str,
    text: str,
    alert_type: str = "info",
    tags: Optional[List[str]] = None
):
    """
    Send event to DataDog for monitoring.
    
    Args:
        title: Event title
        text: Event description
        alert_type: "info", "warning", "error", "success"
        tags: List of tags
        
    Example:
        send_dd_event("ETL Success", "Pipeline completed", tags=["env:prod"])
    """
    datadog_api_key = os.getenv("DATADOG_API_KEY")
    if not datadog_api_key:
        logger.debug("DataDog API key not configured, skipping event")
        return
    
    url = "https://api.datadoghq.com/api/v1/events"
    payload = {
        "title": title,
        "text": text,
        "alert_type": alert_type,
        "tags": tags or ["service:pyspark-interview-project"]
    }
    headers = {
        "DD-API-KEY": datadog_api_key,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=3)
        response.raise_for_status()
        logger.debug(f"DataDog event sent: {title}")
    except Exception as e:
        logger.warning(f"Failed to send DataDog event: {e}")


def send_slack_alert(
    message: str,
    channel: str = "#etl-alerts",
    username: str = "ETL Bot"
):
    """
    Send alert to Slack channel.
    
    Args:
        message: Alert message
        channel: Slack channel (with #)
        username: Bot username
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.debug("Slack webhook not configured, skipping alert")
        return
    
    payload = {
        "channel": channel,
        "username": username,
        "text": message,
        "icon_emoji": ":robot_face:"
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=3)
        response.raise_for_status()
        logger.debug(f"Slack alert sent to {channel}")
    except Exception as e:
        logger.warning(f"Failed to send Slack alert: {e}")


def send_sns_notification(
    subject: str,
    message: str,
    topic_arn: str = None
):
    """
    Send SNS notification.
    
    Args:
        subject: Email subject
        message: Email body
        topic_arn: SNS topic ARN
    """
    import boto3
    
    try:
        sns = boto3.client("sns", region_name=os.getenv("AWS_REGION", "us-east-1"))
        topic_arn = topic_arn or os.getenv("SNS_TOPIC_ARN")
        
        if not topic_arn:
            logger.debug("SNS topic ARN not configured, skipping notification")
            return
        
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message
        )
        logger.debug(f"SNS notification sent: {subject}")
    except Exception as e:
        logger.warning(f"Failed to send SNS notification: {e}")


def on_etl_success(context):
    """
    Callback for successful ETL task.
    
    Usage in DAG:
        task = PythonOperator(
            task_id='run_etl',
            python_callable=run_pipeline,
            on_success_callback=on_etl_success
        )
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    
    message = f"✅ ETL Task {dag_id}.{task_id} completed successfully"
    
    send_dd_event(
        title="ETL Success",
        text=message,
        alert_type="success",
        tags=[f"dag:{dag_id}", f"task:{task_id}", "status:success"]
    )
    
    logger.info(message)


def on_etl_failure(context):
    """
    Callback for failed ETL task.
    
    Usage in DAG:
        task = PythonOperator(
            task_id='run_etl',
            python_callable=run_pipeline,
            on_failure_callback=on_etl_failure
        )
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    exception = context.get('exception')
    
    error_msg = f"❌ ETL Task {dag_id}.{task_id} failed: {str(exception)}"
    
    # Send to multiple channels
    send_dd_event(
        title="ETL Failure",
        text=error_msg,
        alert_type="error",
        tags=[f"dag:{dag_id}", f"task:{task_id}", "status:failed", "alert:true"]
    )
    
    send_slack_alert(
        message=error_msg,
        channel="#etl-alerts"
    )
    
    send_sns_notification(
        subject=f"ETL Failure: {dag_id}.{task_id}",
        message=error_msg
    )
    
    logger.error(error_msg)
