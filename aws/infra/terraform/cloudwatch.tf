# CloudWatch Logging and Monitoring

# Log Group for EMR Serverless
resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/spark/${var.project}-${var.environment}"
  retention_in_days = var.log_retention_days
  
  tags = merge(
    var.tags,
    {
      Name = "${var.project}-${var.environment}-emr-logs"
    }
  )
}

# Log Group for Application Logs
resource "aws_cloudwatch_log_group" "application" {
  name              = "/aws/data-platform/${var.project}-${var.environment}"
  retention_in_days = var.log_retention_days
  
  tags = merge(
    var.tags,
    {
      Name = "${var.project}-${var.environment}-app-logs"
    }
  )
}

# CloudWatch Alarm for EMR Job Failures
resource "aws_cloudwatch_metric_alarm" "emr_job_failures" {
  alarm_name          = "${var.project}-${var.environment}-emr-job-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "JobRunFailed"
  namespace           = "AWS/EMRServerless"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "This metric monitors EMR Serverless job failures"
  
  dimensions = {
    ApplicationId = aws_emrserverless_application.main.id
  }
  
  tags = var.tags
}

# SNS Topic for Alerts
resource "aws_sns_topic" "data_platform_alerts" {
  name = "${var.project}-${var.environment}-data-platform-alerts"
  
  tags = var.tags
}

# Subscribe to alerts
resource "aws_sns_topic_subscription" "email" {
  count = length(var.alert_emails)
  
  topic_arn = aws_sns_topic.data_platform_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_emails[count.index]
}

