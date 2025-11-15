# CloudWatch Logging and Monitoring

variable "enable_cloudwatch_dashboards" {
  description = "Enable CloudWatch dashboards"
  type        = bool
  default     = true
}

variable "enable_emr_alarms" {
  description = "Enable EMR job alarms"
  type        = bool
  default     = true
}

variable "alarm_email" {
  description = "Email address for CloudWatch alarms"
  type        = string
  default     = ""
}

# Log Group for EMR Serverless
resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/spark/${var.project_name}-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = merge(
    var.tags,
    {
      Name = "${var.project_name}-${var.environment}-emr-logs"
    }
  )
}

# Log Group for Application Logs
resource "aws_cloudwatch_log_group" "application" {
  name              = "/aws/data-platform/${var.project_name}-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = merge(
    var.tags,
    {
      Name = "${var.project_name}-${var.environment}-app-logs"
    }
  )
}

# SNS Topic for Alerts
resource "aws_sns_topic" "project_a_alerts" {
  name = "${var.project_name}-${var.environment}-alerts"

  tags = merge(
    var.tags,
    {
      Name = "${var.project_name}-${var.environment}-alerts"
    }
  )
}

# Email subscription (if alarm_email is provided)
resource "aws_sns_topic_subscription" "email" {
  count = var.alarm_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.project_a_alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# Legacy subscription for alert_emails list (backward compatibility)
resource "aws_sns_topic_subscription" "email_list" {
  count = length(var.alert_emails)

  topic_arn = aws_sns_topic.project_a_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_emails[count.index]
}

# CloudWatch Alarm for EMR Job Failures (AWS native metric)
resource "aws_cloudwatch_metric_alarm" "emr_job_failures" {
  count = var.enable_emr_alarms ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-emr-job-failures"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "JobRunFailed"
  namespace           = "AWS/EMRServerless"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alerts when EMR Serverless jobs fail"
  alarm_actions       = [aws_sns_topic.project_a_alerts.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    ApplicationId = aws_emrserverless_application.spark.id
  }

  tags = var.tags
}

# Custom metric alarm for EMR job failures (from application metrics)
resource "aws_cloudwatch_metric_alarm" "emr_job_failures_custom" {
  count = var.enable_emr_alarms ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-emr-job-failures-custom"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "EMRJobFailures"
  namespace           = "ProjectA/EMR"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alerts when custom EMR job failure metrics are emitted"
  alarm_actions       = [aws_sns_topic.project_a_alerts.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    Environment = var.environment
  }

  tags = var.tags
}

# Alarm for EMR job duration (jobs running too long)
resource "aws_cloudwatch_metric_alarm" "emr_job_duration" {
  count = var.enable_emr_alarms ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-emr-job-duration"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "EMRJobDurationSeconds"
  namespace           = "ProjectA/EMR"
  period              = 300
  statistic           = "Average"
  threshold           = 3600 # 1 hour
  alarm_description   = "Alerts when EMR jobs take longer than 1 hour"
  alarm_actions       = [aws_sns_topic.project_a_alerts.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    Environment = var.environment
  }

  tags = var.tags
}

# CloudWatch Dashboard for EMR Monitoring
resource "aws_cloudwatch_dashboard" "emr_monitoring" {
  count = var.enable_cloudwatch_dashboards ? 1 : 0

  dashboard_name = "${var.project_name}-${var.environment}-emr-monitoring"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/EMRServerless", "JobRunFailed", { "stat" = "Sum", "label" = "Failed Jobs" }],
            ["AWS/EMRServerless", "JobRunSucceeded", { "stat" = "Sum", "label" = "Succeeded Jobs" }],
            [".", "JobRunRunning", { "stat" = "Sum", "label" = "Running Jobs" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "EMR Serverless Job Status"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["ProjectA/EMR", "EMRJobSuccess", { "stat" = "Sum", "label" = "Success" }],
            ["ProjectA/EMR", "EMRJobFailures", { "stat" = "Sum", "label" = "Failures" }],
            ["ProjectA/EMR", "EMRJobDurationSeconds", { "stat" = "Average", "label" = "Avg Duration (s)" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Custom EMR Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 24
        height = 6

        properties = {
          metrics = [
            ["ProjectA/EMR", "EMRJobDurationSeconds", { "stat" = "Average", "dimensions" = { "JobName" = "bronze_to_silver" } }],
            ["ProjectA/EMR", "EMRJobDurationSeconds", { "stat" = "Average", "dimensions" = { "JobName" = "silver_to_gold" } }],
            ["ProjectA/EMR", "EMRJobDurationSeconds", { "stat" = "Average", "dimensions" = { "JobName" = "fx_json_to_bronze" } }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Job Duration by Job Name"
          period  = 300
        }
      }
    ]
  })

  tags = var.tags
}

