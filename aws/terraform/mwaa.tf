# MWAA (Managed Workflows for Apache Airflow) Module
# Optional but production-grade orchestration
# Note: data.aws_caller_identity.current is defined in lake_formation.tf

variable "enable_mwaa" {
  description = "Enable MWAA environment"
  type        = bool
  default     = false
}

variable "mwaa_environment_name" {
  description = "Name of the MWAA environment"
  type        = string
  default     = "project-a-airflow"
}

variable "mwaa_dag_s3_path" {
  description = "S3 path for DAGs (relative to MWAA bucket)"
  type        = string
  default     = "dags"
}

variable "mwaa_plugins_s3_path" {
  description = "S3 path for plugins (relative to MWAA bucket)"
  type        = string
  default     = "plugins"
}

variable "mwaa_requirements_s3_path" {
  description = "S3 path for requirements.txt (relative to MWAA bucket)"
  type        = string
  default     = "requirements.txt"
}

# MWAA S3 Bucket
resource "aws_s3_bucket" "mwaa" {
  count  = var.enable_mwaa ? 1 : 0
  bucket = "${var.project_name}-mwaa-${var.environment}-${data.aws_caller_identity.current.account_id}"

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}-mwaa-${var.environment}"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_s3_bucket_versioning" "mwaa" {
  count  = var.enable_mwaa ? 1 : 0
  bucket = aws_s3_bucket.mwaa[0].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa" {
  count  = var.enable_mwaa ? 1 : 0
  bucket = aws_s3_bucket.mwaa[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# MWAA IAM Role
resource "aws_iam_role" "mwaa" {
  count = var.enable_mwaa ? 1 : 0
  name  = "${var.project_name}-mwaa-${var.environment}-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "airflow.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
      {
        Effect = "Allow"
        Principal = {
          Service = "airflow-env.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name = "${var.project_name}-mwaa-${var.environment}-execution-role"
    }
  )
}

# MWAA Execution Policy
resource "aws_iam_role_policy" "mwaa_execution" {
  count = var.enable_mwaa ? 1 : 0
  name  = "${var.project_name}-mwaa-${var.environment}-execution-policy"
  role  = aws_iam_role.mwaa[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "airflow:PublishMetrics"
        ]
        Resource = "arn:aws:airflow:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:environment/${var.mwaa_environment_name}"
      },
      {
        Effect = "Deny"
        Action = [
          "s3:ListAllMyBuckets"
        ]
        Resource = [
          aws_s3_bucket.mwaa[0].arn,
          "${aws_s3_bucket.mwaa[0].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:PutObject*",
          "s3:DeleteObject*"
        ]
        Resource = "${aws_s3_bucket.mwaa[0].arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.mwaa[0].arn
        Condition = {
          StringEquals = {
            "s3:prefix" = [
              var.mwaa_dag_s3_path,
              var.mwaa_plugins_s3_path,
              var.mwaa_requirements_s3_path
            ]
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:DescribeLogGroups"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${var.mwaa_environment_name}-*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:*"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "Airflow"
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject*"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/*",
          "${aws_s3_bucket.artifacts.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          aws_s3_bucket.artifacts.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun",
          "emr-serverless:ListJobRuns"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = "*"
      }
    ]
  })
}

# CloudWatch Log Groups for MWAA
resource "aws_cloudwatch_log_group" "mwaa" {
  count             = var.enable_mwaa ? 1 : 0
  name              = "airflow-${var.mwaa_environment_name}"
  retention_in_days = 7

  tags = merge(
    var.tags,
    {
      Name = "airflow-${var.mwaa_environment_name}"
    }
  )
}

resource "aws_cloudwatch_log_group" "mwaa_scheduler" {
  count             = var.enable_mwaa ? 1 : 0
  name              = "airflow-${var.mwaa_environment_name}-Scheduler"
  retention_in_days = 7

  tags = merge(
    var.common_tags,
    {
      Name = "airflow-${var.mwaa_environment_name}-Scheduler"
    }
  )
}

resource "aws_cloudwatch_log_group" "mwaa_worker" {
  count             = var.enable_mwaa ? 1 : 0
  name              = "airflow-${var.mwaa_environment_name}-Worker"
  retention_in_days = 7

  tags = merge(
    var.common_tags,
    {
      Name = "airflow-${var.mwaa_environment_name}-Worker"
    }
  )
}

resource "aws_cloudwatch_log_group" "mwaa_webserver" {
  count             = var.enable_mwaa ? 1 : 0
  name              = "airflow-${var.mwaa_environment_name}-Webserver"
  retention_in_days = 7

  tags = merge(
    var.common_tags,
    {
      Name = "airflow-${var.mwaa_environment_name}-Webserver"
    }
  )
}

# MWAA Environment
resource "aws_mwaa_environment" "main" {
  count             = var.enable_mwaa ? 1 : 0
  name              = var.mwaa_environment_name
  airflow_version   = "2.8.2"
  environment_class = "mw1.small" # Start small, scale up as needed

  execution_role_arn = aws_iam_role.mwaa[0].arn

  source_bucket_arn    = aws_s3_bucket.mwaa[0].arn
  dag_s3_path          = var.mwaa_dag_s3_path
  plugins_s3_path      = var.mwaa_plugins_s3_path
  requirements_s3_path = var.mwaa_requirements_s3_path

  network_configuration {
    security_group_ids = [aws_security_group.mwaa[0].id]
    subnet_ids         = length(var.subnet_ids) > 0 ? var.subnet_ids : [] # MWAA needs private subnets
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  # Airflow configuration options
  airflow_configuration_options = {
    "core.default_timezone"           = "utc"
    "webserver.dag_default_view"      = "tree"
    "webserver.dag_orientation"       = "LR"
    "scheduler.dag_dir_list_interval" = "300"
    "api.auth_backend"                = "airflow.api.auth.backend.basic_auth"
  }

  tags = merge(
    var.tags,
    {
      Name = var.mwaa_environment_name
    }
  )

  depends_on = [
    aws_cloudwatch_log_group.mwaa,
    aws_cloudwatch_log_group.mwaa_scheduler,
    aws_cloudwatch_log_group.mwaa_worker,
    aws_cloudwatch_log_group.mwaa_webserver,
  ]
}

# Security Group for MWAA
resource "aws_security_group" "mwaa" {
  count       = var.enable_mwaa ? 1 : 0
  name        = "${var.project_name}-mwaa-${var.environment}-sg"
  description = "Security group for MWAA"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    var.tags,
    {
      Name = "${var.project_name}-mwaa-${var.environment}-sg"
    }
  )
}

# Outputs
output "mwaa_environment_name" {
  description = "MWAA environment name"
  value       = var.enable_mwaa ? aws_mwaa_environment.main[0].name : null
}

output "mwaa_webserver_url" {
  description = "MWAA webserver URL"
  value       = var.enable_mwaa ? aws_mwaa_environment.main[0].webserver_url : null
}

output "mwaa_s3_bucket" {
  description = "MWAA S3 bucket for DAGs"
  value       = var.enable_mwaa ? aws_s3_bucket.mwaa[0].id : null
}

