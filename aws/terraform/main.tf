# AWS EMR Serverless + S3 Data Lake Infrastructure

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Local values
locals {
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# S3 Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project}-${var.environment}-data-lake"

  tags = merge(local.common_tags, {
    Name = "Data Lake Bucket"
  })
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "transition_to_ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# S3 Code Artifacts Bucket
resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.project}-${var.environment}-artifacts"

  tags = merge(local.common_tags, {
    Name = "Code Artifacts Bucket"
  })
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# KMS Key for S3 encryption
resource "aws_kms_key" "s3" {
  description             = "KMS key for S3 encryption"
  deletion_window_in_days = 7

  tags = merge(local.common_tags, {
    Name = "S3 Encryption Key"
  })
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${var.project}-${var.environment}-s3"
  target_key_id = aws_kms_key.s3.key_id
}

# IAM Role for EMR Serverless
resource "aws_iam_role" "emr_serverless_job_role" {
  name = "${var.project}-${var.environment}-emr-serverless-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "emr_serverless_job_policy" {
  name = "${var.project}-${var.environment}-emr-serverless-job-policy"
  role = aws_iam_role.emr_serverless_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
          aws_s3_bucket.artifacts.arn,
          "${aws_s3_bucket.artifacts.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless/*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.project}-${var.environment}-*"
      }
    ]
  })
}

# EMR Serverless Application
resource "aws_emrserverless_application" "spark_app" {
  name         = "${var.project}-${var.environment}-spark-app"
  release_label = "emr-6.15.0"
  type         = "spark"

  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"
    initial_capacity_config {
      worker_count = 2
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "8 GB"
      }
    }
  }

  maximum_capacity {
    max_cpu    = "20 vCPU"
    max_memory = "40 GB"
  }

  tags = local.common_tags
}

# Glue Databases
resource "aws_glue_catalog_database" "silver" {
  name = "${var.project}_silver"

  tags = merge(local.common_tags, {
    Name = "Silver Database"
  })
}

resource "aws_glue_catalog_database" "gold" {
  name = "${var.project}_gold"

  tags = merge(local.common_tags, {
    Name = "Gold Database"
  })
}

# Secrets Manager entries (placeholders)
resource "aws_secretsmanager_secret" "salesforce_credentials" {
  name                    = "${var.project}-${var.environment}-salesforce-credentials"
  description             = "Salesforce API credentials"
  recovery_window_in_days = 7

  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "salesforce_credentials" {
  secret_id = aws_secretsmanager_secret.salesforce_credentials.id
  secret_string = jsonencode({
    username       = "your-salesforce-username"
    password       = "your-salesforce-password"
    security_token = "your-salesforce-token"
    domain         = "login"
  })
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/${aws_emrserverless_application.spark_app.id}"
  retention_in_days = 14

  tags = local.common_tags
}
