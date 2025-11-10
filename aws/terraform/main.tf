##############################################################
# Provider & Terraform
##############################################################
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project = var.project_name
      Env     = var.environment
      Owner   = "kunal"
    }
  }
}

# Data sources
data "aws_region" "current" {}
# Note: aws_caller_identity is defined in lake_formation.tf

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  kms_alias   = "alias/${local.name_prefix}-cmk"
}

##############################################################
# KMS (CMK + alias)
##############################################################
resource "aws_kms_key" "cmk" {
  description             = "CMK for ${local.name_prefix}"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "cmk_alias" {
  name          = local.kms_alias
  target_key_id = aws_kms_key.cmk.key_id
}

##############################################################
# S3 buckets (data lake, artifacts) + hardening
##############################################################
resource "aws_s3_bucket" "data_lake" {
  bucket = "my-etl-lake-demo-424570854632"
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "my-etl-artifacts-demo-424570854632"
}

# Versioning
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

# SSE-KMS encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.cmk.arn
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.cmk.arn
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: IA @30d, Glacier @90d (current & noncurrent)
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "ia-30-glacier-90"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "ia-30-glacier-90"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }
  }
}

##############################################################
# IAM roles (EMR exec, Glue)
##############################################################
data "aws_iam_policy_document" "emr_trust" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_exec" {
  name               = "${local.name_prefix}-emr-exec"
  assume_role_policy = data.aws_iam_policy_document.emr_trust.json
}

resource "aws_iam_role_policy" "emr_exec" {
  name = "${local.name_prefix}-emr-exec-policy"
  role = aws_iam_role.emr_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakeRW"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
          aws_s3_bucket.artifacts.arn,
          "${aws_s3_bucket.artifacts.arn}/*"
        ]
      },
      {
        Sid      = "KMSAccess"
        Effect   = "Allow"
        Action   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = aws_kms_key.cmk.arn
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:CreateLogGroup"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:log-group:/aws/emr-serverless/*"
      },
      {
        Sid    = "GlueAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartition",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:catalog",
          "arn:aws:glue:${var.aws_region}:*:database/*",
          "arn:aws:glue:${var.aws_region}:*:table/*"
        ]
      },
      {
        Sid    = "SecretsManagerAccess"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:${local.name_prefix}-*"
      }
    ]
  })
}

data "aws_iam_policy_document" "glue_trust" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

##############################################################
# EMR Serverless application (Delta defaults)
##############################################################
resource "aws_emrserverless_application" "spark" {
  name          = "${local.name_prefix}-spark"
  release_label = "emr-7.1.0"
  type          = "SPARK"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  # Note: Spark configurations (Delta Lake, KMS) should be set at job runtime
  # via spark-submit or job configuration parameters
}

##############################################################
# (Intentionally omitted here to avoid duplicates)
# - Glue DBs -> glue_catalog.tf
# - Secrets  -> secrets.tf
# - CW Logs & SNS -> cloudwatch.tf
##############################################################