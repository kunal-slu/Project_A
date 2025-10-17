terraform {
  backend "s3" {
    bucket         = "projecta-tfstate"
    key            = "aws/infra/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "projecta-tf-locks"
    encrypt        = true
  }
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "project-a"
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# S3 Buckets
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-lake-${var.environment}"

  tags = {
    Name        = "${var.project_name}-data-lake-${var.environment}"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket" "tfstate" {
  bucket = "${var.project_name}-tfstate-${var.environment}"

  tags = {
    Name        = "${var.project_name}-tfstate-${var.environment}"
    Environment = var.environment
    Project     = var.project_name
  }
}

# S3 Bucket configurations
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# DynamoDB for Terraform locks
resource "aws_dynamodb_table" "tf_locks" {
  name           = "${var.project_name}-tf-locks"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name        = "${var.project_name}-tf-locks"
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Roles
resource "aws_iam_role" "emr_serverless_role" {
  name = "${var.project_name}-emr-serverless-role"

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

  tags = {
    Name        = "${var.project_name}-emr-serverless-role"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_iam_role_policy_attachment" "emr_serverless_s3" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Outputs
output "data_lake_bucket" {
  description = "S3 bucket for data lake"
  value       = aws_s3_bucket.data_lake.bucket
}

output "tfstate_bucket" {
  description = "S3 bucket for Terraform state"
  value       = aws_s3_bucket.tfstate.bucket
}

output "emr_serverless_role_arn" {
  description = "EMR Serverless role ARN"
  value       = aws_iam_role.emr_serverless_role.arn
}

output "region" {
  description = "AWS region"
  value       = data.aws_region.current.name
}

output "account_id" {
  description = "AWS account ID"
  value       = data.aws_caller_identity.current.account_id
}
