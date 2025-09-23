# Main Terraform configuration for PySpark Data Engineering Platform
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure AWS Provider
provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Local values
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
  
  # S3 bucket names
  data_lake_bucket = "${var.project_name}-${var.environment}-data-lake"
  artifacts_bucket = "${var.project_name}-${var.environment}-artifacts"
  logs_bucket      = "${var.project_name}-${var.environment}-logs"
  
  # Glue database names
  bronze_db = "${var.project_name}_${var.environment}_bronze"
  silver_db = "${var.project_name}_${var.environment}_silver"
  gold_db   = "${var.project_name}_${var.environment}_gold"
}

# KMS Keys
module "kms" {
  source = "./modules/kms"
  
  project_name = var.project_name
  environment  = var.environment
  tags         = local.common_tags
}

# S3 Data Lake
module "s3" {
  source = "./modules/s3"
  
  project_name = var.project_name
  environment  = var.environment
  
  data_lake_bucket = local.data_lake_bucket
  artifacts_bucket = local.artifacts_bucket
  logs_bucket      = local.logs_bucket
  
  s3_kms_key_id = module.kms.s3_key_id
  logs_kms_key_id = module.kms.logs_key_id
  
  tags = local.common_tags
}

# IAM Roles and Policies
module "iam" {
  source = "./modules/iam"
  
  project_name = var.project_name
  environment  = var.environment
  
  data_lake_bucket = local.data_lake_bucket
  artifacts_bucket = local.artifacts_bucket
  logs_bucket      = local.logs_bucket
  
  bronze_db = local.bronze_db
  silver_db = local.silver_db
  gold_db   = local.gold_db
  
  tags = local.common_tags
}

# Glue Data Catalog
module "glue" {
  source = "./modules/glue"
  
  project_name = var.project_name
  environment  = var.environment
  
  bronze_db = local.bronze_db
  silver_db = local.silver_db
  gold_db   = local.gold_db
  
  tags = local.common_tags
}

# EMR Serverless
module "emrserverless" {
  source = "./modules/emrserverless"
  
  project_name = var.project_name
  environment  = var.environment
  
  execution_role_arn = module.iam.emr_execution_role_arn
  job_role_arn       = module.iam.emr_job_role_arn
  
  artifacts_bucket = local.artifacts_bucket
  logs_bucket      = local.logs_bucket
  
  tags = local.common_tags
}

# SQS Queues
module "sqs" {
  source = "./modules/sqs"
  
  project_name = var.project_name
  environment  = var.environment
  
  tags = local.common_tags
}

# Lambda Functions
module "lambda" {
  source = "./modules/lambda"
  
  project_name = var.project_name
  environment  = var.environment
  
  execution_role_arn = module.iam.lambda_execution_role_arn
  
  rest_queue_url = module.sqs.rest_queue_url
  rest_dlq_url   = module.sqs.rest_dlq_url
  
  data_lake_bucket = local.data_lake_bucket
  artifacts_bucket = local.artifacts_bucket
  
  tags = local.common_tags
}

# RDS (optional, for CDC demo)
module "rds" {
  count  = var.enable_rds ? 1 : 0
  source = "./modules/rds"
  
  project_name = var.project_name
  environment  = var.environment
  
  vpc_id             = var.vpc_id
  private_subnet_ids = var.private_subnet_ids
  
  tags = local.common_tags
}

# DMS (optional, for CDC)
module "dms" {
  count  = var.enable_dms ? 1 : 0
  source = "./modules/dms"
  
  project_name = var.project_name
  environment  = var.environment
  
  replication_role_arn = module.iam.dms_role_arn
  
  rds_endpoint = var.enable_rds ? module.rds[0].endpoint : var.rds_endpoint
  rds_port     = var.enable_rds ? module.rds[0].port : var.rds_port
  
  target_bucket = local.data_lake_bucket
  
  tags = local.common_tags
}

# AppFlow (optional, for Salesforce)
module "appflow" {
  count  = var.enable_appflow ? 1 : 0
  source = "./modules/appflow"
  
  project_name = var.project_name
  environment  = var.environment
  
  appflow_role_arn = module.iam.appflow_role_arn
  
  target_bucket = local.data_lake_bucket
  
  tags = local.common_tags
}

# MWAA (Airflow)
module "mwaa" {
  source = "./modules/mwaa"
  
  project_name = var.project_name
  environment  = var.environment
  
  execution_role_arn = module.iam.mwaa_execution_role_arn
  
  artifacts_bucket = local.artifacts_bucket
  logs_bucket      = local.logs_bucket
  
  vpc_id             = var.vpc_id
  private_subnet_ids = var.private_subnet_ids
  
  tags = local.common_tags
}

# Athena
module "athena" {
  source = "./modules/athena"
  
  project_name = var.project_name
  environment  = var.environment
  
  workgroup_name = "${var.project_name}-${var.environment}-workgroup"
  output_location = "s3://${local.logs_bucket}/athena-results/"
  
  tags = local.common_tags
}

# Budget and Monitoring
module "budgets" {
  source = "./modules/budgets"
  
  project_name = var.project_name
  environment  = var.environment
  
  budget_limit = var.budget_limit
  budget_email = var.budget_email
  
  tags = local.common_tags
}