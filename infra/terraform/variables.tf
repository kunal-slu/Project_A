# Terraform variables for PySpark Data Engineering Platform

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "pyspark-interview"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

# VPC Configuration
variable "vpc_id" {
  description = "VPC ID for MWAA and RDS"
  type        = string
  default     = null
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for MWAA and RDS"
  type        = list(string)
  default     = []
}

# Feature Toggles
variable "enable_rds" {
  description = "Enable RDS for CDC demonstration"
  type        = bool
  default     = true
}

variable "enable_dms" {
  description = "Enable DMS for CDC"
  type        = bool
  default     = true
}

variable "enable_appflow" {
  description = "Enable AppFlow for Salesforce integration"
  type        = bool
  default     = true
}

variable "enable_lake_formation" {
  description = "Enable Lake Formation for fine-grained access control"
  type        = bool
  default     = false
}

# RDS Configuration (if not using module)
variable "rds_endpoint" {
  description = "RDS endpoint (if not using module)"
  type        = string
  default     = null
}

variable "rds_port" {
  description = "RDS port"
  type        = number
  default     = 5432
}

# Budget Configuration
variable "budget_limit" {
  description = "Monthly budget limit in USD"
  type        = number
  default     = 50
}

variable "budget_email" {
  description = "Email for budget alerts"
  type        = string
  default     = "admin@example.com"
}

# EMR Configuration
variable "emr_spark_version" {
  description = "EMR Serverless Spark version"
  type        = string
  default     = "3.5.1"
}

variable "emr_delta_version" {
  description = "Delta Lake version"
  type        = string
  default     = "3.2.0"
}

# Lambda Configuration
variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory_size" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 512
}

# MWAA Configuration
variable "mwaa_environment_class" {
  description = "MWAA environment class"
  type        = string
  default     = "mw1.small"
}

variable "mwaa_max_workers" {
  description = "Maximum number of MWAA workers"
  type        = number
  default     = 1
}

variable "mwaa_min_workers" {
  description = "Minimum number of MWAA workers"
  type        = number
  default     = 1
}

# Tags
variable "additional_tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}