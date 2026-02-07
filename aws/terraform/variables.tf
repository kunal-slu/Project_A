# AWS Infrastructure Variables

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "region" {
  description = "AWS region (alias for aws_region)"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name (deprecated, use project_name)"
  type        = string
  default     = "pyspark-etl-project"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "pyspark-etl-project"
}

variable "aws_profile" {
  description = "AWS profile to use"
  type        = string
  default     = null
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "VPC ID for EMR Serverless (optional)"
  type        = string
  default     = null
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "create_vpc" {
  description = "Create VPC or use existing"
  type        = bool
  default     = true
}

variable "create_redshift_security_group" {
  description = "Create Redshift security group"
  type        = bool
  default     = false
}

variable "subnet_ids" {
  description = "Subnet IDs for EMR Serverless (optional)"
  type        = list(string)
  default     = []
}

variable "log_retention_days" {
  description = "CloudWatch log retention days"
  type        = number
  default     = 30
}

variable "alert_emails" {
  description = "List of email addresses for alerts"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}

variable "alarm_email" {
  description = "Email address for CloudWatch alarms"
  type        = string
  default     = ""
}

variable "enable_external_access" {
  description = "Enable external analytics access"
  type        = bool
  default     = false
}

variable "external_analytics_role_arn" {
  description = "ARN of external analytics role (if enable_external_access is true)"
  type        = string
  default     = ""
}
