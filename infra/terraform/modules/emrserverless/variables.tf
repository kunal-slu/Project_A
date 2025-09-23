# EMR Serverless Module Variables

variable "project" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g., dev, test, prod)"
  type        = string
}

variable "release_label" {
  description = "EMR release label"
  type        = string
  default     = "emr-6.15.0"
}

variable "emr_job_role_arn" {
  description = "ARN of the IAM role for EMR job execution"
  type        = string
}

variable "s3_artifacts_bucket_id" {
  description = "ID of the S3 artifacts bucket"
  type        = string
}

variable "s3_logs_bucket_id" {
  description = "ID of the S3 logs bucket"
  type        = string
}

variable "log_uri" {
  description = "S3 URI for EMR Serverless logs"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for EMR Serverless"
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "List of security group IDs for EMR Serverless"
  type        = list(string)
  default     = []
}