# MWAA Module Variables

variable "project" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g., dev, test, prod)"
  type        = string
}

variable "mwaa_execution_role_arn" {
  description = "ARN of the MWAA execution role"
  type        = string
}

variable "mwaa_s3_bucket_id" {
  description = "ID of the S3 bucket for MWAA"
  type        = string
}

variable "mwaa_s3_bucket_arn" {
  description = "ARN of the S3 bucket for MWAA"
  type        = string
}

variable "webserver_access_mode" {
  description = "Webserver access mode for MWAA"
  type        = string
  default     = "PUBLIC_ONLY"
  validation {
    condition     = contains(["PUBLIC_ONLY", "PRIVATE_ONLY"], var.webserver_access_mode)
    error_message = "Webserver access mode must be either PUBLIC_ONLY or PRIVATE_ONLY."
  }
}

variable "subnet_ids" {
  description = "List of subnet IDs for MWAA"
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "List of security group IDs for MWAA"
  type        = list(string)
  default     = []
}