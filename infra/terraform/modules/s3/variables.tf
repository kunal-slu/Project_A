# Variables for S3 module

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "data_lake_bucket" {
  description = "Data lake bucket name"
  type        = string
}

variable "artifacts_bucket" {
  description = "Artifacts bucket name"
  type        = string
}

variable "logs_bucket" {
  description = "Logs bucket name"
  type        = string
}

variable "s3_kms_key_id" {
  description = "KMS key ID for S3 encryption"
  type        = string
}

variable "logs_kms_key_id" {
  description = "KMS key ID for logs encryption"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
