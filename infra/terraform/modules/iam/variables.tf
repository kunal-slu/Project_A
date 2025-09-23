# IAM Module Variables

variable "project" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g., dev, test, prod)"
  type        = string
}

variable "data_lake_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "ARN of the S3 artifacts bucket"
  type        = string
}

variable "logs_bucket_arn" {
  description = "ARN of the S3 logs bucket"
  type        = string
}

variable "logs_bucket_id" {
  description = "ID of the S3 logs bucket"
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for encryption"
  type        = string
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS queue"
  type        = string
}