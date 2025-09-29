# AWS Infrastructure Outputs

output "data_lake_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "artifacts_bucket_name" {
  description = "Name of the S3 artifacts bucket"
  value       = aws_s3_bucket.artifacts.bucket
}

output "artifacts_bucket_arn" {
  description = "ARN of the S3 artifacts bucket"
  value       = aws_s3_bucket.artifacts.arn
}

output "emr_serverless_application_id" {
  description = "ID of the EMR Serverless application"
  value       = aws_emrserverless_application.spark_app.id
}

output "emr_serverless_application_arn" {
  description = "ARN of the EMR Serverless application"
  value       = aws_emrserverless_application.spark_app.arn
}

output "emr_serverless_job_role_arn" {
  description = "ARN of the IAM role for EMR Serverless jobs"
  value       = aws_iam_role.emr_serverless_job_role.arn
}

output "glue_database_silver" {
  description = "Name of the Glue silver database"
  value       = aws_glue_catalog_database.silver.name
}

output "glue_database_gold" {
  description = "Name of the Glue gold database"
  value       = aws_glue_catalog_database.gold.name
}

output "kms_key_id" {
  description = "ID of the KMS key for S3 encryption"
  value       = aws_kms_key.s3.key_id
}

output "kms_key_arn" {
  description = "ARN of the KMS key for S3 encryption"
  value       = aws_kms_key.s3.arn
}

output "secrets_manager_salesforce_arn" {
  description = "ARN of the Salesforce credentials secret"
  value       = aws_secretsmanager_secret.salesforce_credentials.arn
}

output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch log group for EMR Serverless"
  value       = aws_cloudwatch_log_group.emr_serverless.name
}
