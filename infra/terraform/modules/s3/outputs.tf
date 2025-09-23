# Outputs for S3 module

output "data_lake_bucket" {
  description = "Data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_bucket_arn" {
  description = "Data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "artifacts_bucket" {
  description = "Artifacts bucket name"
  value       = aws_s3_bucket.artifacts.bucket
}

output "artifacts_bucket_arn" {
  description = "Artifacts bucket ARN"
  value       = aws_s3_bucket.artifacts.arn
}

output "logs_bucket" {
  description = "Logs bucket name"
  value       = aws_s3_bucket.logs.bucket
}

output "logs_bucket_arn" {
  description = "Logs bucket ARN"
  value       = aws_s3_bucket.logs.arn
}
