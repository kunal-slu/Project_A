# MWAA Module Outputs

output "environment_name" {
  description = "Name of the MWAA environment"
  value       = aws_mwaa_environment.this.name
}

output "environment_arn" {
  description = "ARN of the MWAA environment"
  value       = aws_mwaa_environment.this.arn
}

output "webserver_url" {
  description = "URL of the MWAA webserver"
  value       = "https://${aws_mwaa_environment.this.webserver_url}"
}

output "mwaa_bucket_id" {
  description = "ID of the S3 bucket for MWAA"
  value       = aws_s3_bucket.mwaa_bucket.id
}

output "mwaa_bucket_arn" {
  description = "ARN of the S3 bucket for MWAA"
  value       = aws_s3_bucket.mwaa_bucket.arn
}