# Outputs for KMS module

output "s3_key_id" {
  description = "S3 KMS key ID"
  value       = aws_kms_key.s3_key.key_id
}

output "s3_key_arn" {
  description = "S3 KMS key ARN"
  value       = aws_kms_key.s3_key.arn
}

output "logs_key_id" {
  description = "Logs KMS key ID"
  value       = aws_kms_key.logs_key.key_id
}

output "logs_key_arn" {
  description = "Logs KMS key ARN"
  value       = aws_kms_key.logs_key.arn
}

output "emr_key_id" {
  description = "EMR KMS key ID"
  value       = aws_kms_key.emr_key.key_id
}

output "emr_key_arn" {
  description = "EMR KMS key ARN"
  value       = aws_kms_key.emr_key.arn
}

output "secrets_key_id" {
  description = "Secrets Manager KMS key ID"
  value       = aws_kms_key.secrets_key.key_id
}

output "secrets_key_arn" {
  description = "Secrets Manager KMS key ARN"
  value       = aws_kms_key.secrets_key.arn
}
