# EMR Serverless Module Outputs

output "application_id" {
  description = "ID of the EMR Serverless application"
  value       = aws_emrserverless_application.this.id
}

output "application_arn" {
  description = "ARN of the EMR Serverless application"
  value       = aws_emrserverless_application.this.arn
}

output "application_name" {
  description = "Name of the EMR Serverless application"
  value       = aws_emrserverless_application.this.name
}