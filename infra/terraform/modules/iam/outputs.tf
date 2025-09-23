# IAM Module Outputs

output "emr_serverless_job_role_arn" {
  description = "ARN of the EMR Serverless job role"
  value       = aws_iam_role.emr_serverless_job_role.arn
}

output "mwaa_execution_role_arn" {
  description = "ARN of the MWAA execution role"
  value       = aws_iam_role.mwaa_execution_role.arn
}

output "lambda_rest_producer_role_arn" {
  description = "ARN of the Lambda REST producer role"
  value       = aws_iam_role.lambda_rest_producer_role.arn
}

output "lambda_rest_consumer_role_arn" {
  description = "ARN of the Lambda REST consumer role"
  value       = aws_iam_role.lambda_rest_consumer_role.arn
}