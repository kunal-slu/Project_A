# Lambda Module

# Create deployment package
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.source_path
  output_path = "/tmp/${var.function_name}.zip"
}

# Lambda function
resource "aws_lambda_function" "this" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = var.function_name
  role            = var.iam_role_arn
  handler         = var.handler
  runtime         = var.runtime
  memory_size     = var.memory_size
  timeout         = var.timeout

  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = var.environment_variables
  }

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "LambdaFunction"
  }
}

# Event source mapping for SQS (if provided)
resource "aws_lambda_event_source_mapping" "sqs" {
  count            = var.event_source_arn != null ? 1 : 0
  event_source_arn = var.event_source_arn
  function_name    = aws_lambda_function.this.arn
  batch_size       = 10
  maximum_batching_window_in_seconds = 5
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.this.function_name}"
  retention_in_days = 14

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "LambdaLogs"
  }
}