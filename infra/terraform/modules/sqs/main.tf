# SQS Module

resource "aws_sqs_queue" "this" {
  name                      = "${var.project}-${var.environment}-rest-api-queue"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 0
  visibility_timeout_seconds = 30

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "RESTAPIIngestion"
  }
}

# Dead Letter Queue
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project}-${var.environment}-rest-api-dlq"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 0
  visibility_timeout_seconds = 30

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "RESTAPIDLQ"
  }
}

# Redrive policy for main queue
resource "aws_sqs_queue_redrive_policy" "this" {
  queue_url = aws_sqs_queue.this.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}