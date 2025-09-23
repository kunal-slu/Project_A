# EMR Serverless Module

resource "aws_emrserverless_application" "this" {
  name         = "${var.project}-${var.environment}-emr-serverless"
  release_label = var.release_label
  type         = "spark"

  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "8 GB"
      }
    }
  }

  maximum_capacity {
    cpu    = "10 vCPU"
    memory = "20 GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled                         = true
    idle_timeout_minutes           = 15
    auto_stop_post_workload_stopped = true
  }

  network_configuration {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "DataProcessing"
  }
}

# CloudWatch Log Group for EMR Serverless
resource "aws_cloudwatch_log_group" "emr_serverless_logs" {
  name              = "/aws/emr-serverless/${aws_emrserverless_application.this.id}"
  retention_in_days = 30

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "EMRServerlessLogs"
  }
}