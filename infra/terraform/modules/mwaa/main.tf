# MWAA Module

# S3 bucket for MWAA DAGs and plugins
resource "aws_s3_bucket" "mwaa_bucket" {
  bucket = "${var.project}-${var.environment}-mwaa-${random_string.bucket_suffix.result}"
  
  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "MWAADags"
  }
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

resource "aws_s3_bucket_versioning" "mwaa_bucket" {
  bucket = aws_s3_bucket.mwaa_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa_bucket" {
  bucket = aws_s3_bucket.mwaa_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "mwaa_bucket" {
  bucket = aws_s3_bucket.mwaa_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# MWAA Environment
resource "aws_mwaa_environment" "this" {
  name         = "${var.project}-${var.environment}-mwaa"
  airflow_version = "2.6.3"
  environment_class = "mw1.small"

  execution_role_arn = var.mwaa_execution_role_arn

  source_bucket_arn = aws_s3_bucket.mwaa_bucket.arn

  dag_s3_path = "dags"
  plugins_s3_path = "plugins"
  plugins_s3_object_version = "1"

  webserver_access_mode = var.webserver_access_mode

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  network_configuration {
    security_group_ids = var.security_group_ids
    subnet_ids         = var.subnet_ids
  }

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "MWAAEnvironment"
  }
}

# CloudWatch Log Groups for MWAA
resource "aws_cloudwatch_log_group" "mwaa_dag_processing" {
  name              = "/aws/mwaa/${aws_mwaa_environment.this.name}/DAGProcessing"
  retention_in_days = 14

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "MWAADagProcessing"
  }
}

resource "aws_cloudwatch_log_group" "mwaa_scheduler" {
  name              = "/aws/mwaa/${aws_mwaa_environment.this.name}/Scheduler"
  retention_in_days = 14

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "MWAAScheduler"
  }
}

resource "aws_cloudwatch_log_group" "mwaa_task" {
  name              = "/aws/mwaa/${aws_mwaa_environment.this.name}/Task"
  retention_in_days = 14

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "MWAATask"
  }
}

resource "aws_cloudwatch_log_group" "mwaa_webserver" {
  name              = "/aws/mwaa/${aws_mwaa_environment.this.name}/WebServer"
  retention_in_days = 14

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "MWAAWebServer"
  }
}

resource "aws_cloudwatch_log_group" "mwaa_worker" {
  name              = "/aws/mwaa/${aws_mwaa_environment.this.name}/Worker"
  retention_in_days = 14

  tags = {
    Project     = var.project
    Environment = var.environment
    Purpose     = "MWAAWorker"
  }
}