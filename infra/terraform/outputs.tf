# Terraform outputs for PySpark Data Engineering Platform

# S3 Buckets
output "data_lake_bucket" {
  description = "S3 data lake bucket name"
  value       = module.s3.data_lake_bucket
}

output "artifacts_bucket" {
  description = "S3 artifacts bucket name"
  value       = module.s3.artifacts_bucket
}

output "logs_bucket" {
  description = "S3 logs bucket name"
  value       = module.s3.logs_bucket
}

# Glue Databases
output "glue_databases" {
  description = "Glue database names"
  value = {
    bronze = module.glue.bronze_database
    silver = module.glue.silver_database
    gold   = module.glue.gold_database
  }
}

# EMR Serverless
output "emr_application_id" {
  description = "EMR Serverless application ID"
  value       = module.emrserverless.application_id
}

output "emr_application_arn" {
  description = "EMR Serverless application ARN"
  value       = module.emrserverless.application_arn
}

# SQS Queues
output "sqs_queues" {
  description = "SQS queue URLs"
  value = {
    rest_queue = module.sqs.rest_queue_url
    rest_dlq   = module.sqs.rest_dlq_url
  }
}

# Lambda Functions
output "lambda_functions" {
  description = "Lambda function names"
  value = {
    rest_producer = module.lambda.rest_producer_function_name
    rest_consumer = module.lambda.rest_consumer_function_name
    ge_checkpoint = module.lambda.ge_checkpoint_function_name
  }
}

# RDS (if enabled)
output "rds_endpoint" {
  description = "RDS endpoint"
  value       = var.enable_rds ? module.rds[0].endpoint : null
}

output "rds_port" {
  description = "RDS port"
  value       = var.enable_rds ? module.rds[0].port : null
}

# DMS (if enabled)
output "dms_replication_instance_arn" {
  description = "DMS replication instance ARN"
  value       = var.enable_dms ? module.dms[0].replication_instance_arn : null
}

# AppFlow (if enabled)
output "appflow_flow_arn" {
  description = "AppFlow flow ARN"
  value       = var.enable_appflow ? module.appflow[0].flow_arn : null
}

# MWAA
output "mwaa_environment_name" {
  description = "MWAA environment name"
  value       = module.mwaa.environment_name
}

output "mwaa_webserver_url" {
  description = "MWAA webserver URL"
  value       = module.mwaa.webserver_url
}

# Athena
output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = module.athena.workgroup_name
}

# KMS Keys
output "kms_keys" {
  description = "KMS key IDs"
  value = {
    s3_key_id   = module.kms.s3_key_id
    logs_key_id = module.kms.logs_key_id
  }
}

# IAM Roles
output "iam_roles" {
  description = "IAM role ARNs"
  value = {
    emr_execution_role = module.iam.emr_execution_role_arn
    emr_job_role       = module.iam.emr_job_role_arn
    mwaa_execution_role = module.iam.mwaa_execution_role_arn
    lambda_execution_role = module.iam.lambda_execution_role_arn
    dms_role           = module.iam.dms_role_arn
    appflow_role       = module.iam.appflow_role_arn
  }
}

# Configuration for runtime
output "runtime_config" {
  description = "Runtime configuration values"
  value = {
    project_name     = var.project_name
    environment      = var.environment
    aws_region       = var.aws_region
    account_id       = data.aws_caller_identity.current.account_id
    
    # S3 paths
    bronze_path = "s3://${module.s3.data_lake_bucket}/bronze"
    silver_path = "s3://${module.s3.data_lake_bucket}/silver"
    gold_path   = "s3://${module.s3.data_lake_bucket}/gold"
    
    # Glue databases
    bronze_db = module.glue.bronze_database
    silver_db = module.glue.silver_database
    gold_db   = module.glue.gold_database
    
    # EMR
    emr_application_id = module.emrserverless.application_id
    
    # SQS
    rest_queue_url = module.sqs.rest_queue_url
    
    # Lambda
    rest_producer_function = module.lambda.rest_producer_function_name
    rest_consumer_function = module.lambda.rest_consumer_function_name
    
    # MWAA
    mwaa_environment_name = module.mwaa.environment_name
    
    # Athena
    athena_workgroup = module.athena.workgroup_name
  }
}