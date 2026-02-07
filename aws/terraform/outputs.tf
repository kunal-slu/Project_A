# AWS Infrastructure Outputs

output "kms_key_arn" {
  value = aws_kms_key.cmk.arn
}

output "s3_lake_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "s3_artifacts_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "s3_logs_bucket_name" {
  value = aws_s3_bucket.logs.bucket
}

output "s3_code_bucket_name" {
  value = aws_s3_bucket.code.bucket
}

output "emr_app_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_exec_role_arn" {
  value = aws_iam_role.emr_exec.arn
}
