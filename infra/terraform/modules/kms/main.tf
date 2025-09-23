# KMS module for encryption keys

# S3 KMS Key
resource "aws_kms_key" "s3_key" {
  description             = "KMS key for S3 data lake encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-s3-key"
    Purpose     = "s3-encryption"
    Environment = var.environment
  })
}

resource "aws_kms_alias" "s3_key" {
  name          = "alias/${var.project_name}-${var.environment}-s3-key"
  target_key_id = aws_kms_key.s3_key.key_id
}

# Logs KMS Key
resource "aws_kms_key" "logs_key" {
  description             = "KMS key for logs encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-logs-key"
    Purpose     = "logs-encryption"
    Environment = var.environment
  })
}

resource "aws_kms_alias" "logs_key" {
  name          = "alias/${var.project_name}-${var.environment}-logs-key"
  target_key_id = aws_kms_key.logs_key.key_id
}

# EMR KMS Key
resource "aws_kms_key" "emr_key" {
  description             = "KMS key for EMR encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-emr-key"
    Purpose     = "emr-encryption"
    Environment = var.environment
  })
}

resource "aws_kms_alias" "emr_key" {
  name          = "alias/${var.project_name}-${var.environment}-emr-key"
  target_key_id = aws_kms_key.emr_key.key_id
}

# Secrets Manager KMS Key
resource "aws_kms_key" "secrets_key" {
  description             = "KMS key for Secrets Manager encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-secrets-key"
    Purpose     = "secrets-encryption"
    Environment = var.environment
  })
}

resource "aws_kms_alias" "secrets_key" {
  name          = "alias/${var.project_name}-${var.environment}-secrets-key"
  target_key_id = aws_kms_key.secrets_key.key_id
}
