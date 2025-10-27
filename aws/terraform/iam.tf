# IAM Roles and Policies for EMR Serverless

# EMR Serverless Execution Role
resource "aws_iam_role" "emr_serverless_execution_role" {
  name = "${var.project}-${var.environment}-emr-execution-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "emr-serverless.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
  
  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-emr-execution-role"
  })
}

# EMR Serverless Execution Policy (Least Privilege)
resource "aws_iam_role_policy" "emr_serverless_execution_policy" {
  name = "${var.project}-${var.environment}-emr-execution-policy"
  role = aws_iam_role.emr_serverless_execution_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/*",
          "${aws_s3_bucket.data_lake.arn}",
          "${aws_s3_bucket.code_bundle.arn}/*",
          "${aws_s3_bucket.code_bundle.arn}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:CreateLogGroup"
        ]
        Resource = "arn:aws:logs:${var.region}:*:log-group:/aws/emr-serverless/*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartition",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:*:catalog",
          "arn:aws:glue:${var.region}:*:database/*",
          "arn:aws:glue:${var.region}:*:table/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          aws_secretsmanager_secret.hubspot.arn,
          aws_secretsmanager_secret.snowflake.arn,
          aws_secretsmanager_secret.redshift.arn,
          aws_secretsmanager_secret.kafka.arn,
          aws_secretsmanager_secret.fx_vendor.arn
        ]
      }
    ]
  })
}

# Service Role for EMR Serverless
resource "aws_iam_role" "emr_serverless_service_role" {
  name = "${var.project}-${var.environment}-emr-service-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "emr-serverless.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
  
  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-emr-service-role"
  })
}

# Attach AWS managed policy for EMR Serverless service role
resource "aws_iam_role_policy_attachment" "emr_serverless_service_role_policy" {
  role       = aws_iam_role.emr_serverless_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServerlessServiceRolePolicy"
}

