# Networking Configuration for AWS Resources
# Creates VPC, subnets, and security groups for private resources

resource "aws_vpc" "data_platform" {
  count = var.create_vpc ? 1 : 0
  
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = merge(
    var.tags,
    {
      Name = "${var.project}-${var.environment}-vpc"
    }
  )
}

# Security Group for EMR Serverless
resource "aws_security_group" "emr_serverless" {
  name_prefix = "${var.project}-${var.environment}-emr-"
  vpc_id      = var.create_vpc ? aws_vpc.data_platform[0].id : var.vpc_id
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(
    var.tags,
    {
      Name = "${var.project}-${var.environment}-emr-sg"
    }
  )
}

# Security Group for Redshift (if private)
resource "aws_security_group" "redshift" {
  count = var.create_redshift_security_group ? 1 : 0
  
  name_prefix = "${var.project}-${var.environment}-redshift-"
  vpc_id      = var.create_vpc ? aws_vpc.data_platform[0].id : var.vpc_id
  
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  
  tags = merge(
    var.tags,
    {
      Name = "${var.project}-${var.environment}-redshift-sg"
    }
  )
}

