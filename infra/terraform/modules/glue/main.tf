# Glue Data Catalog Module

# Bronze Database
resource "aws_glue_catalog_database" "bronze" {
  name        = "${var.project}_bronze"
  description = "Bronze layer database for raw data"
  
  catalog_id = data.aws_caller_identity.current.account_id

  tags = {
    Project     = var.project
    Environment = var.environment
    Layer       = "bronze"
  }
}

# Silver Database
resource "aws_glue_catalog_database" "silver" {
  name        = "${var.project}_silver"
  description = "Silver layer database for cleaned data"
  
  catalog_id = data.aws_caller_identity.current.account_id

  tags = {
    Project     = var.project
    Environment = var.environment
    Layer       = "silver"
  }
}

# Gold Database
resource "aws_glue_catalog_database" "gold" {
  name        = "${var.project}_gold"
  description = "Gold layer database for business-ready data"
  
  catalog_id = data.aws_caller_identity.current.account_id

  tags = {
    Project     = var.project
    Environment = var.environment
    Layer       = "gold"
  }
}

# Data sources
data "aws_caller_identity" "current" {}