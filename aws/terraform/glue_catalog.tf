# Glue Catalog - Metadata for Delta Tables

# Bronze Database
resource "aws_glue_catalog_database" "bronze" {
  name        = "${var.project_name}_bronze_${var.environment}"
  description = "Bronze layer for raw ingested data"

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}_bronze_${var.environment}"
      Layer       = "Bronze"
      Environment = var.environment
    }
  )
}

# Silver Database
resource "aws_glue_catalog_database" "silver" {
  name        = "${var.project_name}_silver_${var.environment}"
  description = "Silver layer for cleaned and conformed data"

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}_silver_${var.environment}"
      Layer       = "Silver"
      Environment = var.environment
    }
  )
}

# Gold Database
resource "aws_glue_catalog_database" "gold" {
  name        = "${var.project_name}_gold_${var.environment}"
  description = "Gold layer for business-ready data"

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}_gold_${var.environment}"
      Layer       = "Gold"
      Environment = var.environment
    }
  )
}

