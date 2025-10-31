# Lake Formation Permissions
# Implements tiered access control for Bronze/Silver/Gold layers

resource "aws_lakeformation_data_lake_settings" "main" {
  data_lake_admins = [
    aws_iam_role.emr_execution_role.arn,
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
  ]
  
  create_database_default_permissions {
    principal   = aws_iam_role.emr_execution_role.arn
    permissions = ["ALL"]
  }
  
  create_table_default_permissions {
    principal   = aws_iam_role.emr_execution_role.arn
    permissions = ["ALL"]
  }
}

# Bronze Layer Permissions (Data Engineers only)
resource "aws_lakeformation_permissions" "bronze_data_engineer" {
  principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
  permissions = ["ALL"]
  
  database {
    name = aws_glue_catalog_database.bronze.name
  }
}

resource "aws_lakeformation_permissions" "bronze_emr_role" {
  principal   = aws_iam_role.emr_execution_role.arn
  permissions = ["ALL"]
  
  database {
    name = aws_glue_catalog_database.bronze.name
  }
}

# Silver Layer Permissions (Data Engineers + Data Analysts)
resource "aws_lakeformation_permissions" "silver_data_engineer" {
  principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
  permissions = ["ALL"]
  
  database {
    name = aws_glue_catalog_database.silver.name
  }
}

resource "aws_lakeformation_permissions" "silver_data_analyst" {
  principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataAnalystRole"
  permissions = ["SELECT"]
  
  database {
    name = aws_glue_catalog_database.silver.name
  }
}

resource "aws_lakeformation_permissions" "silver_emr_role" {
  principal   = aws_iam_role.emr_execution_role.arn
  permissions = ["ALL"]
  
  database {
    name = aws_glue_catalog_database.silver.name
  }
}

# Gold Layer Permissions (All roles)
resource "aws_lakeformation_permissions" "gold_data_engineer" {
  principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
  permissions = ["ALL"]
  
  database {
    name = aws_glue_catalog_database.gold.name
  }
}

resource "aws_lakeformation_permissions" "gold_data_analyst" {
  principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataAnalystRole"
  permissions = ["SELECT"]
  
  database {
    name = aws_glue_catalog_database.gold.name
  }
}

resource "aws_lakeformation_permissions" "gold_business_user" {
  principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/BusinessUserRole"
  permissions = ["SELECT"]
  
  database {
    name = aws_glue_catalog_database.gold.name
  }
}

resource "aws_lakeformation_permissions" "gold_emr_role" {
  principal   = aws_iam_role.emr_execution_role.arn
  permissions = ["ALL"]
  
  database {
    name = aws_glue_catalog_database.gold.name
  }
}

# Specific table permissions for sensitive data
resource "aws_lakeformation_permissions" "gold_customers_restricted" {
  principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/BusinessUserRole"
  permissions = ["SELECT"]
  
  table {
    database_name = aws_glue_catalog_database.gold.name
    name          = "dim_customers"
    column_names  = ["customer_id", "customer_name", "age_group", "region"]
  }
}

# Cross-account access for external analytics
resource "aws_lakeformation_permissions" "external_analytics" {
  count = var.enable_external_access ? 1 : 0
  
  principal   = var.external_analytics_role_arn
  permissions = ["SELECT"]
  
  database {
    name = aws_glue_catalog_database.gold.name
  }
}

# Data source for current account
data "aws_caller_identity" "current" {}
