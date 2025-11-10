# Lake Formation Permissions
# Implements tiered access control for Bronze/Silver/Gold layers

resource "aws_lakeformation_data_lake_settings" "main" {
  admins = [
    aws_iam_role.emr_exec.arn
    # Note: Add additional admin roles/users here once they exist in your AWS account
    # "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
  ]

  # Note: Default permissions are set per database/table via aws_lakeformation_permissions
  # Default permissions blocks only support IAM users, not roles
}

# Bronze Layer Permissions (Data Engineers only)
# Note: Commented out until DataEngineerRole is created in AWS account
# resource "aws_lakeformation_permissions" "bronze_data_engineer" {
#   principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
#   permissions = ["ALL"]
#   database {
#     name = aws_glue_catalog_database.bronze.name
#   }
# }

resource "aws_lakeformation_permissions" "bronze_emr_role" {
  principal   = aws_iam_role.emr_exec.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.bronze.name
  }
}

# Silver Layer Permissions (Data Engineers + Data Analysts)
# Note: Commented out until roles are created in AWS account
# resource "aws_lakeformation_permissions" "silver_data_engineer" {
#   principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
#   permissions = ["ALL"]
#   database {
#     name = aws_glue_catalog_database.silver.name
#   }
# }

# resource "aws_lakeformation_permissions" "silver_data_analyst" {
#   principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataAnalystRole"
#   permissions = ["SELECT"]
#   database {
#     name = aws_glue_catalog_database.silver.name
#   }
# }

resource "aws_lakeformation_permissions" "silver_emr_role" {
  principal   = aws_iam_role.emr_exec.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.silver.name
  }
}

# Gold Layer Permissions (All roles)
# Note: Commented out until roles are created in AWS account
# resource "aws_lakeformation_permissions" "gold_data_engineer" {
#   principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataEngineerRole"
#   permissions = ["ALL"]
#   database {
#     name = aws_glue_catalog_database.gold.name
#   }
# }

# resource "aws_lakeformation_permissions" "gold_data_analyst" {
#   principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataAnalystRole"
#   permissions = ["SELECT"]
#   database {
#     name = aws_glue_catalog_database.gold.name
#   }
# }

# resource "aws_lakeformation_permissions" "gold_business_user" {
#   principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/BusinessUserRole"
#   permissions = ["SELECT"]
#   database {
#     name = aws_glue_catalog_database.gold.name
#   }
# }

resource "aws_lakeformation_permissions" "gold_emr_role" {
  principal   = aws_iam_role.emr_exec.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.gold.name
  }
}

# Specific table permissions for sensitive data
# Note: Commented out until table exists and role is created
# This can be set up after ETL jobs create the dim_customers table
# resource "aws_lakeformation_permissions" "gold_customers_restricted" {
#   principal   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/BusinessUserRole"
#   permissions = ["SELECT"]
#   table {
#     database_name = aws_glue_catalog_database.gold.name
#     name          = "dim_customers"
#   }
# }

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
