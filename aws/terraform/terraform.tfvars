# Default Terraform Variables
# Override these per environment using -var-file flag

project     = "pyspark-etl-project"
environment = "dev"
region      = "us-east-1"

# VPC Configuration
vpc_cidr                   = "10.0.0.0/16"
create_vpc                 = true
create_redshift_security_group = true

# Logging
log_retention_days = 30

# Alerting
alert_emails = [
  # "admin@example.com"
]

# Tags
tags = {
  Project     = "pyspark-etl-project"
  Environment = "dev"
  ManagedBy   = "Terraform"
  Owner       = "Data Engineering Team"
}

