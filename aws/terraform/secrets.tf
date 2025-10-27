# Secrets Manager - Store credentials for data sources

# HubSpot/Salesforce credentials
resource "aws_secretsmanager_secret" "hubspot" {
  name = "${var.project}-${var.environment}-hubspot-creds"
  description = "HubSpot CRM credentials"
  
  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-hubspot-creds"
    Source = "hubspot"
  })
}

resource "aws_secretsmanager_secret_version" "hubspot" {
  secret_id = aws_secretsmanager_secret.hubspot.id
  
  secret_string = jsonencode({
    api_key = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
    base_url = "https://api.hubapi.com"
  })
}

# Snowflake credentials
resource "aws_secretsmanager_secret" "snowflake" {
  name = "${var.project}-${var.environment}-snowflake-creds"
  description = "Snowflake database credentials"
  
  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-snowflake-creds"
    Source = "snowflake"
  })
}

resource "aws_secretsmanager_secret_version" "snowflake" {
  secret_id = aws_secretsmanager_secret.snowflake.id
  
  secret_string = jsonencode({
    account = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
    warehouse = "COMPUTE_WH"
    database = "SOURCE_DB"
    schema = "PUBLIC"
    user = "ETL_USER"
    password = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
    role = "ACCOUNTADMIN"
  })
}

# Redshift credentials
resource "aws_secretsmanager_secret" "redshift" {
  name = "${var.project}-${var.environment}-redshift-creds"
  description = "Redshift analytics database credentials"
  
  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-redshift-creds"
    Source = "redshift"
  })
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id = aws_secretsmanager_secret.redshift.id
  
  secret_string = jsonencode({
    host = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
    port = "5439"
    database = "analytics"
    user = "etl_user"
    password = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
  })
}

# Kafka/MSK credentials
resource "aws_secretsmanager_secret" "kafka" {
  name = "${var.project}-${var.environment}-kafka-creds"
  description = "Kafka/MSK connection credentials"
  
  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-kafka-creds"
    Source = "kafka"
  })
}

resource "aws_secretsmanager_secret_version" "kafka" {
  secret_id = aws_secretsmanager_secret.kafka.id
  
  secret_string = jsonencode({
    bootstrap_servers = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
    topic = "orders_stream"
    security_protocol = "SASL_SSL"
    sasl_mechanism = "PLAIN"
  })
}

# FX Vendor credentials
resource "aws_secretsmanager_secret" "fx_vendor" {
  name = "${var.project}-${var.environment}-fx-vendor-creds"
  description = "FX rates vendor API credentials"
  
  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-fx-vendor-creds"
    Source = "fx_vendor"
  })
}

resource "aws_secretsmanager_secret_version" "fx_vendor" {
  secret_id = aws_secretsmanager_secret.fx_vendor.id
  
  secret_string = jsonencode({
    api_key = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
    base_url = "https://api.fx-vendor.com"
    api_version = "v1"
  })
}

