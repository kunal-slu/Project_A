# Glue Module Outputs

output "bronze_database_name" {
  description = "Name of the Glue Bronze Database"
  value       = aws_glue_catalog_database.bronze.name
}

output "silver_database_name" {
  description = "Name of the Glue Silver Database"
  value       = aws_glue_catalog_database.silver.name
}

output "gold_database_name" {
  description = "Name of the Glue Gold Database"
  value       = aws_glue_catalog_database.gold.name
}