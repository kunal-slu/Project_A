# Azure Databricks + ADLS Gen2 Data Lake Infrastructure

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Data sources
data "azurerm_client_config" "current" {}

# Local values
locals {
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "${var.project}-${var.environment}-rg"
  location = var.location

  tags = local.common_tags
}

# Storage Account for Data Lake
resource "azurerm_storage_account" "datalake" {
  name                     = "${var.project}${var.environment}datalake"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  tags = local.common_tags
}

# Storage Container for Data Lake
resource "azurerm_storage_container" "lake" {
  name                  = "lake"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Storage Container for Artifacts
resource "azurerm_storage_container" "artifacts" {
  name                  = "artifacts"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Key Vault for Secrets
resource "azurerm_key_vault" "main" {
  name                = "${var.project}-${var.environment}-kv"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  purge_protection_enabled = false

  tags = local.common_tags
}

# Key Vault Access Policy for Current User
resource "azurerm_key_vault_access_policy" "current_user" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = [
    "Get",
    "List",
    "Set",
    "Delete",
    "Purge",
    "Recover"
  ]
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "main" {
  name                = "${var.project}-${var.environment}-dbw"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "standard"

  custom_parameters {
    no_public_ip = false
  }

  tags = local.common_tags
}

# Managed Identity for Databricks
resource "azurerm_databricks_workspace" "main_managed_identity" {
  name                = "${var.project}-${var.environment}-dbw-mi"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "standard"

  custom_parameters {
    no_public_ip = false
  }

  tags = local.common_tags
}

# Key Vault Access Policy for Databricks Managed Identity
resource "azurerm_key_vault_access_policy" "databricks" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = azurerm_databricks_workspace.main.managed_identity[0].tenant_id
  object_id    = azurerm_databricks_workspace.main.managed_identity[0].principal_id

  secret_permissions = [
    "Get",
    "List"
  ]
}

# Storage Account Access Policy for Databricks
resource "azurerm_role_assignment" "databricks_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_workspace.main.managed_identity[0].principal_id
}

# Data Factory
resource "azurerm_data_factory" "main" {
  name                = "${var.project}-${var.environment}-adf"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

# Data Factory Linked Service for ADLS Gen2
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "datalake" {
  name                = "datalake_linked_service"
  resource_group_name = azurerm_resource_group.main.name
  data_factory_name   = azurerm_data_factory.main.name
  url                 = "https://${azurerm_storage_account.datalake.name}.dfs.core.windows.net"
  use_managed_identity = true
}

# Data Factory Linked Service for Key Vault
resource "azurerm_data_factory_linked_service_key_vault" "keyvault" {
  name                = "keyvault_linked_service"
  resource_group_name = azurerm_resource_group.main.name
  data_factory_name   = azurerm_data_factory.main.name
  key_vault_id        = azurerm_key_vault.main.id
}

# Application Insights
resource "azurerm_application_insights" "main" {
  name                = "${var.project}-${var.environment}-ai"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  application_type    = "web"

  tags = local.common_tags
}

# Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.project}-${var.environment}-law"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30

  tags = local.common_tags
}

# Private Endpoints (optional)
resource "azurerm_private_endpoint" "storage" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${var.project}-${var.environment}-storage-pe"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = var.subnet_id

  private_service_connection {
    name                           = "storage-connection"
    private_connection_resource_id = azurerm_storage_account.datalake.id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  tags = local.common_tags
}

resource "azurerm_private_endpoint" "keyvault" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${var.project}-${var.environment}-kv-pe"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = var.subnet_id

  private_service_connection {
    name                           = "keyvault-connection"
    private_connection_resource_id = azurerm_key_vault.main.id
    subresource_names              = ["vault"]
    is_manual_connection           = false
  }

  tags = local.common_tags
}
