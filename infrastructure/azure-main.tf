terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.0"
    }
  }
  
  backend "azurerm" {
    resource_group_name  = "terraform-state-rg"
    storage_account_name = "tfstatepyspark"
    container_name       = "tfstate"
    key                 = "pyspark-data-engineering/terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
  
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}

provider "azuread" {
  tenant_id = var.tenant_id
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "${var.project_name}-rg-${var.environment}"
  location = var.location
  
  tags = {
    Project     = "PySpark-Data-Engineering"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# Virtual Network
resource "azurerm_virtual_network" "main" {
  name                = "${var.project_name}-vnet-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  address_space       = var.vnet_address_space
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Subnets
resource "azurerm_subnet" "private" {
  count                = length(var.private_subnet_address_prefixes)
  name                 = "${var.project_name}-private-subnet-${count.index + 1}-${var.environment}"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.private_subnet_address_prefixes[count.index]]
  
  service_endpoints = ["Microsoft.Storage", "Microsoft.KeyVault"]
}

resource "azurerm_subnet" "public" {
  count                = length(var.public_subnet_address_prefixes)
  name                 = "${var.project_name}-public-subnet-${count.index + 1}-${var.environment}"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.public_subnet_address_prefixes[count.index]]
}

# Storage Accounts
resource "azurerm_storage_account" "data_lake" {
  name                     = "st${var.project_name}dl${var.environment}${random_string.storage_suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  
  is_hns_enabled = true  # Enable Hierarchical Namespace for Data Lake Gen2
  
  network_rules {
    default_action = "Deny"
    virtual_network_subnet_ids = azurerm_subnet.private[*].id
    ip_rules                   = var.allowed_ip_ranges
  }
  
  tags = {
    Name        = "Data Lake Storage"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "azurerm_storage_account" "logs" {
  name                     = "st${var.project_name}logs${var.environment}${random_string.storage_suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  
  network_rules {
    default_action = "Deny"
    virtual_network_subnet_ids = azurerm_subnet.private[*].id
    ip_rules                   = var.allowed_ip_ranges
  }
  
  tags = {
    Name        = "Logs Storage"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "azurerm_storage_account" "artifacts" {
  name                     = "st${var.project_name}art${var.environment}${random_string.storage_suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  
  network_rules {
    default_action = "Deny"
    virtual_network_subnet_ids = azurerm_subnet.private[*].id
    ip_rules                   = var.allowed_ip_ranges
  }
  
  tags = {
    Name        = "Artifacts Storage"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Data Lake Gen2 Containers
resource "azurerm_storage_data_lake_gen2_filesystem" "data_lake_bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.data_lake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "data_lake_silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.data_lake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "data_lake_gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.data_lake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "data_lake_output" {
  name               = "output"
  storage_account_id = azurerm_storage_account.data_lake.id
}

# HDInsight Spark Cluster
resource "azurerm_hdinsight_spark_cluster" "main" {
  name                = "${var.project_name}-spark-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  cluster_version     = "4.0"
  tier                = var.environment == "prod" ? "Premium" : "Standard"
  
  component_version {
    spark = "3.2"
  }
  
  gateway {
    enabled  = true
    username = var.gateway_username
    password = var.gateway_password
  }
  
  storage_account {
    storage_container_id = azurerm_storage_data_lake_gen2_filesystem.data_lake_bronze.id
    storage_account_key  = azurerm_storage_account.data_lake.primary_access_key
    is_default           = true
  }
  
  storage_account {
    storage_container_id = azurerm_storage_data_lake_gen2_filesystem.data_lake_silver.id
    storage_account_key  = azurerm_storage_account.data_lake.primary_access_key
    is_default           = false
  }
  
  storage_account {
    storage_container_id = azurerm_storage_data_lake_gen2_filesystem.data_lake_gold.id
    storage_account_key  = azurerm_storage_account.data_lake.primary_access_key
    is_default           = false
  }
  
  storage_account {
    storage_container_id = azurerm_storage_data_lake_gen2_filesystem.data_lake_output.id
    storage_account_key  = azurerm_storage_account.data_lake.primary_access_key
    is_default           = false
  }
  
  roles {
    head_node {
      vm_size  = var.head_node_vm_size
      username = var.cluster_username
      password = var.cluster_password
      
      subnet_id = azurerm_subnet.private[0].id
    }
    
    worker_node {
      vm_size               = var.worker_node_vm_size
      username              = var.cluster_username
      password              = var.cluster_password
      target_instance_count = var.worker_node_count
      
      subnet_id = azurerm_subnet.private[0].id
    }
    
    zookeeper_node {
      vm_size  = "Standard_D2s_v3"
      username = var.cluster_username
      password = var.cluster_password
      
      subnet_id = azurerm_subnet.private[0].id
    }
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Azure Databricks Workspace (Alternative to HDInsight)
resource "azurerm_databricks_workspace" "main" {
  count               = var.use_databricks ? 1 : 0
  name                = "${var.project_name}-dbricks-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.environment == "prod" ? "premium" : "standard"
  
  managed_services_cmk_key_vault_key_id = var.use_customer_managed_key ? azurerm_key_vault_key.databricks[0].id : null
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Key Vault for secrets management
resource "azurerm_key_vault" "main" {
  count                       = var.environment == "prod" ? 1 : 0
  name                        = "kv-${var.project_name}-${var.environment}-${random_string.storage_suffix.result}"
  location                    = azurerm_resource_group.main.location
  resource_group_name         = azurerm_resource_group.main.name
  enabled_for_disk_encryption = true
  tenant_id                   = var.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false
  
  sku_name = "standard"
  
  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
    ip_rules       = var.allowed_ip_ranges
    virtual_network_subnet_ids = azurerm_subnet.private[*].id
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Key Vault Key for Databricks encryption
resource "azurerm_key_vault_key" "databricks" {
  count        = var.use_databricks && var.environment == "prod" ? 1 : 0
  name         = "databricks-cmk"
  key_vault_id = azurerm_key_vault.main[0].id
  key_type     = "RSA"
  key_size     = 2048
  
  key_op = [
    "encrypt",
    "decrypt",
    "sign",
    "verify"
  ]
}

# Network Security Groups
resource "azurerm_network_security_group" "private" {
  name                = "${var.project_name}-nsg-private-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  
  security_rule {
    name                       = "AllowHTTPS"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  security_rule {
    name                       = "AllowSSH"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "azurerm_network_security_group" "public" {
  name                = "${var.project_name}-nsg-public-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  
  security_rule {
    name                       = "AllowHTTPS"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Associate NSGs with subnets
resource "azurerm_subnet_network_security_group_association" "private" {
  count                     = length(var.private_subnet_address_prefixes)
  subnet_id                 = azurerm_subnet.private[count.index].id
  network_security_group_id = azurerm_network_security_group.private.id
}

resource "azurerm_subnet_network_security_group_association" "public" {
  count                     = length(var.public_subnet_address_prefixes)
  subnet_id                 = azurerm_subnet.public[count.index].id
  network_security_group_id = azurerm_network_security_group.public.id
}

# Application Insights for monitoring
resource "azurerm_application_insights" "main" {
  name                = "${var.project_name}-appinsights-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  application_type    = "web"
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.project_name}-law-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Random string for unique resource names
resource "random_string" "storage_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Variables
variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "tenant_id" {
  description = "Azure tenant ID"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "pyspark-data-engineering"
}

variable "vnet_address_space" {
  description = "Virtual network address space"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "private_subnet_address_prefixes" {
  description = "Private subnet address prefixes"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "public_subnet_address_prefixes" {
  description = "Public subnet address prefixes"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24"]
}

variable "allowed_ip_ranges" {
  description = "Allowed IP ranges for storage access"
  type        = list(string)
  default     = []
}

variable "head_node_vm_size" {
  description = "HDInsight head node VM size"
  type        = string
  default     = "Standard_D4s_v3"
}

variable "worker_node_vm_size" {
  description = "HDInsight worker node VM size"
  type        = string
  default     = "Standard_D4s_v3"
}

variable "worker_node_count" {
  description = "Number of HDInsight worker nodes"
  type        = number
  default     = 2
}

variable "cluster_username" {
  description = "HDInsight cluster username"
  type        = string
  default     = "admin"
}

variable "cluster_password" {
  description = "HDInsight cluster password"
  type        = string
  sensitive   = true
}

variable "gateway_username" {
  description = "HDInsight gateway username"
  type        = string
  default     = "admin"
}

variable "gateway_password" {
  description = "HDInsight gateway password"
  type        = string
  sensitive   = true
}

variable "use_databricks" {
  description = "Whether to use Azure Databricks instead of HDInsight"
  type        = bool
  default     = false
}

variable "use_customer_managed_key" {
  description = "Whether to use customer managed keys for encryption"
  type        = bool
  default     = false
}

# Outputs
output "resource_group_name" {
  description = "Resource group name"
  value       = azurerm_resource_group.main.name
}

output "hdinsight_cluster_id" {
  description = "HDInsight cluster ID"
  value       = azurerm_hdinsight_spark_cluster.main.id
}

output "hdinsight_cluster_ssh_endpoint" {
  description = "HDInsight cluster SSH endpoint"
  value       = azurerm_hdinsight_spark_cluster.main.https_endpoint
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
  value       = var.use_databricks ? azurerm_databricks_workspace.main[0].id : null
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = var.use_databricks ? azurerm_databricks_workspace.main[0].workspace_url : null
}

output "data_lake_storage_account" {
  description = "Data lake storage account name"
  value       = azurerm_storage_account.data_lake.name
}

output "data_lake_storage_account_key" {
  description = "Data lake storage account primary key"
  value       = azurerm_storage_account.data_lake.primary_access_key
  sensitive   = true
}

output "logs_storage_account" {
  description = "Logs storage account name"
  value       = azurerm_storage_account.logs.name
}

output "artifacts_storage_account" {
  description = "Artifacts storage account name"
  value       = azurerm_storage_account.artifacts.name
}

output "application_insights_key" {
  description = "Application Insights instrumentation key"
  value       = azurerm_application_insights.main.instrumentation_key
}

output "log_analytics_workspace_id" {
  description = "Log Analytics workspace ID"
  value       = azurerm_log_analytics_workspace.main.id
}
