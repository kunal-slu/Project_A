# Azure Infrastructure Variables

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "East US"
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "pyspark-de-project"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints for storage and key vault"
  type        = bool
  default     = false
}

variable "subnet_id" {
  description = "Subnet ID for private endpoints"
  type        = string
  default     = null
}

variable "tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}
