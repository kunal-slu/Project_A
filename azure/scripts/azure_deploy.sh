#!/bin/bash

# Azure Deployment Script for PySpark ETL Project
# This script automates the creation of Azure resources and data upload

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
RESOURCE_GROUP="pyspark-etl-rg"
LOCATION="East US"
STORAGE_ACCOUNT="pysparketlstorage"
DATABRICKS_WORKSPACE="pyspark-etl-workspace"
KEY_VAULT="pyspark-etl-kv"

print_status "Starting Azure deployment for PySpark ETL project..."

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    print_error "Azure CLI is not installed. Please install it first."
    exit 1
fi

# Check if logged in to Azure
if ! az account show &> /dev/null; then
    print_error "Not logged in to Azure. Please run 'az login' first."
    exit 1
fi

print_success "Azure CLI is installed and logged in"

# Step 1: Create Resource Group
print_status "Creating resource group: $RESOURCE_GROUP"
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --tags "project=pyspark-etl" "environment=dev" \
    --output none

print_success "Resource group created successfully"

# Step 2: Create Storage Account
print_status "Creating storage account: $STORAGE_ACCOUNT"
az storage account create \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku "Standard_LRS" \
    --kind "StorageV2" \
    --hierarchical-namespace true \
    --min-tls-version "TLS1_2" \
    --output none

print_success "Storage account created successfully"

# Step 3: Get Storage Account Key
print_status "Getting storage account key"
STORAGE_KEY=$(az storage account keys list \
    --account-name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query "[0].value" \
    --output tsv)

print_success "Storage account key retrieved"

# Step 4: Create Containers
print_status "Creating storage containers"
az storage container create \
    --name "lakehouse" \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --output none

az storage container create \
    --name "input-data" \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --output none

az storage container create \
    --name "output-data" \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --output none

az storage container create \
    --name "backups" \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --output none

print_success "Storage containers created successfully"

# Step 5: Create Key Vault
print_status "Creating Key Vault: $KEY_VAULT"
az keyvault create \
    --name "$KEY_VAULT" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku "standard" \
    --output none

print_success "Key Vault created successfully"

# Step 6: Store Storage Key in Key Vault
print_status "Storing storage account key in Key Vault"
az keyvault secret set \
    --vault-name "$KEY_VAULT" \
    --name "storage-account-key" \
    --value "$STORAGE_KEY" \
    --output none

print_success "Storage account key stored in Key Vault"

# Step 7: Create Databricks Workspace
print_status "Creating Databricks workspace: $DATABRICKS_WORKSPACE"
az databricks workspace create \
    --resource-group "$RESOURCE_GROUP" \
    --workspace-name "$DATABRICKS_WORKSPACE" \
    --location "$LOCATION" \
    --sku "standard" \
    --output none

print_success "Databricks workspace created successfully"

# Step 8: Get Databricks Workspace URL
print_status "Getting Databricks workspace URL"
WORKSPACE_URL=$(az databricks workspace show \
    --resource-group "$RESOURCE_GROUP" \
    --workspace-name "$DATABRICKS_WORKSPACE" \
    --query "workspaceUrl" \
    --output tsv)

print_success "Databricks workspace URL: https://$WORKSPACE_URL"

# Step 9: Upload Data Files
print_status "Uploading data files to Azure Storage"

# Check if data directory exists
if [ ! -d "data/input_data" ]; then
    print_warning "Data directory not found. Skipping data upload."
    print_warning "Please run 'python3 scripts/generate_input_data.py' first to generate sample data."
else
    # Upload all CSV files
    for file in data/input_data/*.csv; do
        if [ -f "$file" ]; then
            filename=$(basename "$file")
            print_status "Uploading $filename"
            az storage blob upload \
                --account-name "$STORAGE_ACCOUNT" \
                --account-key "$STORAGE_KEY" \
                --container-name "input-data" \
                --name "$filename" \
                --file "$file" \
                --type block \
                --output none
            print_success "Uploaded $filename"
        fi
    done

    # Upload all JSON files
    for file in data/input_data/*.json; do
        if [ -f "$file" ]; then
            filename=$(basename "$file")
            print_status "Uploading $filename"
            az storage blob upload \
                --account-name "$STORAGE_ACCOUNT" \
                --account-key "$STORAGE_KEY" \
                --container-name "input-data" \
                --name "$filename" \
                --file "$file" \
                --type block \
                --output none
            print_success "Uploaded $filename"
        fi
    done
fi

# Step 10: Create Environment File
print_status "Creating environment file"
cat > .env.azure << EOF
# Azure Environment Variables
AZURE_STORAGE_ACCOUNT=$STORAGE_ACCOUNT
AZURE_STORAGE_KEY=$STORAGE_KEY
DATABRICKS_WORKSPACE_URL=$WORKSPACE_URL
DATA_ROOT=abfss://lakehouse@$STORAGE_ACCOUNT.dfs.core.windows.net
RESOURCE_GROUP=$RESOURCE_GROUP
KEY_VAULT=$KEY_VAULT
EOF

print_success "Environment file created: .env.azure"

# Step 11: Display Summary
print_success "Azure deployment completed successfully!"
echo ""
echo "📋 Deployment Summary:"
echo "======================"
echo "Resource Group: $RESOURCE_GROUP"
echo "Storage Account: $STORAGE_ACCOUNT"
echo "Databricks Workspace: $DATABRICKS_WORKSPACE"
echo "Key Vault: $KEY_VAULT"
echo "Databricks URL: https://$WORKSPACE_URL"
echo ""
echo "📁 Storage Containers:"
echo "- lakehouse (for data lake)"
echo "- input-data (for source data)"
echo "- output-data (for processed data)"
echo "- backups (for disaster recovery)"
echo ""
echo "🔑 Secrets stored in Key Vault:"
echo "- storage-account-key"
echo ""
echo "📝 Next Steps:"
echo "1. Access Databricks workspace: https://$WORKSPACE_URL"
echo "2. Create a cluster in Databricks"
echo ""
echo "📋 Choose your deployment option:"
echo "   A) Minimal Upload (Simple):"
echo "      - Upload: config/ and notebooks/simple_azure_etl.py"
echo "      - Run: simple_azure_etl.py notebook"
echo ""
echo "   B) Complete End-to-End (Full Features):"
echo "      - Upload: src/, config/, and notebooks/complete_azure_etl.py"
echo "      - Run: complete_azure_etl.py notebook (ALL features included)"
echo ""
echo "   C) Full Code Upload (Advanced):"
echo "      - Upload: entire project (src/, config/, tests/, etc.)"
echo "      - Run: notebooks/azure_etl_pipeline.py notebook"
echo ""
echo "📖 For detailed instructions, see: AZURE_DEPLOYMENT_GUIDE.md"
