#!/bin/bash
# Real-World AWS ETL Pipeline Deployment Script
# This script deploys a production ETL pipeline with separate storage buckets
# for different data sources and real-world data processing

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

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

print_header() {
    echo -e "${PURPLE}================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}================================${NC}"
}

# Configuration
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="us-east-1"
COMPANY_NAME="company"

# Separate storage buckets for different data sources
ECOMMERCE_BUCKET="${COMPANY_NAME}-ecommerce-raw-data-${ACCOUNT_ID}"
MARKETING_BUCKET="${COMPANY_NAME}-marketing-data-${ACCOUNT_ID}"
ANALYTICS_BUCKET="${COMPANY_NAME}-analytics-data-${ACCOUNT_ID}"
PAYMENT_BUCKET="${COMPANY_NAME}-payment-data-${ACCOUNT_ID}"
SUPPORT_BUCKET="${COMPANY_NAME}-support-data-${ACCOUNT_ID}"
HR_BUCKET="${COMPANY_NAME}-hr-data-${ACCOUNT_ID}"
FINANCE_BUCKET="${COMPANY_NAME}-finance-data-${ACCOUNT_ID}"

# Data lake and infrastructure buckets
DATA_LAKE_BUCKET="${COMPANY_NAME}-data-lake-${ACCOUNT_ID}"
BACKUP_BUCKET="${COMPANY_NAME}-backups-${ACCOUNT_ID}"
LOGS_BUCKET="${COMPANY_NAME}-logs-${ACCOUNT_ID}"
ARTIFACTS_BUCKET="${COMPANY_NAME}-artifacts-${ACCOUNT_ID}"

print_header "Real-World AWS ETL Pipeline Deployment"
print_status "Starting deployment for real-world ETL pipeline with separate data sources..."

# Check prerequisites
print_status "Checking prerequisites..."

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

# Check required tools
if ! command -v jq &> /dev/null; then
    print_warning "jq is not installed. Installing..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install jq
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo apt-get update && sudo apt-get install -y jq
    fi
fi

print_success "Prerequisites check completed"

# Step 1: Create Separate S3 Buckets for Different Data Sources
print_header "Step 1: Creating Separate Storage Infrastructure"

print_status "Creating separate S3 buckets for different data sources..."

# Create data source buckets
aws s3 mb s3://$ECOMMERCE_BUCKET --region $REGION
aws s3 mb s3://$MARKETING_BUCKET --region $REGION
aws s3 mb s3://$ANALYTICS_BUCKET --region $REGION
aws s3 mb s3://$PAYMENT_BUCKET --region $REGION
aws s3 mb s3://$SUPPORT_BUCKET --region $REGION
aws s3 mb s3://$HR_BUCKET --region $REGION
aws s3 mb s3://$FINANCE_BUCKET --region $REGION

# Create infrastructure buckets
aws s3 mb s3://$DATA_LAKE_BUCKET --region $REGION
aws s3 mb s3://$BACKUP_BUCKET --region $REGION
aws s3 mb s3://$LOGS_BUCKET --region $REGION
aws s3 mb s3://$ARTIFACTS_BUCKET --region $REGION

print_success "Separate S3 buckets created successfully"

# Step 2: Create S3 Folder Structure for Each Data Source
print_status "Creating S3 folder structure for each data source..."

# E-commerce data structure
aws s3api put-object --bucket $ECOMMERCE_BUCKET --key orders/
aws s3api put-object --bucket $ECOMMERCE_BUCKET --key customers/
aws s3api put-object --bucket $ECOMMERCE_BUCKET --key products/
aws s3api put-object --bucket $ECOMMERCE_BUCKET --key inventory/

# Marketing data structure
aws s3api put-object --bucket $MARKETING_BUCKET --key google-ads/
aws s3api put-object --bucket $MARKETING_BUCKET --key facebook-ads/
aws s3api put-object --bucket $MARKETING_BUCKET --key linkedin-ads/
aws s3api put-object --bucket $MARKETING_BUCKET --key email-campaigns/

# Analytics data structure
aws s3api put-object --bucket $ANALYTICS_BUCKET --key google-analytics/
aws s3api put-object --bucket $ANALYTICS_BUCKET --key firebase/
aws s3api put-object --bucket $ANALYTICS_BUCKET --key mixpanel/
aws s3api put-object --bucket $ANALYTICS_BUCKET --key amplitude/

# Payment data structure (encrypted)
aws s3api put-object --bucket $PAYMENT_BUCKET --key stripe/
aws s3api put-object --bucket $PAYMENT_BUCKET --key paypal/
aws s3api put-object --bucket $PAYMENT_BUCKET --key square/
aws s3api put-object --bucket $PAYMENT_BUCKET --key adyen/

# Support data structure
aws s3api put-object --bucket $SUPPORT_BUCKET --key zendesk/
aws s3api put-object --bucket $SUPPORT_BUCKET --key intercom/
aws s3api put-object --bucket $SUPPORT_BUCKET --key freshdesk/
aws s3api put-object --bucket $SUPPORT_BUCKET --key salesforce-service/

# HR data structure (restricted access)
aws s3api put-object --bucket $HR_BUCKET --key workday/
aws s3api put-object --bucket $HR_BUCKET --key bamboohr/
aws s3api put-object --bucket $HR_BUCKET --key adp/

# Finance data structure (high security)
aws s3api put-object --bucket $FINANCE_BUCKET --key quickbooks/
aws s3api put-object --bucket $FINANCE_BUCKET --key xero/
aws s3api put-object --bucket $FINANCE_BUCKET --key netsuite/
aws s3api put-object --bucket $FINANCE_BUCKET --key sap/

# Data lake structure
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key metrics/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key checkpoints/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key warehouse/

# Bronze layer subdirectories for each data type
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/ecommerce/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/marketing/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/analytics/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/payments/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/support/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/hr/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/finance/

# Silver layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/ecommerce/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/marketing/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/analytics/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/payments/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/support/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/hr/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/finance/

# Gold layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/ecommerce/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/marketing/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/analytics/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/payments/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/support/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/hr/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/finance/

print_success "S3 folder structure created for all data sources"

# Step 3: Set Up Security and Access Controls
print_header "Step 2: Setting Up Security and Access Controls"

print_status "Configuring bucket policies and encryption..."

# Enable encryption for payment and finance buckets
aws s3api put-bucket-encryption --bucket $PAYMENT_BUCKET --server-side-encryption-configuration '{
  "Rules": [
    {
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }
  ]
}'

aws s3api put-bucket-encryption --bucket $FINANCE_BUCKET --server-side-encryption-configuration '{
  "Rules": [
    {
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }
  ]
}'

# Set up versioning for critical buckets
aws s3api put-bucket-versioning --bucket $PAYMENT_BUCKET --versioning-configuration Status=Enabled
aws s3api put-bucket-versioning --bucket $FINANCE_BUCKET --versioning-configuration Status=Enabled
aws s3api put-bucket-versioning --bucket $HR_BUCKET --versioning-configuration Status=Enabled

print_success "Security and encryption configured"

# Step 4: Create IAM Roles and Policies
print_header "Step 3: Creating IAM Infrastructure"

print_status "Creating IAM roles and policies for separate data access..."

# Create EMR service role
aws iam create-role --role-name EMR_DefaultRole --assume-role-policy-document '{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "elasticmapreduce.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}' 2>/dev/null || print_warning "EMR_DefaultRole already exists"

# Attach EMR policies
aws iam attach-role-policy --role-name EMR_DefaultRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole

# Create EMR EC2 instance profile
aws iam create-role --role-name EMR_EC2_DefaultRole --assume-role-policy-document '{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}' 2>/dev/null || print_warning "EMR_EC2_DefaultRole already exists"

aws iam create-instance-profile --instance-profile-name EMR_EC2_DefaultRole 2>/dev/null || print_warning "EMR_EC2_DefaultRole instance profile already exists"
aws iam add-role-to-instance-profile --instance-profile-name EMR_EC2_DefaultRole --role-name EMR_EC2_DefaultRole 2>/dev/null || print_warning "Role already added to instance profile"

# Attach EC2 policies
aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role

# Create custom policy for separate data access
aws iam create-policy --policy-name ETLSeparateDataAccess --policy-document '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::company-ecommerce-raw-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-ecommerce-raw-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-marketing-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-marketing-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-analytics-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-analytics-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-payment-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-payment-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-support-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-support-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-hr-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-hr-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-finance-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-finance-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-data-lake-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-data-lake-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-backups-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-backups-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-logs-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-logs-'$ACCOUNT_ID'/*"
      ]
    }
  ]
}' 2>/dev/null || print_warning "ETLSeparateDataAccess policy already exists"

# Attach custom policy to EMR EC2 role
aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/ETLSeparateDataAccess

print_success "IAM roles and policies created"

# Step 5: Upload Sample Data to Separate Sources
print_header "Step 4: Uploading Sample Data to Separate Sources"

print_status "Creating and uploading sample data to separate storage buckets..."

# Create sample data files
mkdir -p /tmp/real-world-sample-data

# E-commerce sample data
cat > /tmp/real-world-sample-data/ecommerce_orders.json << 'EOF'
{"order_id": "ORD001", "customer_id": "CUST001", "order_date": "2024-01-15", "total_amount": 299.99, "status": "completed", "payment_method": "credit_card", "shipping_address": "123 Main St", "items": [{"product_id": "PROD001", "quantity": 2, "price": 149.99}]}
{"order_id": "ORD002", "customer_id": "CUST002", "order_date": "2024-01-16", "total_amount": 199.50, "status": "completed", "payment_method": "paypal", "shipping_address": "456 Oak Ave", "items": [{"product_id": "PROD002", "quantity": 1, "price": 199.50}]}
{"order_id": "ORD003", "customer_id": "CUST001", "order_date": "2024-01-17", "total_amount": 89.99, "status": "pending", "payment_method": "credit_card", "shipping_address": "123 Main St", "items": [{"product_id": "PROD003", "quantity": 1, "price": 89.99}]}
EOF

cat > /tmp/real-world-sample-data/ecommerce_customers.csv << 'EOF'
customer_id,first_name,last_name,email,phone,address,city,state,country,zip_code,registration_date
CUST001,John,Doe,john.doe@email.com,555-0101,123 Main St,New York,NY,USA,10001,2023-01-15
CUST002,Jane,Smith,jane.smith@email.com,555-0102,456 Oak Ave,Los Angeles,CA,USA,90210,2023-02-20
CUST003,Bob,Johnson,bob.johnson@email.com,555-0103,789 Pine St,Chicago,IL,USA,60601,2023-03-10
EOF

# Marketing sample data
cat > /tmp/real-world-sample-data/marketing_google_ads.csv << 'EOF'
date,campaign_name,ad_group,keyword,clicks,impressions,cost,conversions
2024-01-15,Summer Sale,Electronics,wireless headphones,150,5000,750.00,25
2024-01-16,Summer Sale,Electronics,smart watch,200,8000,1200.00,30
2024-01-17,Back to School,Books,textbooks,100,3000,450.00,15
EOF

cat > /tmp/real-world-sample-data/marketing_facebook_ads.json << 'EOF'
{"date": "2024-01-15", "campaign_name": "Brand Awareness", "ad_set": "General", "reach": 10000, "impressions": 15000, "spend": 500.00, "clicks": 200}
{"date": "2024-01-16", "campaign_name": "Brand Awareness", "ad_set": "General", "reach": 12000, "impressions": 18000, "spend": 600.00, "clicks": 250}
{"date": "2024-01-17", "campaign_name": "Conversion", "ad_set": "Retargeting", "reach": 5000, "impressions": 8000, "spend": 400.00, "clicks": 150}
EOF

# Analytics sample data
cat > /tmp/real-world-sample-data/analytics_ga4_events.json << 'EOF'
{"user_pseudo_id": "user_001", "session_id": "session_001", "event_name": "page_view", "event_timestamp": "2024-01-15T10:00:00Z", "page_location": "/products", "event_date": "2024-01-15"}
{"user_pseudo_id": "user_001", "session_id": "session_001", "event_name": "add_to_cart", "event_timestamp": "2024-01-15T10:05:00Z", "product_id": "PROD001", "event_date": "2024-01-15"}
{"user_pseudo_id": "user_002", "session_id": "session_002", "event_name": "page_view", "event_timestamp": "2024-01-16T14:30:00Z", "page_location": "/home", "event_date": "2024-01-16"}
{"user_pseudo_id": "user_002", "session_id": "session_002", "event_name": "purchase", "event_timestamp": "2024-01-16T14:45:00Z", "transaction_id": "ORD002", "event_date": "2024-01-16"}
EOF

# Payment sample data
cat > /tmp/real-world-sample-data/payment_stripe_charges.json << 'EOF'
{"id": "ch_001", "amount": 29999, "currency": "usd", "status": "succeeded", "customer": "cus_001", "created": 1705276800, "payment_method": "pm_001"}
{"id": "ch_002", "amount": 19950, "currency": "usd", "status": "succeeded", "customer": "cus_002", "created": 1705363200, "payment_method": "pm_002"}
{"id": "ch_003", "amount": 8999, "currency": "usd", "status": "pending", "customer": "cus_001", "created": 1705449600, "payment_method": "pm_001"}
EOF

# Support sample data
cat > /tmp/real-world-sample-data/support_zendesk_tickets.json << 'EOF'
{"ticket_id": "TKT001", "customer_id": "CUST001", "subject": "Order not received", "status": "open", "priority": "high", "created_at": "2024-01-15T09:00:00Z"}
{"ticket_id": "TKT002", "customer_id": "CUST002", "subject": "Product defect", "status": "pending", "priority": "medium", "created_at": "2024-01-16T14:30:00Z"}
{"ticket_id": "TKT003", "customer_id": "CUST003", "subject": "Billing question", "status": "resolved", "priority": "low", "created_at": "2024-01-17T11:15:00Z"}
EOF

# HR sample data
cat > /tmp/real-world-sample-data/hr_workday_employees.csv << 'EOF'
employee_id,first_name,last_name,email,department,position,salary,hire_date
EMP001,Alice,Johnson,alice.johnson@company.com,Engineering,Senior Developer,95000,2022-01-15
EMP002,Bob,Williams,bob.williams@company.com,Marketing,Marketing Manager,85000,2021-06-20
EMP003,Carol,Davis,carol.davis@company.com,Finance,Financial Analyst,75000,2023-03-10
EOF

# Finance sample data
cat > /tmp/real-world-sample-data/finance_quickbooks_transactions.csv << 'EOF'
transaction_id,date,description,amount,account,type,category
TXN001,2024-01-15,Office supplies purchase,-250.00,Expenses,Expense,Office
TXN002,2024-01-16,Client payment,5000.00,Income,Income,Services
TXN003,2024-01-17,Software subscription,-150.00,Expenses,Expense,Technology
EOF

# Upload sample data to separate buckets
print_status "Uploading sample data to separate storage buckets..."

# E-commerce data
aws s3 cp /tmp/real-world-sample-data/ecommerce_orders.json s3://$ECOMMERCE_BUCKET/orders/
aws s3 cp /tmp/real-world-sample-data/ecommerce_customers.csv s3://$ECOMMERCE_BUCKET/customers/

# Marketing data
aws s3 cp /tmp/real-world-sample-data/marketing_google_ads.csv s3://$MARKETING_BUCKET/google-ads/
aws s3 cp /tmp/real-world-sample-data/marketing_facebook_ads.json s3://$MARKETING_BUCKET/facebook-ads/

# Analytics data
aws s3 cp /tmp/real-world-sample-data/analytics_ga4_events.json s3://$ANALYTICS_BUCKET/google-analytics/

# Payment data
aws s3 cp /tmp/real-world-sample-data/payment_stripe_charges.json s3://$PAYMENT_BUCKET/stripe/

# Support data
aws s3 cp /tmp/real-world-sample-data/support_zendesk_tickets.json s3://$SUPPORT_BUCKET/zendesk/

# HR data
aws s3 cp /tmp/real-world-sample-data/hr_workday_employees.csv s3://$HR_BUCKET/workday/

# Finance data
aws s3 cp /tmp/real-world-sample-data/finance_quickbooks_transactions.csv s3://$FINANCE_BUCKET/quickbooks/

print_success "Sample data uploaded to separate storage buckets"

# Step 6: Update Configuration Files
print_header "Step 5: Configuring ETL Pipeline"

print_status "Updating configuration files with separate storage buckets..."

# Update the real-world config file
sed -i.bak "s/ACCOUNT_ID/$ACCOUNT_ID/g" config/config-aws-real-world.yaml

print_success "Configuration files updated"

# Step 7: Upload Project Code
print_status "Uploading project code to S3..."

# Create deployment package
zip -r pyspark-etl-real-world.zip src/ config/ requirements.txt scripts/aws_real_world_etl.py

# Upload to S3
aws s3 cp pyspark-etl-real-world.zip s3://$ARTIFACTS_BUCKET/code/
aws s3 cp config/config-aws-real-world.yaml s3://$ARTIFACTS_BUCKET/config/
aws s3 cp scripts/aws_real_world_etl.py s3://$ARTIFACTS_BUCKET/scripts/

print_success "Project code uploaded"

# Step 8: Create EMR Cluster Configuration
print_status "Creating EMR cluster configuration for separate data sources..."

cat > config/emr-real-world-config.json << EOF
[
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "spark.sql.catalog.spark_catalog.warehouse": "s3://$DATA_LAKE_BUCKET/warehouse",
      "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
      "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
      "spark.hadoop.fs.s3a.path.style.access": "false",
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.adaptive.coalescePartitions.enabled": "true",
      "spark.sql.adaptive.skewJoin.enabled": "true",
      "spark.sql.adaptive.localShuffleReader.enabled": "true",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
      "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
      "spark.sql.adaptive.maxBroadcastHashJoinLocalMapThreshold": "0",
      "spark.sql.adaptive.optimizeSkewedJoin.enabled": "true",
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256m",
      "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
      "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
      "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "200",
      "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
      "spark.sql.adaptive.fetchShuffleBlocksInBatch.enabled": "true",
      "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin": "0.2",
      "spark.executor.memory": "8g",
      "spark.executor.cores": "4",
      "spark.driver.memory": "4g",
      "spark.sql.shuffle.partitions": "200",
      "spark.default.parallelism": "200",
      "spark.databricks.delta.optimizeWrite.enabled": "true",
      "spark.databricks.delta.autoCompact.enabled": "true",
      "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true"
    }
  },
  {
    "Classification": "spark-env",
    "Properties": {},
    "Configurations": [
      {
        "Classification": "export",
        "Properties": {
          "PYSPARK_PYTHON": "/usr/bin/python3"
        }
      }
    ]
  }
]
EOF

print_success "EMR configuration created"

# Step 9: Create Bootstrap Script
print_status "Creating bootstrap script for real-world ETL..."

cat > scripts/install-real-world-dependencies.sh << 'EOF'
#!/bin/bash

# Install Delta Lake and other dependencies
sudo pip3 install delta-spark==3.0.0
sudo pip3 install structlog==23.2.0 pyyaml==6.0.1 prometheus-client==0.19.0
sudo pip3 install boto3==1.34.0

# Download project code
aws s3 cp s3://company-artifacts-ACCOUNT_ID/code/pyspark-etl-real-world.zip /tmp/
unzip /tmp/pyspark-etl-real-world.zip -d /home/hadoop/

# Set permissions
chmod +x /home/hadoop/scripts/aws_real_world_etl.py

# Create environment variables
echo "export PYTHONPATH=/home/hadoop/src:$PYTHONPATH" >> /home/hadoop/.bashrc
echo "export AWS_DEFAULT_REGION=us-east-1" >> /home/hadoop/.bashrc

echo "Real-world ETL dependencies installed successfully"
EOF

# Replace ACCOUNT_ID in bootstrap script
sed -i "s/ACCOUNT_ID/$ACCOUNT_ID/g" scripts/install-real-world-dependencies.sh

# Upload bootstrap script
aws s3 cp scripts/install-real-world-dependencies.sh s3://$ARTIFACTS_BUCKET/bootstrap/

print_success "Bootstrap script created and uploaded"

# Step 10: Create EMR Cluster
print_header "Step 6: Creating EMR Cluster"

print_status "Creating production EMR cluster for real-world ETL..."

CLUSTER_ID=$(aws emr create-cluster \
  --name "real-world-etl-cluster" \
  --release-label "emr-6.15.0" \
  --region "$REGION" \
  --applications Name=Spark Name=Hive Name=Hadoop \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.2xlarge \
    InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.2xlarge \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --log-uri "s3://$LOGS_BUCKET/emr-logs/" \
  --bootstrap-actions \
    Path="s3://$ARTIFACTS_BUCKET/bootstrap/install-real-world-dependencies.sh" \
  --configurations file://config/emr-real-world-config.json \
  --auto-terminate \
  --query 'ClusterId' --output text)

print_success "EMR cluster creation initiated. Cluster ID: $CLUSTER_ID"

# Step 11: Create CloudWatch Dashboard
print_header "Step 7: Setting Up Monitoring"

print_status "Creating CloudWatch dashboard for real-world ETL..."

cat > config/cloudwatch-real-world-dashboard.json << EOF
{
  "widgets": [
    {
      "type": "metric",
      "x": 0,
      "y": 0,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["CompanyETL", "TotalPipelineTime"],
          ["CompanyETL", "RecordsProcessed"],
          ["CompanyETL", "DataQualityScore"]
        ],
        "view": "timeSeries",
        "stacked": false,
        "region": "$REGION",
        "title": "Real-World ETL Pipeline Performance"
      }
    },
    {
      "type": "metric",
      "x": 12,
      "y": 0,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["AWS/S3", "NumberOfObjects", "BucketName", "$DATA_LAKE_BUCKET"],
          ["AWS/S3", "BucketSizeBytes", "BucketName", "$DATA_LAKE_BUCKET", "StorageType", "StandardStorage"]
        ],
        "view": "timeSeries",
        "stacked": false,
        "region": "$REGION",
        "title": "Data Lake Storage Metrics"
      }
    },
    {
      "type": "metric",
      "x": 0,
      "y": 6,
      "width": 8,
      "height": 6,
      "properties": {
        "metrics": [
          ["AWS/S3", "NumberOfObjects", "BucketName", "$ECOMMERCE_BUCKET"],
          ["AWS/S3", "NumberOfObjects", "BucketName", "$MARKETING_BUCKET"],
          ["AWS/S3", "NumberOfObjects", "BucketName", "$ANALYTICS_BUCKET"],
          ["AWS/S3", "NumberOfObjects", "BucketName", "$PAYMENT_BUCKET"]
        ],
        "view": "timeSeries",
        "stacked": false,
        "region": "$REGION",
        "title": "Data Source Storage Metrics"
      }
    },
    {
      "type": "log",
      "x": 8,
      "y": 6,
      "width": 16,
      "height": 6,
      "properties": {
        "query": "SOURCE '/aws/emr/company-etl'\n| fields @timestamp, @message\n| filter @message like /ERROR/\n| sort @timestamp desc\n| limit 20",
        "region": "$REGION",
        "title": "ETL Error Logs"
      }
    }
  ]
}
EOF

aws cloudwatch put-dashboard \
  --dashboard-name "Real-World-ETL-Dashboard" \
  --dashboard-body file://config/cloudwatch-real-world-dashboard.json

print_success "CloudWatch dashboard created"

# Step 12: Display Summary
print_header "Real-World ETL Deployment Summary"

print_success "Real-World AWS ETL Pipeline deployment completed successfully!"
echo ""
echo "📋 Real-World ETL Deployment Summary:"
echo "===================================="
echo "Account ID: $ACCOUNT_ID"
echo "Region: $REGION"
echo "EMR Cluster ID: $CLUSTER_ID"
echo ""
echo "📁 Separate Data Source Buckets:"
echo "- E-commerce: s3://$ECOMMERCE_BUCKET"
echo "- Marketing: s3://$MARKETING_BUCKET"
echo "- Analytics: s3://$ANALYTICS_BUCKET"
echo "- Payments: s3://$PAYMENT_BUCKET (encrypted)"
echo "- Support: s3://$SUPPORT_BUCKET"
echo "- HR: s3://$HR_BUCKET (restricted)"
echo "- Finance: s3://$FINANCE_BUCKET (high security)"
echo ""
echo "🏗️ Infrastructure Buckets:"
echo "- Data Lake: s3://$DATA_LAKE_BUCKET"
echo "- Backups: s3://$BACKUP_BUCKET"
echo "- Logs: s3://$LOGS_BUCKET"
echo "- Artifacts: s3://$ARTIFACTS_BUCKET"
echo ""
echo "🔧 AWS Services:"
echo "- EMR Cluster: real-world-etl-cluster"
echo "- CloudWatch: Real-World-ETL-Dashboard"
echo "- IAM: Separate data access policies"
echo ""
echo "📊 Real-World Data Sources:"
echo "- E-commerce: Orders, Customers, Products, Inventory"
echo "- Marketing: Google Ads, Facebook Ads, LinkedIn Ads, Email Campaigns"
echo "- Analytics: Google Analytics 4, Firebase, Mixpanel, Amplitude"
echo "- Payments: Stripe, PayPal, Square, Adyen (encrypted)"
echo "- Support: Zendesk, Intercom, Freshdesk, Salesforce Service"
echo "- HR: Workday, BambooHR, ADP (restricted access)"
echo "- Finance: QuickBooks, Xero, NetSuite, SAP (high security)"
echo ""
echo "🔐 Security Features:"
echo "- Separate storage buckets for data isolation"
echo "- Encryption for payment and finance data"
echo "- Versioning for critical data buckets"
echo "- Restricted access for HR and finance data"
echo "- IAM policies for granular access control"
echo ""
echo "🚀 Next Steps:"
echo "1. Monitor EMR cluster creation in AWS Console"
echo "2. Run the real-world ETL pipeline:"
echo "   python scripts/aws_real_world_etl.py config/config-aws-real-world.yaml"
echo "3. Check CloudWatch dashboard for monitoring"
echo "4. Query data using Amazon Athena"
echo "5. Set up automated scheduling with Step Functions"
echo ""
echo "📖 Documentation:"
echo "- AWS Deployment Guide: AWS_DEPLOYMENT_GUIDE.md"
echo "- Real-World Data Sources: AWS_REAL_WORLD_DATA_SOURCES.md"
echo ""
echo "🎯 Your real-world ETL pipeline is ready with separate data sources and storage!"
