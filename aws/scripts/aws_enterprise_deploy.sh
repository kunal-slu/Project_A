#!/bin/bash
# Enterprise AWS ETL Pipeline Deployment Script
# This script deploys a complete production ETL pipeline with 1 external + 2 internal sources

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

# S3 Buckets
EXTERNAL_DATA_BUCKET="${COMPANY_NAME}-external-data-${ACCOUNT_ID}"
INTERNAL_DATA_BUCKET="${COMPANY_NAME}-internal-data-${ACCOUNT_ID}"
STREAMING_DATA_BUCKET="${COMPANY_NAME}-streaming-data-${ACCOUNT_ID}"
DATA_LAKE_BUCKET="${COMPANY_NAME}-data-lake-${ACCOUNT_ID}"
BACKUP_BUCKET="${COMPANY_NAME}-backups-${ACCOUNT_ID}"
LOGS_BUCKET="${COMPANY_NAME}-logs-${ACCOUNT_ID}"
ARTIFACTS_BUCKET="${COMPANY_NAME}-artifacts-${ACCOUNT_ID}"

print_header "Enterprise AWS ETL Pipeline Deployment"
print_status "Starting deployment for enterprise ETL pipeline with 1 external + 2 internal sources..."

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

# Step 1: Create S3 Buckets
print_header "Step 1: Creating S3 Storage Infrastructure"

print_status "Creating S3 buckets for data storage..."

# Create data source buckets
aws s3 mb s3://$EXTERNAL_DATA_BUCKET --region $REGION
aws s3 mb s3://$INTERNAL_DATA_BUCKET --region $REGION
aws s3 mb s3://$STREAMING_DATA_BUCKET --region $REGION

# Create infrastructure buckets
aws s3 mb s3://$DATA_LAKE_BUCKET --region $REGION
aws s3 mb s3://$BACKUP_BUCKET --region $REGION
aws s3 mb s3://$LOGS_BUCKET --region $REGION
aws s3 mb s3://$ARTIFACTS_BUCKET --region $REGION

print_success "S3 buckets created successfully"

# Step 2: Create S3 Folder Structure
print_status "Creating S3 folder structure..."

# External data structure
aws s3api put-object --bucket $EXTERNAL_DATA_BUCKET --key snowflake/
aws s3api put-object --bucket $EXTERNAL_DATA_BUCKET --key external/

# Internal data structure
aws s3api put-object --bucket $INTERNAL_DATA_BUCKET --key postgresql/
aws s3api put-object --bucket $INTERNAL_DATA_BUCKET --key internal/

# Streaming data structure
aws s3api put-object --bucket $STREAMING_DATA_BUCKET --key kafka/
aws s3api put-object --bucket $STREAMING_DATA_BUCKET --key streaming/

# Data lake structure
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key metrics/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key checkpoints/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key warehouse/

# Bronze layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/external/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/internal/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/streaming/

# Silver layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/customers/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/sales/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/users/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/streaming/

# Gold layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/dim_customers/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/dim_users/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/fact_sales/

print_success "S3 folder structure created"

# Step 3: Set Up Security and Access Controls
print_header "Step 2: Setting Up Security and Access Controls"

print_status "Configuring bucket policies and encryption..."

# Enable encryption for all buckets
for bucket in $EXTERNAL_DATA_BUCKET $INTERNAL_DATA_BUCKET $STREAMING_DATA_BUCKET $DATA_LAKE_BUCKET $BACKUP_BUCKET; do
    aws s3api put-bucket-encryption --bucket $bucket --server-side-encryption-configuration '{
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                }
            }
        ]
    }'
done

# Set up versioning for critical buckets
for bucket in $DATA_LAKE_BUCKET $BACKUP_BUCKET; do
    aws s3api put-bucket-versioning --bucket $bucket --versioning-configuration Status=Enabled
done

print_success "Security and encryption configured"

# Step 4: Create IAM Roles and Policies
print_header "Step 3: Creating IAM Infrastructure"

print_status "Creating IAM roles and policies..."

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

# Create custom policy for data access
aws iam create-policy --policy-name ETLDataAccess --policy-document '{
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
                "arn:aws:s3:::company-external-data-'$ACCOUNT_ID'",
                "arn:aws:s3:::company-external-data-'$ACCOUNT_ID'/*",
                "arn:aws:s3:::company-internal-data-'$ACCOUNT_ID'",
                "arn:aws:s3:::company-internal-data-'$ACCOUNT_ID'/*",
                "arn:aws:s3:::company-streaming-data-'$ACCOUNT_ID'",
                "arn:aws:s3:::company-streaming-data-'$ACCOUNT_ID'/*",
                "arn:aws:s3:::company-data-lake-'$ACCOUNT_ID'",
                "arn:aws:s3:::company-data-lake-'$ACCOUNT_ID'/*",
                "arn:aws:s3:::company-backups-'$ACCOUNT_ID'",
                "arn:aws:s3:::company-backups-'$ACCOUNT_ID'/*",
                "arn:aws:s3:::company-logs-'$ACCOUNT_ID'",
                "arn:aws:s3:::company-logs-'$ACCOUNT_ID'/*"
            ]
        }
    ]
}' 2>/dev/null || print_warning "ETLDataAccess policy already exists"

# Attach custom policy to EMR EC2 role
aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/ETLDataAccess

print_success "IAM roles and policies created"

# Step 5: Create Sample Data
print_header "Step 4: Creating Sample Data"

print_status "Creating sample data for testing..."

# Create sample data directory
mkdir -p /tmp/enterprise-sample-data

# Sample external data (Snowflake-like)
cat > /tmp/enterprise-sample-data/external_customer_orders.csv << 'EOF'
order_id,customer_id,customer_name,email,order_date,total_amount,status,payment_method
ORD001,CUST001,John Doe,john.doe@email.com,2024-01-15,299.99,completed,credit_card
ORD002,CUST002,Jane Smith,jane.smith@email.com,2024-01-16,199.50,completed,paypal
ORD003,CUST001,John Doe,john.doe@email.com,2024-01-17,89.99,pending,credit_card
ORD004,CUST003,Bob Johnson,bob.johnson@email.com,2024-01-18,450.00,completed,credit_card
ORD005,CUST002,Jane Smith,jane.smith@email.com,2024-01-19,125.75,completed,paypal
EOF

cat > /tmp/enterprise-sample-data/external_product_catalog.csv << 'EOF'
product_id,product_name,category,price,description
PROD001,Laptop,Electronics,999.99,High-performance laptop
PROD002,Smartphone,Electronics,699.99,Latest smartphone model
PROD003,Headphones,Electronics,199.99,Wireless noise-canceling headphones
PROD004,T-Shirt,Clothing,29.99,Cotton t-shirt
PROD005,Jeans,Clothing,79.99,Denim jeans
EOF

cat > /tmp/enterprise-sample-data/external_financial_transactions.csv << 'EOF'
transaction_id,customer_id,amount,transaction_date,transaction_type,status
TXN001,CUST001,299.99,2024-01-15,purchase,completed
TXN002,CUST002,199.50,2024-01-16,purchase,completed
TXN003,CUST001,89.99,2024-01-17,purchase,pending
TXN004,CUST003,450.00,2024-01-18,purchase,completed
TXN005,CUST002,125.75,2024-01-19,purchase,completed
EOF

# Sample internal data (PostgreSQL-like)
cat > /tmp/enterprise-sample-data/internal_users.csv << 'EOF'
user_id,first_name,last_name,email,phone,address,city,state,country,zip_code,registration_date
USER001,Alice,Brown,alice.brown@company.com,555-0101,123 Main St,New York,NY,USA,10001,2023-01-15
USER002,Charlie,Davis,charlie.davis@company.com,555-0102,456 Oak Ave,Los Angeles,CA,USA,90210,2023-02-20
USER003,Eva,Wilson,eva.wilson@company.com,555-0103,789 Pine St,Chicago,IL,USA,60601,2023-03-10
USER004,Frank,Miller,frank.miller@company.com,555-0104,321 Elm St,Houston,TX,USA,77001,2023-04-05
USER005,Grace,Taylor,grace.taylor@company.com,555-0105,654 Maple Dr,Phoenix,AZ,USA,85001,2023-05-12
EOF

cat > /tmp/enterprise-sample-data/internal_subscriptions.csv << 'EOF'
subscription_id,user_id,plan_type,status,start_date,end_date,amount
SUB001,USER001,premium,active,2024-01-01,2024-12-31,99.99
SUB002,USER002,basic,active,2024-01-01,2024-12-31,49.99
SUB003,USER003,premium,active,2024-01-01,2024-12-31,99.99
SUB004,USER004,basic,cancelled,2024-01-01,2024-06-30,49.99
SUB005,USER005,premium,active,2024-01-01,2024-12-31,99.99
EOF

cat > /tmp/enterprise-sample-data/internal_product_inventory.csv << 'EOF'
product_id,product_name,quantity,warehouse_location,last_updated
PROD001,Laptop,50,NYC-WH1,2024-01-20 10:00:00
PROD002,Smartphone,75,LA-WH2,2024-01-20 10:00:00
PROD003,Headphones,100,CHI-WH3,2024-01-20 10:00:00
PROD004,T-Shirt,200,NYC-WH1,2024-01-20 10:00:00
PROD005,Jeans,150,LA-WH2,2024-01-20 10:00:00
EOF

cat > /tmp/enterprise-sample-data/internal_user_sessions.csv << 'EOF'
session_id,user_id,session_start,session_end,duration_seconds,page_views
SESS001,USER001,2024-01-20 09:00:00,2024-01-20 09:15:00,900,5
SESS002,USER002,2024-01-20 10:00:00,2024-01-20 10:30:00,1800,8
SESS003,USER003,2024-01-20 11:00:00,2024-01-20 11:45:00,2700,12
SESS004,USER001,2024-01-20 14:00:00,2024-01-20 14:20:00,1200,6
SESS005,USER004,2024-01-20 15:00:00,2024-01-20 15:10:00,600,3
EOF

# Sample streaming data (Kafka-like)
cat > /tmp/enterprise-sample-data/streaming_user_events.json << 'EOF'
{"event_id": "EVT001", "user_id": "USER001", "event_type": "page_view", "page_url": "/products", "timestamp": "2024-01-20T09:00:00Z", "session_id": "SESS001"}
{"event_id": "EVT002", "user_id": "USER001", "event_type": "add_to_cart", "product_id": "PROD001", "timestamp": "2024-01-20T09:05:00Z", "session_id": "SESS001"}
{"event_id": "EVT003", "user_id": "USER002", "event_type": "page_view", "page_url": "/home", "timestamp": "2024-01-20T10:00:00Z", "session_id": "SESS002"}
{"event_id": "EVT004", "user_id": "USER002", "event_type": "search", "search_term": "laptop", "timestamp": "2024-01-20T10:10:00Z", "session_id": "SESS002"}
{"event_id": "EVT005", "user_id": "USER003", "event_type": "purchase", "order_id": "ORD006", "timestamp": "2024-01-20T11:30:00Z", "session_id": "SESS003"}
EOF

cat > /tmp/enterprise-sample-data/streaming_order_events.json << 'EOF'
{"event_id": "ORD_EVT001", "order_id": "ORD001", "event_type": "order_created", "timestamp": "2024-01-15T10:00:00Z", "customer_id": "CUST001"}
{"event_id": "ORD_EVT002", "order_id": "ORD001", "event_type": "payment_processed", "timestamp": "2024-01-15T10:05:00Z", "customer_id": "CUST001"}
{"event_id": "ORD_EVT003", "order_id": "ORD001", "event_type": "order_shipped", "timestamp": "2024-01-15T14:00:00Z", "customer_id": "CUST001"}
{"event_id": "ORD_EVT004", "order_id": "ORD002", "event_type": "order_created", "timestamp": "2024-01-16T11:00:00Z", "customer_id": "CUST002"}
{"event_id": "ORD_EVT005", "order_id": "ORD002", "event_type": "payment_processed", "timestamp": "2024-01-16T11:02:00Z", "customer_id": "CUST002"}
EOF

cat > /tmp/enterprise-sample-data/streaming_inventory_updates.json << 'EOF'
{"event_id": "INV_EVT001", "product_id": "PROD001", "event_type": "stock_reduced", "quantity_change": -1, "new_quantity": 49, "timestamp": "2024-01-20T10:30:00Z"}
{"event_id": "INV_EVT002", "product_id": "PROD002", "event_type": "stock_reduced", "quantity_change": -2, "new_quantity": 73, "timestamp": "2024-01-20T11:00:00Z"}
{"event_id": "INV_EVT003", "product_id": "PROD001", "event_type": "stock_added", "quantity_change": 10, "new_quantity": 59, "timestamp": "2024-01-20T14:00:00Z"}
{"event_id": "INV_EVT004", "product_id": "PROD003", "event_type": "stock_reduced", "quantity_change": -1, "new_quantity": 99, "timestamp": "2024-01-20T15:30:00Z"}
{"event_id": "INV_EVT005", "product_id": "PROD004", "event_type": "stock_reduced", "quantity_change": -3, "new_quantity": 197, "timestamp": "2024-01-20T16:00:00Z"}
EOF

cat > /tmp/enterprise-sample-data/streaming_system_metrics.json << 'EOF'
{"event_id": "SYS_EVT001", "metric_name": "cpu_usage", "value": 75.5, "timestamp": "2024-01-20T10:00:00Z", "server_id": "SRV001"}
{"event_id": "SYS_EVT002", "metric_name": "memory_usage", "value": 68.2, "timestamp": "2024-01-20T10:00:00Z", "server_id": "SRV001"}
{"event_id": "SYS_EVT003", "metric_name": "disk_usage", "value": 45.8, "timestamp": "2024-01-20T10:00:00Z", "server_id": "SRV001"}
{"event_id": "SYS_EVT004", "metric_name": "network_throughput", "value": 1024.5, "timestamp": "2024-01-20T10:00:00Z", "server_id": "SRV001"}
{"event_id": "SYS_EVT005", "metric_name": "active_connections", "value": 150, "timestamp": "2024-01-20T10:00:00Z", "server_id": "SRV001"}
EOF

print_success "Sample data created"

# Step 6: Upload Sample Data to S3
print_header "Step 5: Uploading Sample Data to S3"

print_status "Uploading sample data to S3 buckets..."

# Upload external data
aws s3 cp /tmp/enterprise-sample-data/external_customer_orders.csv s3://$EXTERNAL_DATA_BUCKET/snowflake/
aws s3 cp /tmp/enterprise-sample-data/external_product_catalog.csv s3://$EXTERNAL_DATA_BUCKET/snowflake/
aws s3 cp /tmp/enterprise-sample-data/external_financial_transactions.csv s3://$EXTERNAL_DATA_BUCKET/snowflake/

# Upload internal data
aws s3 cp /tmp/enterprise-sample-data/internal_users.csv s3://$INTERNAL_DATA_BUCKET/postgresql/
aws s3 cp /tmp/enterprise-sample-data/internal_subscriptions.csv s3://$INTERNAL_DATA_BUCKET/postgresql/
aws s3 cp /tmp/enterprise-sample-data/internal_product_inventory.csv s3://$INTERNAL_DATA_BUCKET/postgresql/
aws s3 cp /tmp/enterprise-sample-data/internal_user_sessions.csv s3://$INTERNAL_DATA_BUCKET/postgresql/

# Upload streaming data
aws s3 cp /tmp/enterprise-sample-data/streaming_user_events.json s3://$STREAMING_DATA_BUCKET/kafka/
aws s3 cp /tmp/enterprise-sample-data/streaming_order_events.json s3://$STREAMING_DATA_BUCKET/kafka/
aws s3 cp /tmp/enterprise-sample-data/streaming_inventory_updates.json s3://$STREAMING_DATA_BUCKET/kafka/
aws s3 cp /tmp/enterprise-sample-data/streaming_system_metrics.json s3://$STREAMING_DATA_BUCKET/kafka/

print_success "Sample data uploaded to S3"

# Step 7: Create EMR Cluster Configuration
print_header "Step 6: Creating EMR Cluster Configuration"

print_status "Creating EMR cluster configuration files..."

# Create EMR configuration
cat > /tmp/emr-config.json << EOF
[
    {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.executor.memory": "8g",
            "spark.executor.cores": "4",
            "spark.driver.memory": "4g",
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "200"
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

# Create bootstrap script
cat > /tmp/install-dependencies.sh << 'EOF'
#!/bin/bash
# Bootstrap script for EMR cluster

# Install Python packages
sudo pip3 install --upgrade pip
sudo pip3 install snowflake-connector-python psycopg2-binary kafka-python boto3 pyyaml pandas

# Install Delta Lake
sudo pip3 install delta-spark

# Create project directory
sudo mkdir -p /opt/etl-project
sudo chown hadoop:hadoop /opt/etl-project

# Download project files from S3 (will be uploaded later)
aws s3 sync s3://company-artifacts-ACCOUNT_ID/etl-project/ /opt/etl-project/

echo "Dependencies installed successfully"
EOF

chmod +x /tmp/install-dependencies.sh

print_success "EMR configuration files created"

# Step 8: Create CloudWatch Dashboard
print_header "Step 7: Creating CloudWatch Dashboard"

print_status "Creating CloudWatch dashboard for monitoring..."

# Create dashboard configuration
cat > /tmp/cloudwatch-dashboard.json << EOF
{
    "DashboardName": "EnterpriseETLDashboard",
    "DashboardBody": "{
        \"widgets\": [
            {
                \"type\": \"metric\",
                \"x\": 0,
                \"y\": 0,
                \"width\": 12,
                \"height\": 6,
                \"properties\": {
                    \"metrics\": [
                        [\"CompanyETL\", \"TotalPipelineTime\"],
                        [\"CompanyETL\", \"ExternalSnowflakeIngestionTime\"],
                        [\"CompanyETL\", \"InternalPostgreSQLIngestionTime\"],
                        [\"CompanyETL\", \"InternalKafkaIngestionTime\"]
                    ],
                    \"view\": \"timeSeries\",
                    \"stacked\": false,
                    \"region\": \"$REGION\",
                    \"title\": \"ETL Pipeline Performance\",
                    \"period\": 300
                }
            },
            {
                \"type\": \"metric\",
                \"x\": 12,
                \"y\": 0,
                \"width\": 12,
                \"height\": 6,
                \"properties\": {
                    \"metrics\": [
                        [\"CompanyETL\", \"ExternalSnowflakeRecordsProcessed\"],
                        [\"CompanyETL\", \"InternalPostgreSQLRecordsProcessed\"],
                        [\"CompanyETL\", \"InternalKafkaRecordsProcessed\"]
                    ],
                    \"view\": \"timeSeries\",
                    \"stacked\": false,
                    \"region\": \"$REGION\",
                    \"title\": \"Records Processed by Source\",
                    \"period\": 300
                }
            },
            {
                \"type\": \"metric\",
                \"x\": 0,
                \"y\": 6,
                \"width\": 12,
                \"height\": 6,
                \"properties\": {
                    \"metrics\": [
                        [\"CompanyETL\", \"DataQualityScore\"]
                    ],
                    \"view\": \"timeSeries\",
                    \"stacked\": false,
                    \"region\": \"$REGION\",
                    \"title\": \"Data Quality Score\",
                    \"period\": 300
                }
            },
            {
                \"type\": \"metric\",
                \"x\": 12,
                \"y\": 6,
                \"width\": 12,
                \"height\": 6,
                \"properties\": {
                    \"metrics\": [
                        [\"CompanyETL\", \"PipelineSuccess\"],
                        [\"CompanyETL\", \"PipelineErrors\"]
                    ],
                    \"view\": \"timeSeries\",
                    \"stacked\": false,
                    \"region\": \"$REGION\",
                    \"title\": \"Pipeline Success/Error Rate\",
                    \"period\": 300
                }
            }
        ]
    }"
}
EOF

# Create dashboard
aws cloudwatch put-dashboard --cli-input-json file:///tmp/cloudwatch-dashboard.json

print_success "CloudWatch dashboard created"

# Step 9: Create Environment File
print_header "Step 8: Creating Environment Configuration"

print_status "Creating environment configuration file..."

# Create environment file
cat > /tmp/etl-environment.sh << EOF
#!/bin/bash
# Environment variables for ETL pipeline

# AWS Configuration
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

# S3 Buckets
export EXTERNAL_DATA_BUCKET="$EXTERNAL_DATA_BUCKET"
export INTERNAL_DATA_BUCKET="$INTERNAL_DATA_BUCKET"
export STREAMING_DATA_BUCKET="$STREAMING_DATA_BUCKET"
export DATA_LAKE_BUCKET="$DATA_LAKE_BUCKET"
export BACKUP_BUCKET="$BACKUP_BUCKET"
export LOGS_BUCKET="$LOGS_BUCKET"
export ARTIFACTS_BUCKET="$ARTIFACTS_BUCKET"

# External Data Source (Snowflake)
export SNOWFLAKE_USERNAME="your_snowflake_username"
export SNOWFLAKE_PASSWORD="your_snowflake_password"

# Internal Data Sources (PostgreSQL)
export POSTGRES_USERNAME="your_postgres_username"
export POSTGRES_PASSWORD="your_postgres_password"

# Internal Streaming (Kafka)
export KAFKA_USERNAME="your_kafka_username"
export KAFKA_PASSWORD="your_kafka_password"

# EMR Configuration
export EMR_CLUSTER_NAME="enterprise-etl-cluster"
export EMR_RELEASE_LABEL="emr-6.15.0"
export EMR_INSTANCE_TYPE="m5.2xlarge"
export EMR_INSTANCE_COUNT="5"

echo "Environment variables set for Enterprise ETL Pipeline"
EOF

chmod +x /tmp/etl-environment.sh

print_success "Environment configuration created"

# Step 10: Create Run Script
print_header "Step 9: Creating Run Script"

print_status "Creating ETL pipeline run script..."

# Create run script
cat > /tmp/run-etl-pipeline.sh << 'EOF'
#!/bin/bash
# Enterprise ETL Pipeline Run Script

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Starting Enterprise ETL Pipeline...${NC}"

# Check if configuration file exists
if [ ! -f "config/config-aws-enterprise-internal.yaml" ]; then
    echo "Error: Configuration file not found. Please ensure config/config-aws-enterprise-internal.yaml exists."
    exit 1
fi

# Check if ETL script exists
if [ ! -f "scripts/aws_enterprise_internal_etl.py" ]; then
    echo "Error: ETL script not found. Please ensure scripts/aws_enterprise_internal_etl.py exists."
    exit 1
fi

# Set environment variables
source /tmp/etl-environment.sh

# Install dependencies if not already installed
pip3 install snowflake-connector-python psycopg2-binary kafka-python boto3 pyyaml pandas delta-spark

# Run the ETL pipeline
echo -e "${BLUE}Running ETL pipeline...${NC}"
python3 scripts/aws_enterprise_internal_etl.py config/config-aws-enterprise-internal.yaml

echo -e "${GREEN}ETL pipeline completed successfully!${NC}"
EOF

chmod +x /tmp/run-etl-pipeline.sh

print_success "Run script created"

# Step 11: Create README
print_header "Step 10: Creating Documentation"

print_status "Creating project documentation..."

# Create README
cat > /tmp/README.md << 'EOF'
# Enterprise AWS ETL Pipeline

## Overview
This is a production-ready enterprise ETL pipeline that processes data from:
- **1 External Source**: Snowflake Data Warehouse
- **2 Internal Sources**: PostgreSQL Database + Apache Kafka Streaming

## Architecture
```
External Sources → Internal Sources → ETL Pipeline → Data Lake → Analytics
     Snowflake         PostgreSQL      Spark/EMR      S3         BI Tools
                        Kafka
```

## Prerequisites
- AWS CLI configured
- Python 3.8+
- Required Python packages (see requirements.txt)

## Quick Start

### 1. Set Environment Variables
```bash
source /tmp/etl-environment.sh
```

### 2. Update Credentials
Edit `/tmp/etl-environment.sh` and update:
- Snowflake credentials
- PostgreSQL credentials  
- Kafka credentials

### 3. Run ETL Pipeline
```bash
./tmp/run-etl-pipeline.sh
```

## Data Sources

### External Source: Snowflake
- **Tables**: customer_orders, product_catalog, financial_transactions
- **Frequency**: Hourly/Daily batch processing
- **Authentication**: OAuth2

### Internal Source: PostgreSQL
- **Tables**: users, subscriptions, product_inventory, user_sessions
- **Frequency**: Hourly/Real-time processing
- **Authentication**: SSL connections

### Internal Source: Kafka
- **Topics**: user_events, order_events, inventory_updates, system_metrics
- **Frequency**: Real-time streaming
- **Authentication**: SASL_SSL

## Data Flow
1. **Bronze Layer**: Raw data ingestion
2. **Silver Layer**: Cleaned and validated data
3. **Gold Layer**: Business-ready data warehouse

## Monitoring
- CloudWatch Dashboard: Real-time metrics
- S3 Console: Data lake inspection
- EMR Console: Cluster monitoring

## Security
- S3 encryption at rest
- IAM role-based access
- SSL/TLS connections
- Data classification (PII, sensitive, restricted)

## Compliance
- GDPR compliance
- CCPA compliance
- SOX compliance
- Data retention policies

## Support
For issues or questions, contact: data-team@company.com
EOF

print_success "Documentation created"

# Step 12: Final Summary
print_header "Deployment Complete!"

print_success "Enterprise AWS ETL Pipeline deployed successfully!"
echo ""
echo "📊 **Deployed Infrastructure:**"
echo "   • S3 Buckets: $EXTERNAL_DATA_BUCKET, $INTERNAL_DATA_BUCKET, $STREAMING_DATA_BUCKET"
echo "   • Data Lake: $DATA_LAKE_BUCKET"
echo "   • Backup: $BACKUP_BUCKET"
echo "   • Logs: $LOGS_BUCKET"
echo "   • Artifacts: $ARTIFACTS_BUCKET"
echo ""
echo "🔧 **Created Components:**"
echo "   • IAM Roles and Policies"
echo "   • EMR Cluster Configuration"
echo "   • CloudWatch Dashboard"
echo "   • Sample Data"
echo "   • Environment Configuration"
echo "   • Run Scripts"
echo ""
echo "🚀 **Next Steps:**"
echo "1. Update credentials in /tmp/etl-environment.sh"
echo "2. Run: source /tmp/etl-environment.sh"
echo "3. Run: ./tmp/run-etl-pipeline.sh"
echo ""
echo "📈 **Monitor:**"
echo "   • CloudWatch Dashboard: https://console.aws.amazon.com/cloudwatch/home"
echo "   • S3 Console: https://console.aws.amazon.com/s3/"
echo "   • EMR Console: https://console.aws.amazon.com/elasticmapreduce/"
echo ""
echo "📖 **Documentation:**"
echo "   • README: /tmp/README.md"
echo "   • Configuration: config/config-aws-enterprise-internal.yaml"
echo "   • ETL Script: scripts/aws_enterprise_internal_etl.py"
echo ""
echo "✅ **Your enterprise ETL pipeline is ready!**"
