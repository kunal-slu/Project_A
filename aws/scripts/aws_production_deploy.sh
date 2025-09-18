#!/bin/bash
# AWS Production ETL Pipeline Deployment Script
# This script deploys a complete production ETL pipeline with real-world data sources

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
DATA_LAKE_BUCKET="${COMPANY_NAME}-data-lake-${ACCOUNT_ID}"
BACKUP_BUCKET="${COMPANY_NAME}-backups-${ACCOUNT_ID}"
LOGS_BUCKET="${COMPANY_NAME}-logs-${ACCOUNT_ID}"
ARTIFACTS_BUCKET="${COMPANY_NAME}-artifacts-${ACCOUNT_ID}"
RAW_DATA_BUCKET="${COMPANY_NAME}-raw-data-${ACCOUNT_ID}"

print_header "AWS Production ETL Pipeline Deployment"
print_status "Starting production deployment for real-world ETL pipeline..."

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
print_header "Step 1: Creating S3 Infrastructure"

print_status "Creating S3 buckets..."
aws s3 mb s3://$DATA_LAKE_BUCKET --region $REGION
aws s3 mb s3://$BACKUP_BUCKET --region $REGION
aws s3 mb s3://$LOGS_BUCKET --region $REGION
aws s3 mb s3://$ARTIFACTS_BUCKET --region $REGION
aws s3 mb s3://$RAW_DATA_BUCKET --region $REGION

print_success "S3 buckets created successfully"

# Step 2: Create S3 Folder Structure
print_status "Creating S3 folder structure..."

# Data lake structure
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key metrics/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key checkpoints/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key warehouse/

# Bronze layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/orders/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/customers/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/products/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/payments/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/analytics/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/marketing/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key bronze/support/

# Silver layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/orders/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/customers/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key silver/analytics/

# Gold layer subdirectories
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/fact_sales/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/dim_customers/
aws s3api put-object --bucket $DATA_LAKE_BUCKET --key gold/marketing_attribution/

# Raw data structure (simulating external data sources)
aws s3api put-object --bucket $RAW_DATA_BUCKET --key orders/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key customers/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key products/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key transactions/

# External data sources (simulated)
aws s3api put-object --bucket $RAW_DATA_BUCKET --key stripe-export/charges/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key paypal-export/transactions/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key ga4-export/events/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key firebase-analytics-export/events/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key google-ads-export/campaigns/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key facebook-ads-export/insights/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key zendesk-export/tickets/
aws s3api put-object --bucket $RAW_DATA_BUCKET --key intercom-export/conversations/

print_success "S3 folder structure created"

# Step 3: Create IAM Roles and Policies
print_header "Step 2: Creating IAM Infrastructure"

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

# Create custom policy for S3 access
aws iam create-policy --policy-name ETLDataLakeAccess --policy-document '{
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
        "arn:aws:s3:::company-data-lake-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-data-lake-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-raw-data-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-raw-data-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-backups-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-backups-'$ACCOUNT_ID'/*",
        "arn:aws:s3:::company-logs-'$ACCOUNT_ID'",
        "arn:aws:s3:::company-logs-'$ACCOUNT_ID'/*"
      ]
    }
  ]
}' 2>/dev/null || print_warning "ETLDataLakeAccess policy already exists"

# Attach custom policy to EMR EC2 role
aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/ETLDataLakeAccess

print_success "IAM roles and policies created"

# Step 4: Upload Sample Data
print_header "Step 3: Uploading Sample Data"

print_status "Uploading sample data to simulate real-world data sources..."

# Create sample data files
mkdir -p /tmp/etl-sample-data

# Sample orders data (JSON)
cat > /tmp/etl-sample-data/sample_orders.json << 'EOF'
{"order_id": "ORD001", "customer_id": "CUST001", "order_date": "2024-01-15", "total_amount": 299.99, "status": "completed", "payment_method": "credit_card", "shipping_address": "123 Main St", "items": [{"product_id": "PROD001", "quantity": 2, "price": 149.99}]}
{"order_id": "ORD002", "customer_id": "CUST002", "order_date": "2024-01-16", "total_amount": 199.50, "status": "completed", "payment_method": "paypal", "shipping_address": "456 Oak Ave", "items": [{"product_id": "PROD002", "quantity": 1, "price": 199.50}]}
{"order_id": "ORD003", "customer_id": "CUST001", "order_date": "2024-01-17", "total_amount": 89.99, "status": "pending", "payment_method": "credit_card", "shipping_address": "123 Main St", "items": [{"product_id": "PROD003", "quantity": 1, "price": 89.99}]}
EOF

# Sample customers data (CSV)
cat > /tmp/etl-sample-data/sample_customers.csv << 'EOF'
customer_id,first_name,last_name,email,phone,address,city,state,country,zip_code,registration_date
CUST001,John,Doe,john.doe@email.com,555-0101,123 Main St,New York,NY,USA,10001,2023-01-15
CUST002,Jane,Smith,jane.smith@email.com,555-0102,456 Oak Ave,Los Angeles,CA,USA,90210,2023-02-20
CUST003,Bob,Johnson,bob.johnson@email.com,555-0103,789 Pine St,Chicago,IL,USA,60601,2023-03-10
EOF

# Sample products data (Parquet format will be created by Spark)
cat > /tmp/etl-sample-data/sample_products.csv << 'EOF'
product_id,name,category,price,description
PROD001,Wireless Headphones,Electronics,149.99,High-quality wireless headphones
PROD002,Smart Watch,Electronics,199.50,Fitness tracking smartwatch
PROD003,Bluetooth Speaker,Electronics,89.99,Portable bluetooth speaker
PROD004,Running Shoes,Sports,129.99,Comfortable running shoes
PROD005,Yoga Mat,Sports,29.99,Non-slip yoga mat
EOF

# Sample Stripe payments data (JSON)
cat > /tmp/etl-sample-data/sample_stripe_charges.json << 'EOF'
{"id": "ch_001", "amount": 29999, "currency": "usd", "status": "succeeded", "customer": "cus_001", "created": 1705276800, "payment_method": "pm_001"}
{"id": "ch_002", "amount": 19950, "currency": "usd", "status": "succeeded", "customer": "cus_002", "created": 1705363200, "payment_method": "pm_002"}
{"id": "ch_003", "amount": 8999, "currency": "usd", "status": "pending", "customer": "cus_001", "created": 1705449600, "payment_method": "pm_001"}
EOF

# Sample Google Analytics data (JSON)
cat > /tmp/etl-sample-data/sample_ga4_events.json << 'EOF'
{"user_pseudo_id": "user_001", "session_id": "session_001", "event_name": "page_view", "event_timestamp": "2024-01-15T10:00:00Z", "page_location": "/products"}
{"user_pseudo_id": "user_001", "session_id": "session_001", "event_name": "add_to_cart", "event_timestamp": "2024-01-15T10:05:00Z", "product_id": "PROD001"}
{"user_pseudo_id": "user_002", "session_id": "session_002", "event_name": "page_view", "event_timestamp": "2024-01-16T14:30:00Z", "page_location": "/home"}
{"user_pseudo_id": "user_002", "session_id": "session_002", "event_name": "purchase", "event_timestamp": "2024-01-16T14:45:00Z", "transaction_id": "ORD002"}
EOF

# Upload sample data to S3
aws s3 cp /tmp/etl-sample-data/sample_orders.json s3://$RAW_DATA_BUCKET/orders/
aws s3 cp /tmp/etl-sample-data/sample_customers.csv s3://$RAW_DATA_BUCKET/customers/
aws s3 cp /tmp/etl-sample-data/sample_products.csv s3://$RAW_DATA_BUCKET/products/
aws s3 cp /tmp/etl-sample-data/sample_stripe_charges.json s3://$RAW_DATA_BUCKET/stripe-export/charges/
aws s3 cp /tmp/etl-sample-data/sample_ga4_events.json s3://$RAW_DATA_BUCKET/ga4-export/events/

print_success "Sample data uploaded successfully"

# Step 5: Update Configuration Files
print_header "Step 4: Configuring ETL Pipeline"

print_status "Updating configuration files with real account IDs..."

# Update the production config file
sed -i.bak "s/ACCOUNT_ID/$ACCOUNT_ID/g" config/config-aws-prod.yaml

print_success "Configuration files updated"

# Step 6: Upload Project Code
print_status "Uploading project code to S3..."

# Create deployment package
zip -r pyspark-etl-production.zip src/ config/ requirements.txt scripts/aws_production_etl.py

# Upload to S3
aws s3 cp pyspark-etl-production.zip s3://$ARTIFACTS_BUCKET/code/
aws s3 cp config/config-aws-prod.yaml s3://$ARTIFACTS_BUCKET/config/
aws s3 cp scripts/aws_production_etl.py s3://$ARTIFACTS_BUCKET/scripts/

print_success "Project code uploaded"

# Step 7: Create EMR Cluster Configuration
print_status "Creating EMR cluster configuration..."

cat > config/emr-production-config.json << EOF
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

# Step 8: Create Bootstrap Script
print_status "Creating bootstrap script..."

cat > scripts/install-production-dependencies.sh << 'EOF'
#!/bin/bash

# Install Delta Lake and other dependencies
sudo pip3 install delta-spark==3.0.0
sudo pip3 install structlog==23.2.0 pyyaml==6.0.1 prometheus-client==0.19.0
sudo pip3 install boto3==1.34.0

# Download project code
aws s3 cp s3://company-artifacts-ACCOUNT_ID/code/pyspark-etl-production.zip /tmp/
unzip /tmp/pyspark-etl-production.zip -d /home/hadoop/

# Set permissions
chmod +x /home/hadoop/scripts/aws_production_etl.py

# Create environment variables
echo "export PYTHONPATH=/home/hadoop/src:$PYTHONPATH" >> /home/hadoop/.bashrc
echo "export AWS_DEFAULT_REGION=us-east-1" >> /home/hadoop/.bashrc

echo "Production dependencies installed successfully"
EOF

# Replace ACCOUNT_ID in bootstrap script
sed -i "s/ACCOUNT_ID/$ACCOUNT_ID/g" scripts/install-production-dependencies.sh

# Upload bootstrap script
aws s3 cp scripts/install-production-dependencies.sh s3://$ARTIFACTS_BUCKET/bootstrap/

print_success "Bootstrap script created and uploaded"

# Step 9: Create EMR Cluster
print_header "Step 5: Creating EMR Cluster"

print_status "Creating production EMR cluster..."

CLUSTER_ID=$(aws emr create-cluster \
  --name "production-etl-cluster" \
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
    Path="s3://$ARTIFACTS_BUCKET/bootstrap/install-production-dependencies.sh" \
  --configurations file://config/emr-production-config.json \
  --auto-terminate \
  --query 'ClusterId' --output text)

print_success "EMR cluster creation initiated. Cluster ID: $CLUSTER_ID"

# Step 10: Create CloudWatch Dashboard
print_header "Step 6: Setting Up Monitoring"

print_status "Creating CloudWatch dashboard..."

cat > config/cloudwatch-production-dashboard.json << EOF
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
        "title": "ETL Pipeline Performance"
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
      "type": "log",
      "x": 0,
      "y": 6,
      "width": 24,
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
  --dashboard-name "Production-ETL-Dashboard" \
  --dashboard-body file://config/cloudwatch-production-dashboard.json

print_success "CloudWatch dashboard created"

# Step 11: Create Athena Database and Tables
print_header "Step 7: Setting Up Analytics"

print_status "Creating Athena database and tables..."

# Create Athena database
aws athena start-query-execution \
  --query-string "CREATE DATABASE IF NOT EXISTS company_data_warehouse" \
  --result-configuration OutputLocation=s3://$LOGS_BUCKET/athena-results/ \
  --query-execution-context Database=default

# Wait for database creation
sleep 10

# Create external tables
aws athena start-query-execution \
  --query-string "
  CREATE EXTERNAL TABLE bronze_orders (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    total_amount DOUBLE,
    status STRING,
    payment_method STRING,
    shipping_address STRING,
    items STRING,
    ingested_at TIMESTAMP
  )
  PARTITIONED BY (year INT, month INT, day INT)
  STORED AS PARQUET
  LOCATION 's3://$DATA_LAKE_BUCKET/bronze/orders/';
  
  CREATE EXTERNAL TABLE bronze_customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    zip_code STRING,
    registration_date STRING,
    ingested_at TIMESTAMP
  )
  PARTITIONED BY (year INT, month INT, day INT)
  STORED AS PARQUET
  LOCATION 's3://$DATA_LAKE_BUCKET/bronze/customers/';
  
  CREATE EXTERNAL TABLE gold_fact_sales (
    order_id STRING,
    customer_id STRING,
    customer_name STRING,
    order_date DATE,
    total_amount DOUBLE,
    status STRING,
    payment_method STRING,
    country STRING,
    state STRING,
    city STRING,
    order_year INT,
    order_month INT,
    order_day INT
  )
  PARTITIONED BY (order_year INT, order_month INT)
  STORED AS PARQUET
  LOCATION 's3://$DATA_LAKE_BUCKET/gold/fact_sales/';
  " \
  --result-configuration OutputLocation=s3://$LOGS_BUCKET/athena-results/ \
  --query-execution-context Database=company_data_warehouse

print_success "Athena database and tables created"

# Step 12: Display Summary
print_header "Deployment Summary"

print_success "AWS Production ETL Pipeline deployment completed successfully!"
echo ""
echo "📋 Production Deployment Summary:"
echo "================================"
echo "Account ID: $ACCOUNT_ID"
echo "Region: $REGION"
echo "EMR Cluster ID: $CLUSTER_ID"
echo ""
echo "📁 S3 Buckets:"
echo "- Data Lake: s3://$DATA_LAKE_BUCKET"
echo "- Raw Data: s3://$RAW_DATA_BUCKET"
echo "- Backups: s3://$BACKUP_BUCKET"
echo "- Logs: s3://$LOGS_BUCKET"
echo "- Artifacts: s3://$ARTIFACTS_BUCKET"
echo ""
echo "🔧 AWS Services:"
echo "- EMR Cluster: production-etl-cluster"
echo "- CloudWatch: Production-ETL-Dashboard"
echo "- Athena: company_data_warehouse"
echo ""
echo "📊 Real-World Data Sources:"
echo "- E-commerce: Orders, Customers, Products"
echo "- Payments: Stripe, PayPal"
echo "- Analytics: Google Analytics 4, Firebase"
echo "- Marketing: Google Ads, Facebook Ads"
echo "- Support: Zendesk, Intercom"
echo ""
echo "🚀 Next Steps:"
echo "1. Monitor EMR cluster creation in AWS Console"
echo "2. Run the production ETL pipeline:"
echo "   python scripts/aws_production_etl.py config/config-aws-prod.yaml"
echo "3. Check CloudWatch dashboard for monitoring"
echo "4. Query data using Amazon Athena"
echo "5. Set up automated scheduling with Step Functions"
echo ""
echo "📖 Documentation:"
echo "- AWS Deployment Guide: AWS_DEPLOYMENT_GUIDE.md"
echo "- Real-World Data Sources: AWS_REAL_WORLD_DATA_SOURCES.md"
echo ""
echo "🎯 Your production ETL pipeline is ready for real-world data processing!"
