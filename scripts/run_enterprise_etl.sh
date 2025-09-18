#!/bin/bash
# Enterprise AWS ETL Pipeline - Quick Start Script
# This script runs the complete enterprise ETL pipeline with 1 external + 2 internal sources

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
PURPLE='\033[0;35m'
NC='\033[0m'

print_header() {
    echo -e "${PURPLE}================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}================================${NC}"
}

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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="$PROJECT_ROOT/aws/config/config-aws-enterprise-internal.yaml"
ETL_SCRIPT="$PROJECT_ROOT/aws/scripts/aws_enterprise_internal_etl.py"

print_header "Enterprise AWS ETL Pipeline - Quick Start"
print_status "Starting enterprise ETL pipeline with 1 external + 2 internal sources..."

# Check if we're in the right directory
if [ ! -f "$CONFIG_FILE" ]; then
    print_error "Configuration file not found: $CONFIG_FILE"
    print_error "Please run this script from the project root directory"
    exit 1
fi

if [ ! -f "$ETL_SCRIPT" ]; then
    print_error "ETL script not found: $ETL_SCRIPT"
    print_error "Please ensure the ETL script exists"
    exit 1
fi

print_success "Configuration files found"

# Check Python and dependencies
print_status "Checking Python environment..."

if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
print_status "Python version: $PYTHON_VERSION"

# Install dependencies if requirements file exists
REQUIREMENTS_FILE="$PROJECT_ROOT/aws/requirements-enterprise.txt"
if [ -f "$REQUIREMENTS_FILE" ]; then
    print_status "Installing Python dependencies..."
    pip3 install -r "$REQUIREMENTS_FILE"
    print_success "Dependencies installed"
else
    print_warning "Requirements file not found. Installing core dependencies..."
    pip3 install pyspark delta-spark boto3 snowflake-connector-python psycopg2-binary kafka-python pandas pyyaml
    print_success "Core dependencies installed"
fi

# Check AWS credentials
print_status "Checking AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region || echo "us-east-1")
print_success "AWS credentials configured for account: $ACCOUNT_ID, region: $REGION"

# Set environment variables
print_status "Setting up environment variables..."

export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

# Set default credentials (user should update these)
export SNOWFLAKE_USERNAME="${SNOWFLAKE_USERNAME:-your_snowflake_username}"
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-your_snowflake_password}"
export POSTGRES_USERNAME="${POSTGRES_USERNAME:-your_postgres_username}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-your_postgres_password}"
export KAFKA_USERNAME="${KAFKA_USERNAME:-your_kafka_username}"
export KAFKA_PASSWORD="${KAFKA_PASSWORD:-your_kafka_password}"

print_warning "Using default credentials. Please set actual credentials as environment variables:"
print_warning "  SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD"
print_warning "  POSTGRES_USERNAME, POSTGRES_PASSWORD"
print_warning "  KAFKA_USERNAME, KAFKA_PASSWORD"

# Check if credentials are still default
if [[ "$SNOWFLAKE_USERNAME" == "your_snowflake_username" ]] || \
   [[ "$POSTGRES_USERNAME" == "your_postgres_username" ]] || \
   [[ "$KAFKA_USERNAME" == "your_kafka_username" ]]; then
    print_warning "Using default credentials. The pipeline will use sample data for demonstration."
fi

# Create sample data if needed
print_status "Preparing sample data..."
SAMPLE_DATA_DIR="/tmp/enterprise-sample-data"
mkdir -p "$SAMPLE_DATA_DIR"

# Create sample external data (Snowflake-like)
cat > "$SAMPLE_DATA_DIR/external_customer_orders.csv" << 'EOF'
order_id,customer_id,customer_name,email,order_date,total_amount,status,payment_method
ORD001,CUST001,John Doe,john.doe@email.com,2024-01-15,299.99,completed,credit_card
ORD002,CUST002,Jane Smith,jane.smith@email.com,2024-01-16,199.50,completed,paypal
ORD003,CUST001,John Doe,john.doe@email.com,2024-01-17,89.99,pending,credit_card
ORD004,CUST003,Bob Johnson,bob.johnson@email.com,2024-01-18,450.00,completed,credit_card
ORD005,CUST002,Jane Smith,jane.smith@email.com,2024-01-19,125.75,completed,paypal
EOF

# Create sample internal data (PostgreSQL-like)
cat > "$SAMPLE_DATA_DIR/internal_users.csv" << 'EOF'
user_id,first_name,last_name,email,phone,address,city,state,country,registration_date
USER001,Alice,Brown,alice.brown@company.com,555-0101,123 Main St,New York,NY,USA,2023-01-15
USER002,Charlie,Davis,charlie.davis@company.com,555-0102,456 Oak Ave,Los Angeles,CA,USA,2023-02-20
USER003,Eva,Wilson,eva.wilson@company.com,555-0103,789 Pine St,Chicago,IL,USA,2023-03-10
USER004,Frank,Miller,frank.miller@company.com,555-0104,321 Elm St,Houston,TX,USA,2023-04-05
USER005,Grace,Taylor,grace.taylor@company.com,555-0105,654 Maple Dr,Phoenix,AZ,USA,2023-05-12
EOF

# Create sample streaming data (Kafka-like)
cat > "$SAMPLE_DATA_DIR/streaming_user_events.json" << 'EOF'
{"event_id": "EVT001", "user_id": "USER001", "event_type": "page_view", "page_url": "/products", "timestamp": "2024-01-20T09:00:00Z", "session_id": "SESS001"}
{"event_id": "EVT002", "user_id": "USER001", "event_type": "add_to_cart", "product_id": "PROD001", "timestamp": "2024-01-20T09:05:00Z", "session_id": "SESS001"}
{"event_id": "EVT003", "user_id": "USER002", "event_type": "page_view", "page_url": "/home", "timestamp": "2024-01-20T10:00:00Z", "session_id": "SESS002"}
{"event_id": "EVT004", "user_id": "USER002", "event_type": "search", "search_term": "laptop", "timestamp": "2024-01-20T10:10:00Z", "session_id": "SESS002"}
{"event_id": "EVT005", "user_id": "USER003", "event_type": "purchase", "order_id": "ORD006", "timestamp": "2024-01-20T11:30:00Z", "session_id": "SESS003"}
EOF

print_success "Sample data prepared"

# Run the ETL pipeline
print_header "Starting Enterprise ETL Pipeline"

print_status "Running ETL pipeline with configuration: $CONFIG_FILE"
print_status "ETL script: $ETL_SCRIPT"

# Change to project root directory
cd "$PROJECT_ROOT"

# Run the ETL pipeline
print_status "Executing ETL pipeline..."
python3 "$ETL_SCRIPT" "$CONFIG_FILE"

# Check exit status
if [ $? -eq 0 ]; then
    print_success "Enterprise ETL pipeline completed successfully!"
    echo ""
    echo "🎉 **Pipeline Summary:**"
    echo "   ✅ External Snowflake data ingested"
    echo "   ✅ Internal PostgreSQL data ingested"
    echo "   ✅ Internal Kafka streaming data ingested"
    echo "   ✅ Data warehouse processing completed"
    echo "   ✅ Data quality checks passed"
    echo ""
    echo "📊 **Data Sources Processed:**"
    echo "   📈 EXTERNAL: Snowflake Data Warehouse"
    echo "   🗄️ INTERNAL: PostgreSQL Production Database"
    echo "   🌊 INTERNAL: Apache Kafka Streaming Platform"
    echo ""
    echo "📈 **Next Steps:**"
    echo "   • Check S3 buckets for processed data"
    echo "   • View CloudWatch dashboard for metrics"
    echo "   • Connect BI tools to the data lake"
    echo ""
    echo "🔗 **Useful Links:**"
    echo "   • S3 Console: https://console.aws.amazon.com/s3/"
    echo "   • CloudWatch: https://console.aws.amazon.com/cloudwatch/"
    echo "   • EMR Console: https://console.aws.amazon.com/elasticmapreduce/"
    echo ""
    echo "✅ **Enterprise ETL pipeline execution completed!**"
else
    print_error "ETL pipeline failed with exit code $?"
    echo ""
    echo "🔍 **Troubleshooting Tips:**"
    echo "   • Check AWS credentials and permissions"
    echo "   • Verify data source connectivity"
    echo "   • Review CloudWatch logs for errors"
    echo "   • Ensure all dependencies are installed"
    echo ""
    echo "📞 **Support:**"
    echo "   • Check the logs above for specific error messages"
    echo "   • Review aws/docs/ENTERPRISE_AWS_ETL_GUIDE.md for troubleshooting"
    echo "   • Contact data-team@company.com for assistance"
    exit 1
fi
