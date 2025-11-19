#!/bin/bash
# Setup Local Airflow - Variables and Connections
# Run this after starting Airflow with docker-compose

set -e

echo "=========================================="
echo "Setting up Local Airflow"
echo "=========================================="

# Check if Airflow is running
if ! docker ps | grep -q airflow-webserver; then
    echo "❌ Airflow webserver not running. Start it first:"
    echo "   docker compose -f docker-compose-airflow.yml up -d"
    exit 1
fi

echo "✅ Airflow is running"

# Get container name
WEBSERVER_CONTAINER=$(docker ps --format "{{.Names}}" | grep airflow-webserver | head -1)

if [ -z "$WEBSERVER_CONTAINER" ]; then
    echo "❌ Could not find Airflow webserver container"
    exit 1
fi

echo "Using container: $WEBSERVER_CONTAINER"

# Set Airflow Variables
echo ""
echo "📝 Setting Airflow Variables..."

docker exec -it "$WEBSERVER_CONTAINER" airflow variables set PROJECT_A_ENV dev
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set PROJECT_A_CONFIG_URI "s3://my-etl-artifacts-demo-424570854632/config/dev.yaml"
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set PROJECT_A_EMR_APP_ID "00g0tm6kccmdcf09"
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set PROJECT_A_EXEC_ROLE_ARN "arn:aws:iam::424570854632:role/project-a-dev-emr-exec"
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set PROJECT_A_LAKE_BUCKET "my-etl-lake-demo-424570854632"
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set PROJECT_A_ARTIFACTS_BUCKET "my-etl-artifacts-demo-424570854632"

# Legacy variable names (for backward compatibility)
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set emr_app_id "00g0tm6kccmdcf09"
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set emr_exec_role_arn "arn:aws:iam::424570854632:role/project-a-dev-emr-exec"
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set artifacts_bucket "my-etl-artifacts-demo-424570854632"
docker exec -it "$WEBSERVER_CONTAINER" airflow variables set project_a_env "dev"

echo "✅ Variables set"

# Set AWS Connection
echo ""
echo "🔗 Setting AWS Connection..."

# Check if connection exists
if docker exec "$WEBSERVER_CONTAINER" airflow connections get aws_default > /dev/null 2>&1; then
    echo "⚠️  aws_default connection exists, deleting..."
    docker exec "$WEBSERVER_CONTAINER" airflow connections delete aws_default
fi

# Create AWS connection
# For local Docker, we'll use the host's AWS credentials mounted at ~/.aws
docker exec "$WEBSERVER_CONTAINER" airflow connections add aws_default \
    --conn-type aws \
    --conn-extra "{\"region_name\": \"us-east-1\"}"

echo "✅ AWS connection configured"

# Verify setup
echo ""
echo "🔍 Verifying setup..."

echo "Variables:"
docker exec "$WEBSERVER_CONTAINER" airflow variables list | grep -E "PROJECT_A_|emr_app_id|artifacts_bucket" || true

echo ""
echo "Connections:"
docker exec "$WEBSERVER_CONTAINER" airflow connections list | grep aws_default || true

echo ""
echo "=========================================="
echo "✅ Airflow setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Access Airflow UI: http://localhost:8080"
echo "2. Login: airflow / airflow"
echo "3. Check DAGs are visible"
echo "4. Trigger a DAG to test"

