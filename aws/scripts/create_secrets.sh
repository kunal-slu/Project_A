#!/bin/bash
# Phase 2: Create Secrets Manager Entries
# Creates all required secrets for ETL pipeline

set -e

AWS_PROFILE="${AWS_PROFILE:-kunal21}"
AWS_REGION="${AWS_REGION:-us-east-1}"
KMS_KEY_ARN="${KMS_KEY_ARN}"  # Must be provided or read from terraform outputs

if [ -z "$KMS_KEY_ARN" ]; then
    echo "❌ KMS_KEY_ARN not set. Reading from terraform-outputs.dev.json..."
    if [ -f "aws/terraform/terraform-outputs.dev.json" ]; then
        KMS_KEY_ARN=$(jq -r '.kms_key_arn.value' aws/terraform/terraform-outputs.dev.json)
        echo "✅ Found KMS ARN: $KMS_KEY_ARN"
    else
        echo "❌ terraform-outputs.dev.json not found. Please provide KMS_KEY_ARN"
        exit 1
    fi
fi

SECRET_PREFIX="project-a-dev"

echo "🔐 Creating Secrets Manager entries..."
echo "   Profile: $AWS_PROFILE"
echo "   Region: $AWS_REGION"
echo "   KMS Key: $KMS_KEY_ARN"
echo ""

# 1. Snowflake
echo "📝 Creating Snowflake secret..."
aws secretsmanager create-secret \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --name "${SECRET_PREFIX}/snowflake/conn" \
  --description "Snowflake connection credentials (dev)" \
  --kms-key-id "$KMS_KEY_ARN" \
  --secret-string '{
    "account": "YOUR_SNOWFLAKE_ACCOUNT",
    "user": "svc_projecta_dev",
    "password": "CHANGE_ME",
    "warehouse": "COMPUTE_WH",
    "database": "RAW",
    "schema": "PUBLIC",
    "role": "SYSADMIN"
  }' || echo "⚠️  Snowflake secret may already exist"

# 2. Redshift
echo "📝 Creating Redshift secret..."
aws secretsmanager create-secret \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --name "${SECRET_PREFIX}/redshift/conn" \
  --description "Redshift connection credentials (dev)" \
  --kms-key-id "$KMS_KEY_ARN" \
  --secret-string '{
    "host": "your-redshift-cluster.xxxxx.us-east-1.redshift.amazonaws.com",
    "port": 5439,
    "database": "dev",
    "user": "etl_user",
    "password": "CHANGE_ME"
  }' || echo "⚠️  Redshift secret may already exist"

# 3. Kafka
echo "📝 Creating Kafka secret..."
aws secretsmanager create-secret \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --name "${SECRET_PREFIX}/kafka/conn" \
  --description "Kafka connection credentials (dev)" \
  --kms-key-id "$KMS_KEY_ARN" \
  --secret-string '{
    "bootstrap_servers": "kafka-broker:9092",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "username": "kafka_user",
    "password": "CHANGE_ME"
  }' || echo "⚠️  Kafka secret may already exist"

# 4. Salesforce (optional)
echo "📝 Creating Salesforce secret..."
aws secretsmanager create-secret \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --name "${SECRET_PREFIX}/salesforce/conn" \
  --description "Salesforce API credentials (dev)" \
  --kms-key-id "$KMS_KEY_ARN" \
  --secret-string '{
    "username": "sf_user@example.com",
    "password": "CHANGE_ME",
    "security_token": "CHANGE_ME",
    "instance_url": "https://yourinstance.salesforce.com"
  }' || echo "⚠️  Salesforce secret may already exist"

# 5. FX Rates API (optional)
echo "📝 Creating FX Rates API secret..."
aws secretsmanager create-secret \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --name "${SECRET_PREFIX}/fx/conn" \
  --description "FX Rates API key (dev)" \
  --kms-key-id "$KMS_KEY_ARN" \
  --secret-string '{
    "api_key": "CHANGE_ME",
    "base_url": "https://api.exchangerate-api.com/v4"
  }' || echo "⚠️  FX secret may already exist"

echo ""
echo "✅ Secrets creation complete!"
echo ""
echo "⚠️  IMPORTANT: Update secret values with real credentials:"
echo "   aws secretsmanager update-secret --name ${SECRET_PREFIX}/snowflake/conn --secret-string '{\"account\":\"...\"}' --profile $AWS_PROFILE"
echo ""
echo "📋 List secrets:"
echo "   aws secretsmanager list-secrets --query \"SecretList[?starts_with(Name, '${SECRET_PREFIX}/')].Name\" --profile $AWS_PROFILE"

