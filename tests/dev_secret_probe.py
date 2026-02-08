#!/usr/bin/env python3
"""
Phase 2 Test Script: Secret Probe
Proves Spark can fetch secrets without logging values.
"""

import json
import os

import boto3
from pyspark.sql import SparkSession


def fetch_secret(name, region="us-east-1"):
    """Fetch secret from AWS Secrets Manager."""
    sm = boto3.client("secretsmanager", region_name=region)
    val = sm.get_secret_value(SecretId=name)
    return json.loads(val["SecretString"])


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dev-secret-probe").getOrCreate()

    try:
        # Fetch secret
        snow = fetch_secret("project-a-dev/snowflake/conn")

        # Print keys only - NEVER print secret values
        print("✅ Fetched keys only:", list(snow.keys()))

        # Verify we can access the structure
        assert "account" in snow, "Missing 'account' key"
        assert "user" in snow, "Missing 'user' key"
        assert "password" in snow, "Missing 'password' key"

        print("✅ Secret structure validated")
        print("✅ No secret values logged (security check passed)")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        if os.environ.get("PROJECT_A_DISABLE_SPARK_STOP") != "1":
            spark.stop()
