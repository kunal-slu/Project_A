#!/usr/bin/env python3
"""
Helper script to copy jobs to S3 artifacts bucket for EMR execution
Run this before deploying jobs to EMR Serverless
"""
import boto3
import sys
from pathlib import Path

def upload_jobs_to_s3(artifacts_bucket: str, profile: str = "kunal21", region: str = "us-east-1"):
    """Upload all job files to S3 artifacts bucket."""
    s3 = boto3.Session(profile_name=profile, region_name=region).client('s3')
    
    repo_root = Path(__file__).parent.parent
    jobs_to_upload = [
        ("jobs/ingest/snowflake_to_bronze.py", "jobs/ingest/snowflake_to_bronze.py"),
        ("jobs/ingest/snowflake_customers_to_bronze.py", "jobs/ingest/snowflake_customers_to_bronze.py"),
        ("jobs/ingest/redshift_to_bronze.py", "jobs/ingest/redshift_to_bronze.py"),
        ("jobs/transform/bronze_to_silver.py", "jobs/transform/bronze_to_silver.py"),
        ("jobs/gold/dim_customer_scd2.py", "jobs/gold/dim_customer_scd2.py"),
        ("jobs/gold/star_schema.py", "jobs/gold/star_schema.py"),
        ("jobs/dq/dq_gate.py", "jobs/dq/dq_gate.py"),
        ("tests/dev_secret_probe.py", "jobs/dev_secret_probe.py"),
    ]
    
    # Also upload src directory (needed for imports)
    src_dir = repo_root / "src"
    
    print(f"📦 Uploading jobs to s3://{artifacts_bucket}/")
    
    for local_path, s3_key in jobs_to_upload:
        local_file = repo_root / local_path
        if local_file.exists():
            try:
                s3.upload_file(str(local_file), artifacts_bucket, s3_key)
                print(f"  ✅ {s3_key}")
            except Exception as e:
                print(f"  ❌ Failed to upload {s3_key}: {e}")
        else:
            print(f"  ⚠️  File not found: {local_path}")
    
    # Upload src directory recursively
    print(f"\n📦 Uploading src/ directory...")
    for py_file in src_dir.rglob("*.py"):
        relative_path = py_file.relative_to(repo_root)
        s3_key = str(relative_path).replace("\\", "/")
        try:
            s3.upload_file(str(py_file), artifacts_bucket, s3_key)
            print(f"  ✅ {s3_key}")
        except Exception as e:
            print(f"  ❌ Failed to upload {s3_key}: {e}")
    
    # Upload config files
    config_dir = repo_root / "config"
    print(f"\n📦 Uploading config/ directory...")
    for config_file in config_dir.glob("*.yaml"):
        s3_key = f"config/{config_file.name}"
        try:
            s3.upload_file(str(config_file), artifacts_bucket, s3_key)
            print(f"  ✅ {s3_key}")
        except Exception as e:
            print(f"  ❌ Failed to upload {s3_key}: {e}")
    
    print(f"\n✅ Upload complete!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python upload_jobs_to_s3.py <artifacts_bucket> [profile]")
        print("Example: python upload_jobs_to_s3.py my-etl-artifacts-demo-424570854632 kunal21")
        sys.exit(1)
    
    artifacts_bucket = sys.argv[1]
    profile = sys.argv[2] if len(sys.argv) > 2 else "kunal21"
    
    upload_jobs_to_s3(artifacts_bucket, profile)

