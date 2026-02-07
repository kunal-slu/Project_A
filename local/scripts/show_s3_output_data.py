#!/usr/bin/env python3
"""
Show Sample Output Data from S3 (No Spark Required)

Uses boto3 and pandas to read and display sample data from S3 Silver and Gold layers.
"""
import sys
import boto3
import pandas as pd
import json
from pathlib import Path
from io import BytesIO
from typing import Dict, Any

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from project_a.config_loader import load_config_resolved
except ImportError:
    # Fallback: load config directly
    import yaml
    def load_config_resolved(path: str) -> Dict[str, Any]:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

def read_parquet_from_s3(s3_client, bucket: str, key: str, max_rows: int = 5) -> pd.DataFrame:
    """Read Parquet file from S3 and return as pandas DataFrame."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        return df.head(max_rows)
    except Exception as e:
        print(f"  ⚠️  Error reading {key}: {e}")
        return pd.DataFrame()

def list_s3_parquet_files(s3_client, bucket: str, prefix: str, max_files: int = 1) -> list:
    """List Parquet files in S3 prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [obj['Key'] for obj in response.get('Contents', []) 
                 if obj['Key'].endswith('.parquet') or obj['Key'].endswith('.snappy.parquet')]
        return files[:max_files]
    except Exception as e:
        print(f"  ⚠️  Error listing {prefix}: {e}")
        return []

def show_silver_data(config: Dict[str, Any]):
    """Show sample data from Silver layer."""
    print("=" * 80)
    print("📊 SILVER LAYER - SAMPLE DATA")
    print("=" * 80)
    
    s3_client = boto3.client('s3', region_name=config.get("aws", {}).get("region", "us-east-1"))
    silver_root = config["paths"]["silver_root"]
    
    # Parse S3 path
    bucket = silver_root.replace("s3://", "").split("/")[0]
    base_prefix = "/".join(silver_root.replace("s3://", "").split("/")[1:])
    
    tables = {
        "customers_silver": f"{base_prefix}/customers_silver",
        "orders_silver": f"{base_prefix}/orders_silver",
        "products_silver": f"{base_prefix}/products_silver",
    }
    
    for table_name, prefix in tables.items():
        print(f"\n🔍 {table_name}:")
        files = list_s3_parquet_files(s3_client, bucket, prefix, max_files=1)
        if files:
            df = read_parquet_from_s3(s3_client, bucket, files[0], max_rows=5)
            if not df.empty:
                print(f"  Rows in sample: {len(df)}")
                print(f"  Columns: {', '.join(df.columns[:10])}")
                print("\n  Sample data:")
                print(df.to_string(index=False))
            else:
                print("  ⚠️  No data found")
        else:
            print("  ⚠️  No Parquet files found")

def show_gold_data(config: Dict[str, Any]):
    """Show sample data from Gold layer."""
    print("\n" + "=" * 80)
    print("📊 GOLD LAYER - SAMPLE DATA")
    print("=" * 80)
    
    s3_client = boto3.client('s3', region_name=config.get("aws", {}).get("region", "us-east-1"))
    gold_root = config["paths"]["gold_root"]
    
    # Parse S3 path
    bucket = gold_root.replace("s3://", "").split("/")[0]
    base_prefix = "/".join(gold_root.replace("s3://", "").split("/")[1:])
    
    tables = {
        "fact_orders": f"{base_prefix}/fact_orders",
        "dim_customer": f"{base_prefix}/dim_customer",
        "dim_product": f"{base_prefix}/dim_product",
    }
    
    for table_name, prefix in tables.items():
        print(f"\n🔍 {table_name}:")
        files = list_s3_parquet_files(s3_client, bucket, prefix, max_files=1)
        if files:
            df = read_parquet_from_s3(s3_client, bucket, files[0], max_rows=5)
            if not df.empty:
                print(f"  Rows in sample: {len(df)}")
                print(f"  Columns: {', '.join(df.columns[:15])}")
                print("\n  Sample data:")
                print(df.to_string(index=False))
            else:
                print("  ⚠️  No data found")
        else:
            print("  ⚠️  No Parquet files found")

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Show sample output data from S3")
    parser.add_argument("--config", default="aws/config/dev.yaml", help="Config file path")
    parser.add_argument("--layer", choices=["silver", "gold", "all"], default="all")
    args = parser.parse_args()
    
    # Handle relative and absolute paths
    if Path(args.config).is_absolute():
        config_path = Path(args.config)
    else:
        config_path = PROJECT_ROOT / args.config
    
    config = load_config_resolved(str(config_path))
    
    if args.layer in ["silver", "all"]:
        show_silver_data(config)
    
    if args.layer in ["gold", "all"]:
        show_gold_data(config)
    
    print("\n" + "=" * 80)
    print("✅ Data display complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()

