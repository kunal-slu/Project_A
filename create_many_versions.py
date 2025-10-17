#!/usr/bin/env python3
"""
Simple script to create many Delta Lake versions
"""
import os
import pandas as pd
import json
from datetime import datetime

def create_many_versions():
    """Create many versions of Delta Lake tables"""
    
    # Target tables to create versions for
    tables = [
        ("bronze", "customers"),
        ("bronze", "orders"), 
        ("silver", "customers"),
        ("silver", "orders"),
        ("gold", "customer_analytics"),
        ("gold", "monthly_revenue"),
        ("gold", "order_analytics")
    ]
    
    for layer, table_name in tables:
        table_path = f"data/lakehouse_delta/{layer}/{table_name}"
        if not os.path.exists(table_path):
            continue
            
        print(f"🔄 Creating versions for {layer}/{table_name}...")
        
        # Read original data
        original_file = f"{table_path}/part-00000-{table_name}.parquet"
        if not os.path.exists(original_file):
            continue
            
        data = pd.read_parquet(original_file)
        
        # Create 5 versions
        for version in range(1, 6):
            # Create modified data
            new_data = data.copy()
            
            # Modify data based on table type
            if "customers" in table_name:
                # Add new customers
                new_records = data.head(10).copy()
                new_records['customer_id'] = range(len(data) + version*10 + 1, len(data) + (version+1)*10 + 1)
                new_records['name'] = [f'Version_{version}_{i}' for i in range(len(data) + version*10 + 1, len(data) + (version+1)*10 + 1)]
                new_data = pd.concat([data, new_records], ignore_index=True)
            elif "orders" in table_name:
                # Increase amounts
                if 'amount' in new_data.columns:
                    new_data['amount'] = new_data['amount'] * (1.1 ** version)
                elif 'total_amount' in new_data.columns:
                    new_data['total_amount'] = new_data['total_amount'] * (1.1 ** version)
            elif "analytics" in table_name or "revenue" in table_name:
                # Increase metrics
                for col in new_data.columns:
                    if col in ['total_revenue', 'revenue', 'total_amount', 'customer_count', 'order_count']:
                        if new_data[col].dtype in ['int64', 'float64']:
                            new_data[col] = new_data[col] * (1.05 ** version)
            
            # Write new version
            version_file = f"{table_path}/part-0000{version}-{table_name}-v{version+1}.parquet"
            new_data.to_parquet(version_file, index=False)
            
            # Create transaction log
            delta_log_dir = f"{table_path}/_delta_log"
            transaction_log = {
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                },
                "add": {
                    "path": f"part-0000{version}-{table_name}-v{version+1}.parquet",
                    "partitionValues": {},
                    "size": os.path.getsize(version_file),
                    "modificationTime": int(datetime.now().timestamp() * 1000) + version,
                    "dataChange": True,
                    "stats": json.dumps({
                        "numRecords": len(new_data),
                        "minValues": {},
                        "maxValues": {},
                        "nullCount": {}
                    })
                }
            }
            
            # Write transaction log
            log_file = f"{delta_log_dir}/0000000000000000000{version}.json"
            with open(log_file, 'w') as f:
                json.dump(transaction_log, f, indent=2)
            
            # Create checksum
            checksum_file = f"{delta_log_dir}/0000000000000000000{version}.crc"
            with open(checksum_file, 'w') as f:
                f.write(f"checksum_{version}")
            
            print(f"  ✅ Created version {version} ({len(new_data)} records)")

if __name__ == "__main__":
    create_many_versions()
    print("\n🎉 Many versions created successfully!")
