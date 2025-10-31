#!/usr/bin/env python3
"""
DR Snapshot Export Script

Copies latest Silver/Gold partitions to a second region (DR bucket)
for disaster recovery purposes. This enables rapid restoration if
primary region fails.
"""

import sys
import os
import logging
import boto3
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..', 'src'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DRSnapshotExporter:
    """Exports data lake snapshots to DR region."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.primary_region = config.get('aws', {}).get('region', 'us-east-1')
        self.dr_region = config.get('dr', {}).get('region', 'us-west-2')
        
        # S3 clients
        self.primary_s3 = boto3.client('s3', region_name=self.primary_region)
        self.dr_s3 = boto3.client('s3', region_name=self.dr_region)
        
        # Bucket names
        lake_config = config.get('lake', {})
        self.primary_bucket = lake_config.get('root', '').replace('s3://', '').split('/')[0]
        self.dr_bucket = config.get('dr', {}).get('bucket', f"{self.primary_bucket}-dr")
        
        logger.info(f"Primary region: {self.primary_region}, bucket: {self.primary_bucket}")
        logger.info(f"DR region: {self.dr_region}, bucket: {self.dr_bucket}")
    
    def get_latest_partitions(self, layer: str, table: str, days_back: int = 7) -> List[str]:
        """
        Get latest partition paths for a table.
        
        Args:
            layer: bronze/silver/gold
            table: Table name
            days_back: How many days back to include
            
        Returns:
            List of partition paths
        """
        prefix = f"{layer}/{table}/"
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)
        
        partitions = []
        
        try:
            paginator = self.primary_s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.primary_bucket,
                Prefix=prefix,
                Delimiter='/'
            )
            
            for page in pages:
                if 'CommonPrefixes' in page:
                    for prefix_obj in page['CommonPrefixes']:
                        partition_path = prefix_obj['Prefix']
                        # Extract date from partition path (assuming format: ingest_date=YYYY-MM-DD/)
                        if 'ingest_date=' in partition_path or 'order_date=' in partition_path:
                            partitions.append(partition_path)
            
            logger.info(f"Found {len(partitions)} partitions for {layer}/{table}")
            return partitions
            
        except Exception as e:
            logger.error(f"Failed to list partitions: {str(e)}")
            return []
    
    def copy_partition(self, source_key: str, dest_key: str) -> bool:
        """
        Copy partition from primary to DR bucket.
        
        Args:
            source_key: Source S3 key
            dest_key: Destination S3 key
            
        Returns:
            True if successful
        """
        try:
            copy_source = {
                'Bucket': self.primary_bucket,
                'Key': source_key
            }
            
            self.dr_s3.copy_object(
                CopySource=copy_source,
                Bucket=self.dr_bucket,
                Key=dest_key
            )
            
            logger.debug(f"Copied {source_key} -> {dest_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy {source_key}: {str(e)}")
            return False
    
    def export_table(self, layer: str, table: str, days_back: int = 7) -> Dict[str, Any]:
        """
        Export all partitions for a table to DR region.
        
        Args:
            layer: bronze/silver/gold
            table: Table name
            days_back: Days of data to export
            
        Returns:
            Export statistics
        """
        logger.info(f"Exporting {layer}/{table} to DR region...")
        
        partitions = self.get_latest_partitions(layer, table, days_back)
        
        stats = {
            "table": f"{layer}/{table}",
            "partitions_found": len(partitions),
            "partitions_copied": 0,
            "partitions_failed": 0,
            "start_time": datetime.utcnow().isoformat()
        }
        
        for partition in partitions:
            dest_key = partition  # Same key structure in DR bucket
            
            if self.copy_partition(partition, dest_key):
                stats["partitions_copied"] += 1
            else:
                stats["partitions_failed"] += 1
        
        stats["end_time"] = datetime.utcnow().isoformat()
        logger.info(f"Export completed: {stats['partitions_copied']}/{stats['partitions_found']} partitions copied")
        
        return stats
    
    def export_all_tables(self, layers: List[str] = None, days_back: int = 7) -> Dict[str, Any]:
        """
        Export all specified layers to DR region.
        
        Args:
            layers: List of layers to export (default: ['silver', 'gold'])
            days_back: Days of data to export
            
        Returns:
            Overall export statistics
        """
        if layers is None:
            layers = ['silver', 'gold']  # Only export cleaned data, not raw bronze
        
        logger.info(f"Starting DR snapshot export for layers: {layers}")
        
        # Tables to export (from config or default)
        tables_config = self.config.get('dr', {}).get('tables', {
            'silver': ['dim_customer', 'orders_clean', 'customers_clean'],
            'gold': ['fact_sales', 'dim_customer', 'marketing_attribution']
        })
        
        overall_stats = {
            "start_time": datetime.utcnow().isoformat(),
            "layers_exported": [],
            "total_partitions_copied": 0,
            "total_partitions_failed": 0,
            "table_stats": []
        }
        
        for layer in layers:
            tables = tables_config.get(layer, [])
            
            for table in tables:
                stats = self.export_table(layer, table, days_back)
                overall_stats["table_stats"].append(stats)
                overall_stats["total_partitions_copied"] += stats["partitions_copied"]
                overall_stats["total_partitions_failed"] += stats["partitions_failed"]
            
            overall_stats["layers_exported"].append(layer)
        
        overall_stats["end_time"] = datetime.utcnow().isoformat()
        
        logger.info(f"DR snapshot export completed:")
        logger.info(f"  Total partitions copied: {overall_stats['total_partitions_copied']}")
        logger.info(f"  Total partitions failed: {overall_stats['total_partitions_failed']}")
        
        return overall_stats
    
    def create_dr_metadata(self, export_stats: Dict[str, Any]) -> None:
        """
        Create metadata file in DR bucket documenting the snapshot.
        
        Args:
            export_stats: Export statistics
        """
        metadata = {
            "snapshot_type": "dr_export",
            "export_time": datetime.utcnow().isoformat(),
            "primary_region": self.primary_region,
            "dr_region": self.dr_region,
            "primary_bucket": self.primary_bucket,
            "dr_bucket": self.dr_bucket,
            "export_stats": export_stats
        }
        
        metadata_key = "dr_snapshots/latest_snapshot_metadata.json"
        
        try:
            import json
            self.dr_s3.put_object(
                Bucket=self.dr_bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Created DR metadata: {metadata_key}")
            
        except Exception as e:
            logger.error(f"Failed to create DR metadata: {str(e)}")


def main():
    """Main entry point."""
    import argparse
    import yaml
    
    parser = argparse.ArgumentParser(description="Export data lake snapshot to DR region")
    parser.add_argument("--config", default="config/prod.yaml", help="Configuration file")
    parser.add_argument("--layers", nargs="+", default=["silver", "gold"],
                       help="Layers to export")
    parser.add_argument("--days-back", type=int, default=7,
                       help="Days of data to export (default: 7)")
    args = parser.parse_args()
    
    # Load config
    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {str(e)}")
        sys.exit(1)
    
    # Run export
    exporter = DRSnapshotExporter(config)
    export_stats = exporter.export_all_tables(args.layers, args.days_back)
    
    # Create metadata
    exporter.create_dr_metadata(export_stats)
    
    # Exit with error if failures
    if export_stats["total_partitions_failed"] > 0:
        logger.warning(f"Export completed with {export_stats['total_partitions_failed']} failures")
        sys.exit(1)
    else:
        logger.info("DR snapshot export completed successfully")
        sys.exit(0)


if __name__ == "__main__":
    main()

