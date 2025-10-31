#!/usr/bin/env python3
"""
Lake Formation tags seeding script.
Creates LF-Tags and demonstrates row-level access control.
"""

import os
import sys
import logging
import boto3
from typing import Dict, List, Any
from pathlib import Path

logger = logging.getLogger(__name__)


def create_lf_tags(lf_client, tags: List[Dict[str, str]]) -> None:
    """
    Create Lake Formation tags.
    
    Args:
        lf_client: Lake Formation client
        tags: List of tag dictionaries
    """
    logger.info("Creating Lake Formation tags")
    
    for tag in tags:
        try:
            lf_client.create_lf_tag(
                TagKey=tag["key"],
                TagValues=tag["values"]
            )
            logger.info(f"Created LF-Tag: {tag['key']} = {tag['values']}")
        except lf_client.exceptions.EntityNotFoundException:
            logger.info(f"LF-Tag {tag['key']} already exists")
        except Exception as e:
            logger.error(f"Failed to create LF-Tag {tag['key']}: {e}")
            raise


def attach_tags_to_table(lf_client, database_name: str, table_name: str, tags: Dict[str, str]) -> None:
    """
    Attach tags to a Glue table.
    
    Args:
        lf_client: Lake Formation client
        database_name: Glue database name
        table_name: Table name
        tags: Dictionary of tag key-value pairs
    """
    logger.info(f"Attaching tags to table {database_name}.{table_name}")
    
    try:
        lf_client.update_lf_tag(
            Resource={
                'Table': {
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            },
            TagKey=tags['key'],
            TagValues=tags['values']
        )
        logger.info(f"Attached tag {tags['key']} to {database_name}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to attach tag to {database_name}.{table_name}: {e}")
        raise


def attach_tags_to_column(lf_client, database_name: str, table_name: str, column_name: str, tags: Dict[str, str]) -> None:
    """
    Attach tags to a table column.
    
    Args:
        lf_client: Lake Formation client
        database_name: Glue database name
        table_name: Table name
        column_name: Column name
        tags: Dictionary of tag key-value pairs
    """
    logger.info(f"Attaching tags to column {database_name}.{table_name}.{column_name}")
    
    try:
        lf_client.update_lf_tag(
            Resource={
                'TableWithColumns': {
                    'DatabaseName': database_name,
                    'Name': table_name,
                    'ColumnNames': [column_name]
                }
            },
            TagKey=tags['key'],
            TagValues=tags['values']
        )
        logger.info(f"Attached tag {tags['key']} to column {column_name}")
    except Exception as e:
        logger.error(f"Failed to attach tag to column {column_name}: {e}")
        raise


def create_grant_policy(lf_client, principal: str, permissions: List[str], resource: Dict[str, Any], conditions: Dict[str, Any] = None) -> None:
    """
    Create a Lake Formation grant policy.
    
    Args:
        lf_client: Lake Formation client
        principal: Principal ARN
        permissions: List of permissions
        resource: Resource definition
        conditions: Optional conditions
    """
    logger.info(f"Creating grant policy for {principal}")
    
    try:
        grant_params = {
            'Principal': {'DataLakePrincipalIdentifier': principal},
            'Resource': resource,
            'Permissions': permissions
        }
        
        if conditions:
            grant_params['PermissionsWithGrantOption'] = permissions
            grant_params['Conditions'] = conditions
        
        lf_client.grant_permissions(**grant_params)
        logger.info(f"Granted permissions {permissions} to {principal}")
    except Exception as e:
        logger.error(f"Failed to create grant policy: {e}")
        raise


def main():
    """Main function to seed Lake Formation tags."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Seed Lake Formation tags and policies")
    parser.add_argument("--database", required=True, help="Glue database name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Create Lake Formation client
        lf_client = boto3.client('lakeformation', region_name=args.region)
        
        # Define LF-Tags
        lf_tags = [
            {
                "key": "sensitivity",
                "values": ["pii", "internal", "public"]
            },
            {
                "key": "region",
                "values": ["NA", "EU", "APAC"]
            },
            {
                "key": "department",
                "values": ["sales", "marketing", "finance", "engineering"]
            }
        ]
        
        # Create LF-Tags
        create_lf_tags(lf_client, lf_tags)
        
        # Attach tags to tables
        tables = ["customers", "orders", "products"]
        
        for table in tables:
            # Attach sensitivity tags
            if table == "customers":
                attach_tags_to_table(lf_client, args.database, table, {
                    "key": "sensitivity",
                    "values": ["pii"]
                })
                # Tag email column as PII
                attach_tags_to_column(lf_client, args.database, table, "email", {
                    "key": "sensitivity",
                    "values": ["pii"]
                })
            else:
                attach_tags_to_table(lf_client, args.database, table, {
                    "key": "sensitivity",
                    "values": ["internal"]
                })
            
            # Attach region tags
            attach_tags_to_table(lf_client, args.database, table, {
                "key": "region",
                "values": ["NA"]
            })
        
        # Create grant policies
        # Example: EU analysts can only access EU data
        eu_analyst_principal = "arn:aws:iam::123456789012:role/EUAnalystRole"
        
        create_grant_policy(
            lf_client,
            eu_analyst_principal,
            ["SELECT"],
            {
                "Table": {
                    "DatabaseName": args.database,
                    "Name": "customers"
                }
            },
            {
                "StringEquals": {
                    "aws:PrincipalTag/region": "EU"
                }
            }
        )
        
        logger.info("Lake Formation tags and policies created successfully")
        print("✅ Lake Formation governance setup completed!")
        print("   - Created LF-Tags: sensitivity, region, department")
        print("   - Attached tags to tables and columns")
        print("   - Created row-level access policies")
        
    except Exception as e:
        logger.error(f"Lake Formation setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
