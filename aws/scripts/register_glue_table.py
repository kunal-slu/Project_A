"""
Register S3 table in AWS Glue Catalog.

Creates Glue table metadata pointing to S3 data for querying via Athena/Spark.
"""
import os
import logging
import boto3

logger = logging.getLogger(__name__)


def create_glue_table(bucket: str, database: str, table_name: str):
    """
    Create Glue table for customer behavior data.
    
    Args:
        bucket: S3 bucket name
        database: Glue database name
        table_name: Table name
    """
    glue = boto3.client("glue", region_name=os.getenv("AWS_REGION", "us-east-1"))
    
    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": [
                {"Name": "event_id", "Type": "string"},
                {"Name": "customer_id", "Type": "string"},
                {"Name": "event_name", "Type": "string"},
                {"Name": "event_ts", "Type": "timestamp"},
                {"Name": "session_id", "Type": "string"},
                {"Name": "url_1", "Type": "string"},
                {"Name": "url_2", "Type": "string"},
                {"Name": "device_type", "Type": "string"},
                {"Name": "browser", "Type": "string"},
                {"Name": "country", "Type": "string"},
                {"Name": "revenue", "Type": "double"},
            ],
            "Location": f"s3://{bucket}/bronze/customer_behavior/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            },
            "NumberOfBuckets": 0,
            "BucketColumns": [],
            "Parameters": {}
        },
        "PartitionKeys": [
            {"Name": "event_date", "Type": "string"}
        ],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "typeOfData": "file"
        }
    }
    
    try:
        # Check if table exists
        try:
            existing_table = glue.get_table(DatabaseName=database, Name=table_name)
            logger.info(f"Table {database}.{table_name} already exists, updating...")
            
            # Update existing table
            glue.update_table(
                DatabaseName=database,
                TableInput=table_input
            )
            logger.info(f"✅ Updated table: {database}.{table_name}")
            
        except glue.exceptions.EntityNotFoundException:
            # Create new table
            glue.create_table(
                DatabaseName=database,
                TableInput=table_input
            )
            logger.info(f"✅ Created table: {database}.{table_name}")
        
    except Exception as e:
        logger.error(f"Failed to create/update Glue table: {e}")
        raise


def main():
    """Register Glue tables for all layers."""
    bucket = os.getenv("S3_BUCKET", "my-etl-lake-demo")
    
    # Create databases if they don't exist
    glue = boto3.client("glue", region_name=os.getenv("AWS_REGION", "us-east-1"))
    
    databases = ["bronze", "silver", "gold"]
    for db in databases:
        try:
            glue.get_database(Name=db)
            logger.info(f"Database {db} already exists")
        except glue.exceptions.EntityNotFoundException:
            glue.create_database(DatabaseInput={"Name": db})
            logger.info(f"✅ Created database: {db}")
    
    # Register tables
    tables_config = [
        ("bronze", "customer_behavior", "bronze/customer_behavior/"),
        ("silver", "customer_behavior", "silver/customer_behavior/"),
        ("gold", "customer_360", "gold/customer_360/"),
    ]
    
    for database, table_name, location in tables_config:
        try:
            create_glue_table(bucket, database, table_name)
        except Exception as e:
            logger.error(f"Failed to register {database}.{table_name}: {e}")
    
    logger.info("✅ Glue table registration complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

