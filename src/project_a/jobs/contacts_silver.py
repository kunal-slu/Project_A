"""
Contacts Silver Layer Transform
Implements staging → validate → publish pattern with schema enforcement.
"""
import logging
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from pyspark.sql import functions as F
from project_a.delta_utils import spark_session, write_staging, merge_publish

logger = logging.getLogger(__name__)

# Strict schema definition
CONTACTS_SCHEMA = StructType([
    StructField("contact_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("is_current", BooleanType(), True),
])

def transform_contacts(bronze_df):
    """Transform contacts from bronze to silver format."""
    return (bronze_df
            .withColumn("email", F.lower(F.trim(F.col("email"))))
            .withColumn("name", F.trim(F.col("name")))
            .withColumn("phone", F.regexp_replace(F.col("phone"), r"[^\d]", ""))
            .withColumn("company", F.trim(F.col("company")))
            .withColumn("is_current", F.lit(True)))

def run(bronze_path: str, staging_path: str, publish_path: str, 
        config: dict = None) -> dict:
    """
    Run contacts silver transform with staging → validate → publish pattern.
    
    Args:
        bronze_path: Path to bronze contacts data
        staging_path: Path for staging area
        publish_path: Path for published silver table
        config: Additional configuration
    
    Returns:
        dict: Processing statistics
    """
    spark = spark_session("contacts_silver", config)
    
    try:
        # Stage 1: Read bronze data with schema enforcement
        logger.info(f"Reading bronze data from: {bronze_path}")
        bronze_df = (spark
                    .read
                    .schema(CONTACTS_SCHEMA)
                    .format("delta")
                    .load(bronze_path))
        
        # Stage 2: Transform data
        logger.info("Transforming contacts data...")
        silver_df = transform_contacts(bronze_df)
        
        # Stage 3: Write to staging
        logger.info(f"Writing to staging: {staging_path}")
        write_staging(silver_df, staging_path)
        
        # Stage 4: Validate staging data
        logger.info("Validating staging data...")
        staging_df = spark.read.format("delta").load(staging_path)
        
        # Basic validation checks
        total_count = staging_df.count()
        null_emails = staging_df.filter(F.col("email").isNull()).count()
        duplicate_contacts = (staging_df
                            .groupBy("contact_id")
                            .count()
                            .filter(F.col("count") > 1)
                            .count())
        
        if null_emails > 0:
            raise ValueError(f"Found {null_emails} records with null emails")
        
        if duplicate_contacts > 0:
            raise ValueError(f"Found {duplicate_contacts} duplicate contact_ids")
        
        logger.info(f"Validation passed: {total_count} records")
        
        # Stage 5: Publish to silver table
        logger.info(f"Publishing to silver table: {publish_path}")
        merge_publish(spark, staging_path, publish_path, key="contact_id")
        
        return {
            "status": "success",
            "records_processed": total_count,
            "staging_path": staging_path,
            "publish_path": publish_path
        }
        
    except Exception as e:
        logger.error(f"Contacts silver transform failed: {e}")
        raise
