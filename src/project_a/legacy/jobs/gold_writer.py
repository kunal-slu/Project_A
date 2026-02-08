"""
Idempotent Gold Layer Writer with MERGE operations
"""

import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


def publish_gold_idempotent(
    source_df, gold_path: str, key_columns: list, merge_condition: str = None, spark_session=None
):
    """
    Publish data to Gold layer using MERGE operation for idempotency

    Args:
        source_df: Source DataFrame to write
        gold_path: Path to Delta table
        key_columns: List of columns to use as merge keys
        merge_condition: Custom merge condition (optional)
        spark_session: SparkSession instance (optional)
    """
    if spark_session is None:
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = spark_session

    try:
        # Try to get existing Delta table
        target = DeltaTable.forPath(spark, gold_path)

        # Create merge condition if not provided
        if merge_condition is None:
            merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in key_columns])

        logger.info(f"🔄 Merging data into existing Delta table at: {gold_path}")
        logger.info(f"📊 Source rows: {source_df.count()}")
        logger.info(f"🔑 Merge keys: {key_columns}")

        # Perform MERGE operation
        (
            target.alias("t")
            .merge(source_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Get final count
        final_count = spark.read.format("delta").load(gold_path).count()
        logger.info(f"✅ MERGE completed. Final table rows: {final_count}")

    except Exception as e:
        # Table doesn't exist yet -> first write
        logger.info(f"📝 Table doesn't exist, creating new Delta table at: {gold_path}")

        (
            source_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(gold_path)
        )

        final_count = spark.read.format("delta").load(gold_path).count()
        logger.info(f"✅ New Delta table created with {final_count} rows")

    # Log the write path and counts
    logger.info(f"📁 Gold written to: {gold_path}")
    logger.info(f"📊 Gold row count: {final_count}")

    return final_count


def publish_customer_analytics(
    source_df, gold_path: str = "data/lakehouse_delta/gold/customer_analytics"
):
    """Publish customer analytics with idempotent MERGE"""
    return publish_gold_idempotent(
        source_df=source_df, gold_path=gold_path, key_columns=["segment"]
    )


def publish_order_analytics(
    source_df, gold_path: str = "data/lakehouse_delta/gold/order_analytics"
):
    """Publish order analytics with idempotent MERGE"""
    return publish_gold_idempotent(
        source_df=source_df, gold_path=gold_path, key_columns=["amount_category"]
    )


def publish_monthly_revenue(
    source_df, gold_path: str = "data/lakehouse_delta/gold/monthly_revenue"
):
    """Publish monthly revenue with idempotent MERGE"""
    return publish_gold_idempotent(source_df=source_df, gold_path=gold_path, key_columns=["month"])
