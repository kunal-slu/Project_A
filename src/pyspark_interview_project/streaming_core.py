import logging
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from .io.path_resolver import resolve


logger = logging.getLogger(__name__)


def write_stream(
    df: DataFrame, cfg: dict, table_path_logical: str, checkpoint_logical: str
):
    """
    Write streaming data with enforced checkpoint and DLQ handling.
    
    Args:
        df: Input DataFrame
        cfg: Configuration dictionary
        table_path_logical: Logical path for output table
        checkpoint_logical: Logical path for checkpoint location
    
    Returns:
        StreamingQuery object
    """
    # Validate required configuration
    if "streaming" not in cfg:
        raise ValueError("Streaming configuration is required")
    
    streaming_cfg = cfg["streaming"]
    watermark = streaming_cfg.get("watermark", "1 hour")
    dlq_logical = streaming_cfg.get("dlq_path")
    
    if not dlq_logical:
        raise ValueError("DLQ path is required in streaming configuration")
    
    # Resolve paths
    out_path = resolve(table_path_logical, cfg)
    chk_path = resolve(checkpoint_logical, cfg)
    dlq_path = resolve(dlq_logical, cfg)
    
    logger.info(f"Streaming write: {out_path}, checkpoint: {chk_path}, DLQ: {dlq_path}")
    
    # Validate DataFrame has required columns
    required_cols = ["event_time", "business_key"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")
    
    # Apply watermark and deduplication
    df_ev = df.withWatermark("event_time", watermark)
    
    # Use dropDuplicatesWithinWatermark for Spark 3.5+ if available
    try:
        df_ev = df_ev.dropDuplicatesWithinWatermark(["business_key"], "event_time")
        logger.info("Using dropDuplicatesWithinWatermark for deduplication")
    except AttributeError:
        # Fallback for older Spark versions
        df_ev = df_ev.dropDuplicates(["business_key", "event_time"])
        logger.info("Using dropDuplicates for deduplication (fallback)")
    
    # Configure streaming write with strict error handling
    query = (
        df_ev.writeStream
        .format("delta")
        .option("checkpointLocation", chk_path)
        .option("failOnDataLoss", "true")  # Fail on data loss instead of silent failure
        .option("maxFilesPerTrigger", streaming_cfg.get("max_files_per_trigger", 1000))
        .option("maxRecordsPerFile", streaming_cfg.get("max_records_per_file", 1000000))
        .outputMode("append")
        .trigger(processingTime=streaming_cfg.get("trigger_interval", "1 minute"))
        .start(out_path)
    )
    
    # Log query details
    logger.info(f"Started streaming query: {query.id}, status: {query.status['message']}")
    
    return query


def create_dlq_handler(cfg: dict, checkpoint_logical: str):
    """
    Create a DLQ handler for malformed/late messages.
    
    Args:
        cfg: Configuration dictionary
        checkpoint_logical: Logical path for checkpoint location
    
    Returns:
        Function to handle DLQ messages
    """
    dlq_logical = cfg["streaming"]["dlq_path"]
    dlq_path = resolve(dlq_logical, cfg)
    dlq_chk_path = resolve(checkpoint_logical, cfg) + "/dlq"
    
    def handle_dlq(malformed_df: DataFrame, error_reason: str):
        """Handle malformed messages by writing to DLQ"""
        if malformed_df.count() > 0:
            # Add error metadata
            dlq_df = malformed_df.withColumn("dlq_timestamp", F.current_timestamp()) \
                                .withColumn("error_reason", F.lit(error_reason))
            
            # Write to DLQ
            dlq_query = (
                dlq_df.writeStream
                .format("delta")
                .option("checkpointLocation", dlq_chk_path)
                .option("failOnDataLoss", "true")
                .outputMode("append")
                .start(dlq_path)
            )
            
            logger.warning(f"Writing {malformed_df.count()} malformed records to DLQ: {error_reason}")
            return dlq_query
        
        return None
    
    return handle_dlq


def validate_streaming_config(cfg: dict):
    """
    Validate streaming configuration to ensure all required settings are present.
    
    Args:
        cfg: Configuration dictionary
    
    Raises:
        ValueError: If required configuration is missing
    """
    if "streaming" not in cfg:
        raise ValueError("Streaming configuration section is required")
    
    streaming_cfg = cfg["streaming"]
    required_keys = ["watermark", "dlq_path"]
    missing_keys = [key for key in required_keys if key not in streaming_cfg]
    
    if missing_keys:
        raise ValueError(f"Missing required streaming configuration keys: {missing_keys}")
    
    # Validate watermark format
    watermark = streaming_cfg["watermark"]
    if not isinstance(watermark, str) or not any(unit in watermark for unit in ["hour", "minute", "second", "day"]):
        raise ValueError(f"Invalid watermark format: {watermark}. Expected format like '1 hour', '30 minutes', etc.")
    
    logger.info("Streaming configuration validation passed")
