"""
Checkpoint Utility for Streaming/Incremental Processing

Manages checkpoints in S3 (or DynamoDB) to track last processed batch/watermark
for incremental and streaming jobs.
"""
import json
import logging
from typing import Optional, Any, Dict
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_last_processed_batch(
    bucket: str,
    key: str,
    region: str = "us-east-1"
) -> Optional[str]:
    """
    Get last processed batch ID from S3 checkpoint.
    
    Args:
        bucket: S3 bucket name
        key: S3 key for checkpoint file
        region: AWS region
        
    Returns:
        Last processed batch ID, or None if not found
    """
    try:
        s3 = boto3.client("s3", region_name=region)
        obj = s3.get_object(Bucket=bucket, Key=key)
        batch_id = obj["Body"].read().decode("utf-8").strip()
        logger.info(f"✅ Retrieved checkpoint: {batch_id} from s3://{bucket}/{key}")
        return batch_id
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info(f"ℹ️  No checkpoint found at s3://{bucket}/{key} (first run)")
            return None
        logger.error(f"❌ Failed to read checkpoint: {e}")
        raise


def set_last_processed_batch(
    bucket: str,
    key: str,
    batch_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    region: str = "us-east-1"
) -> None:
    """
    Set last processed batch ID in S3 checkpoint.
    
    Args:
        bucket: S3 bucket name
        key: S3 key for checkpoint file
        batch_id: Batch ID to store
        metadata: Optional metadata to include
        region: AWS region
    """
    try:
        s3 = boto3.client("s3", region_name=region)
        
        payload = {
            "batch_id": batch_id,
            "timestamp_utc": datetime.utcnow().isoformat(),
        }
        
        if metadata:
            payload["metadata"] = metadata
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"✅ Wrote checkpoint: {batch_id} to s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"❌ Failed to write checkpoint: {e}")
        raise


def get_watermark(
    bucket: str,
    key: str,
    region: str = "us-east-1"
) -> Optional[datetime]:
    """
    Get watermark timestamp from checkpoint.
    
    Args:
        bucket: S3 bucket name
        key: S3 key for checkpoint file
        region: AWS region
        
    Returns:
        Watermark datetime, or None if not found
    """
    try:
        s3 = boto3.client("s3", region_name=region)
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = json.loads(obj["Body"].read().decode("utf-8"))
        
        watermark_str = content.get("watermark")
        if watermark_str:
            return datetime.fromisoformat(watermark_str.replace("Z", "+00:00"))
        return None
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        logger.error(f"❌ Failed to read watermark: {e}")
        raise


def set_watermark(
    bucket: str,
    key: str,
    watermark: datetime,
    metadata: Optional[Dict[str, Any]] = None,
    region: str = "us-east-1"
) -> None:
    """
    Set watermark timestamp in checkpoint.
    
    Args:
        bucket: S3 bucket name
        key: S3 key for checkpoint file
        watermark: Watermark datetime
        metadata: Optional metadata to include
        region: AWS region
    """
    try:
        s3 = boto3.client("s3", region_name=region)
        
        payload = {
            "watermark": watermark.isoformat(),
            "timestamp_utc": datetime.utcnow().isoformat(),
        }
        
        if metadata:
            payload["metadata"] = metadata
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"✅ Wrote watermark: {watermark.isoformat()} to s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"❌ Failed to write watermark: {e}")
        raise

