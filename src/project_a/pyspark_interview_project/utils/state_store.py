"""
State store for watermark and checkpoint management.

Manages S3-based state files for incremental ingestion watermarks,
streaming checkpoints, and job run metadata.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import boto3
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None

logger = logging.getLogger(__name__)


class StateStore:
    """
    Manages state files (watermarks, checkpoints) in S3 or local filesystem.

    Uses S3 for production and local filesystem for development/testing.
    """

    def __init__(self, bucket: str | None = None, prefix: str = "_state", s3_client=None):
        """
        Initialize state store.

        Args:
            bucket: S3 bucket name (if None, uses local filesystem)
            prefix: S3 prefix or local directory path
            s3_client: Optional boto3 S3 client (auto-created if None)
        """
        self.bucket = bucket
        self.prefix = prefix
        self.use_s3 = bucket is not None and BOTO3_AVAILABLE

        if self.use_s3:
            self.s3_client = s3_client or boto3.client("s3")
        else:
            # Local filesystem fallback
            local_dir = Path(prefix)
            local_dir.mkdir(parents=True, exist_ok=True)
            self.local_dir = local_dir
            logger.info(f"StateStore initialized with local directory: {self.local_dir}")

    def _get_key(self, source_name: str) -> str:
        """Get S3 key or local file path for a source."""
        return f"{source_name}.json"

    def get_watermark(self, source_name: str) -> str | None:
        """
        Get watermark value for a source.

        Args:
            source_name: Name of the data source (e.g., 'crm_contacts')

        Returns:
            ISO8601 timestamp string or None if not found
        """
        key = self._get_key(source_name)

        if self.use_s3:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=f"{self.prefix}/{key}")
                data = json.loads(response["Body"].read().decode("utf-8"))
                watermark = data.get("watermark")
                logger.debug(f"Retrieved watermark for {source_name}: {watermark}")
                return watermark
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logger.info(f"No watermark found for {source_name} (first run)")
                    return None
                else:
                    logger.warning(f"Error reading watermark from S3: {e}")
                    return None
            except Exception as e:
                logger.warning(f"Failed to read watermark: {e}")
                return None
        else:
            # Local filesystem
            file_path = self.local_dir / key
            if not file_path.exists():
                logger.info(f"No watermark found for {source_name} at {file_path} (first run)")
                return None

            try:
                with open(file_path) as f:
                    data = json.load(f)
                watermark = data.get("watermark")
                logger.debug(f"Retrieved watermark for {source_name}: {watermark}")
                return watermark
            except Exception as e:
                logger.warning(f"Failed to read local watermark: {e}")
                return None

    def set_watermark(self, source_name: str, value: str) -> None:
        """
        Set watermark value for a source.

        Args:
            source_name: Name of the data source
            value: ISO8601 timestamp string
        """
        key = self._get_key(source_name)
        data = {
            "source": source_name,
            "watermark": value,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }

        if self.use_s3:
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=f"{self.prefix}/{key}",
                    Body=json.dumps(data, indent=2).encode("utf-8"),
                    ContentType="application/json",
                )
                logger.info(f"Updated watermark for {source_name}: {value}")
            except Exception as e:
                logger.error(f"Failed to write watermark to S3: {e}")
                raise
        else:
            # Local filesystem
            file_path = self.local_dir / key
            try:
                with open(file_path, "w") as f:
                    json.dump(data, f, indent=2)
                logger.info(f"Updated watermark for {source_name} at {file_path}: {value}")
            except Exception as e:
                logger.error(f"Failed to write local watermark: {e}")
                raise

    def get_checkpoint(self, stream_name: str) -> dict[str, Any] | None:
        """
        Get checkpoint data for a streaming job.

        Args:
            stream_name: Name of the stream (e.g., 'kafka_orders')

        Returns:
            Checkpoint dictionary or None
        """
        key = f"{stream_name}_checkpoint.json"

        if self.use_s3:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=f"{self.prefix}/{key}")
                data = json.loads(response["Body"].read().decode("utf-8"))
                return data
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    return None
                logger.warning(f"Error reading checkpoint: {e}")
                return None
        else:
            file_path = self.local_dir / key
            if not file_path.exists():
                return None
            try:
                with open(file_path) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read checkpoint: {e}")
                return None

    def set_checkpoint(self, stream_name: str, checkpoint_data: dict[str, Any]) -> None:
        """
        Set checkpoint data for a streaming job.

        Args:
            stream_name: Name of the stream
            checkpoint_data: Checkpoint dictionary
        """
        key = f"{stream_name}_checkpoint.json"
        data = {
            "stream": stream_name,
            "checkpoint": checkpoint_data,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }

        if self.use_s3:
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=f"{self.prefix}/{key}",
                    Body=json.dumps(data, indent=2).encode("utf-8"),
                    ContentType="application/json",
                )
            except Exception as e:
                logger.error(f"Failed to write checkpoint: {e}")
                raise
        else:
            file_path = self.local_dir / key
            try:
                with open(file_path, "w") as f:
                    json.dump(data, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to write checkpoint: {e}")
                raise


def get_state_store(config: dict[str, Any]) -> StateStore:
    """
    Factory function to create StateStore from config.

    Args:
        config: Configuration dictionary

    Returns:
        StateStore instance
    """
    bucket = config.get("data_lake", {}).get("bucket") or config.get("s3", {}).get("bucket")
    prefix = config.get("data_lake", {}).get("state_prefix", "_state")

    return StateStore(bucket=bucket, prefix=prefix)
