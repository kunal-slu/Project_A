"""
Run Audit Trail Utility

Persists job execution metadata (row counts, status, timestamps) to S3
for audit and observability without requiring a separate database.
"""
import json
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def write_run_audit(
    bucket: str,
    job_name: str,
    env: str,
    source: str,
    target: str,
    rows_in: int,
    rows_out: int,
    status: str,
    run_id: Optional[str] = None,
    error_message: Optional[str] = None,
    duration_ms: Optional[float] = None,
    config: Optional[Dict[str, Any]] = None
) -> str:
    """
    Write run audit record to S3.
    
    Args:
        bucket: S3 bucket name
        job_name: Name of the job (e.g., "fx_json_to_bronze")
        env: Environment (dev/staging/prod)
        source: Source data location/identifier
        target: Target data location/identifier
        rows_in: Number of input rows processed
        rows_out: Number of output rows written
        status: Job status ("SUCCESS", "FAILED", "PARTIAL")
        run_id: Optional run ID (generated if not provided)
        error_message: Optional error message if failed
        duration_ms: Optional job duration in milliseconds
        config: Optional config dictionary for reference
        
    Returns:
        S3 key where audit record was written
    """
    if run_id is None:
        run_id = str(uuid.uuid4())
    
    timestamp_utc = datetime.utcnow()
    date_str = timestamp_utc.strftime("%Y-%m-%d")
    
    # S3 key structure: _audit/{env}/{job_name}/{date}/{run_id}.json
    key = f"_audit/{env}/{job_name}/{date_str}/{run_id}.json"
    
    payload = {
        "run_id": run_id,
        "timestamp_utc": timestamp_utc.isoformat(),
        "job_name": job_name,
        "env": env,
        "source": source,
        "target": target,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "status": status,
    }
    
    if error_message:
        payload["error_message"] = error_message
    
    if duration_ms is not None:
        payload["duration_ms"] = duration_ms
    
    if config:
        # Include relevant config sections (sanitize sensitive data)
        payload["config"] = {
            "environment": config.get("environment"),
            "paths": config.get("paths", {}),
        }
    
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"✅ Wrote run audit to s3://{bucket}/{key}")
        return key
    except ClientError as e:
        logger.error(f"❌ Failed to write run audit: {e}")
        # Don't fail the job if audit write fails
        return ""


def read_run_audit(
    bucket: str,
    job_name: str,
    env: str,
    date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Read run audit records for a job.
    
    Args:
        bucket: S3 bucket name
        job_name: Name of the job
        env: Environment
        date: Optional date (YYYY-MM-DD). Defaults to today.
        
    Returns:
        List of audit records
    """
    if date is None:
        date = datetime.utcnow().strftime("%Y-%m-%d")
    
    prefix = f"_audit/{env}/{job_name}/{date}/"
    
    try:
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        records = []
        if "Contents" in response:
            for obj in response["Contents"]:
                try:
                    obj_response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                    content = obj_response["Body"].read().decode("utf-8")
                    record = json.loads(content)
                    records.append(record)
                except Exception as e:
                    logger.warning(f"Failed to read audit record {obj['Key']}: {e}")
        
        return sorted(records, key=lambda x: x.get("timestamp_utc", ""), reverse=True)
    except ClientError as e:
        logger.error(f"❌ Failed to read run audit: {e}")
        return []

