"""
Lineage Emitter for OpenLineage/Marquez

Emits data lineage events to track data flow through the pipeline.
Supports both Marquez and generic OpenLineage backends.
"""
import json
import logging
import requests
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class LineageEmitter:
    """
    Emits lineage events to OpenLineage/Marquez backend.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize lineage emitter from config.
        
        Args:
            config: Lineage configuration dict with:
                - enabled: bool
                - backend: str (marquez or openlineage)
                - url: str (backend URL)
                - namespace: str
                - api_key: Optional[str]
        """
        self.enabled = config.get("enabled", False)
        self.backend = config.get("backend", "marquez").lower()
        self.url = config.get("url", "").rstrip("/")
        self.namespace = config.get("namespace", "project_a_dev")
        self.api_key = config.get("api_key", "")
        
        if self.enabled and not self.url:
            logger.warning("⚠️  Lineage enabled but no URL configured")
            self.enabled = False
    
    def _headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers
    
    def _get_api_path(self) -> str:
        """Get API path based on backend type."""
        if self.backend == "marquez":
            return "/api/v1/lineage"
        else:  # openlineage
            return "/api/v1/lineage"
    
    def emit_job(
        self,
        job_name: str,
        run_id: str,
        inputs: List[str],
        outputs: List[str],
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Emit a lineage event for a job run.
        
        Args:
            job_name: Name of the job (e.g., "bronze_to_silver")
            run_id: Unique run identifier
            inputs: List of input dataset names (e.g., ["bronze.crm.accounts"])
            outputs: List of output dataset names (e.g., ["silver.customer_360"])
            status: Job status ("SUCCESS" or "FAILED")
            error_message: Optional error message if failed
            metadata: Optional additional metadata
        """
        if not self.enabled:
            logger.debug("Lineage disabled, skipping emission")
            return
        
        if not self.url:
            logger.warning("Lineage URL not configured, skipping emission")
            return
        
        try:
            event_time = datetime.now(timezone.utc).isoformat()
            
            # Determine event type
            if status.upper() == "SUCCESS":
                event_type = "COMPLETE"
            else:
                event_type = "FAIL"
            
            # Build OpenLineage-compatible event
            payload = {
                "eventType": event_type,
                "eventTime": event_time,
                "run": {
                    "runId": run_id,
                    "facets": {}
                },
                "job": {
                    "namespace": self.namespace,
                    "name": job_name,
                    "facets": {
                        "documentation": {
                            "description": f"ETL job: {job_name}"
                        }
                    }
                },
                "inputs": [
                    {
                        "namespace": self.namespace,
                        "name": input_name,
                        "facets": {
                            "dataSource": {
                                "name": "S3",
                                "uri": f"s3://my-etl-lake-demo-424570854632/{input_name.replace('.', '/')}"
                            }
                        }
                    }
                    for input_name in inputs
                ],
                "outputs": [
                    {
                        "namespace": self.namespace,
                        "name": output_name,
                        "facets": {
                            "dataSource": {
                                "name": "S3",
                                "uri": f"s3://my-etl-lake-demo-424570854632/{output_name.replace('.', '/')}"
                            }
                        }
                    }
                    for output_name in outputs
                ],
                "producer": "project_a_pyspark",
                "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json"
            }
            
            # Add error information if failed
            if event_type == "FAIL" and error_message:
                payload["run"]["facets"]["errorMessage"] = {
                    "message": error_message
                }
            
            # Add custom metadata if provided
            if metadata:
                payload["run"]["facets"]["custom"] = metadata
            
            # Send to backend
            api_path = self._get_api_path()
            full_url = f"{self.url}{api_path}"
            
            response = requests.post(
                full_url,
                headers=self._headers(),
                json=payload,
                timeout=10
            )
            
            response.raise_for_status()
            
            logger.info(f"✅ Emitted lineage event: {job_name} ({run_id}) - {status}")
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️  Failed to emit lineage event: {e}")
        except Exception as e:
            logger.warning(f"⚠️  Unexpected error emitting lineage: {e}")


def load_lineage_config(config_path: str) -> Dict[str, Any]:
    """
    Load lineage configuration from YAML file (local or S3).
    
    Args:
        config_path: Path to config file (local or s3://...)
    
    Returns:
        Configuration dictionary
    """
    import yaml
    
    if config_path.startswith("s3://"):
        import boto3
        
        s3 = boto3.client("s3")
        path_parts = config_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""
        
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read().decode("utf-8")
            return yaml.safe_load(content) or {}
        except Exception as e:
            logger.warning(f"⚠️  Failed to load lineage config from S3: {e}")
            return {"enabled": False}
    else:
        # Local file
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, "r") as f:
                return yaml.safe_load(f) or {}
        else:
            logger.warning(f"⚠️  Lineage config file not found: {config_path}")
            return {"enabled": False}
