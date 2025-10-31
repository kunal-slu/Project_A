"""
OpenLineage integration for data lineage tracking.
"""

import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


def emit_lineage_event(
    event_type: str,
    job_name: str,
    inputs: list,
    outputs: list,
    config: Dict[str, Any],
    run_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Emit OpenLineage event for data lineage tracking.
    
    Args:
        event_type: 'START', 'COMPLETE', 'FAIL'
        job_name: Name of the job
        inputs: List of input datasets
        outputs: List of output datasets
        config: Configuration dict with lineage settings
        run_id: Optional run ID
        metadata: Optional additional metadata
        
    Returns:
        True if emitted successfully, False otherwise
    """
    if not config.get("lineage", {}).get("enabled", False):
        return False
    
    lineage_config = config.get("lineage", {})
    url = lineage_config.get("url", "").rstrip("/") + "/api/v1/lineage"
    
    if not url or url == "/api/v1/lineage":
        logger.warning("Lineage URL not configured")
        return False
    
    event = {
        "eventType": event_type,
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "run": {
            "runId": run_id or f"{job_name}_{datetime.utcnow().isoformat()}",
        },
        "job": {
            "namespace": config.get("environment", "default"),
            "name": job_name
        },
        "inputs": [
            {
                "namespace": "s3",
                "name": inp.get("name", inp),
                "facets": {
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                        "fields": inp.get("schema", [])
                    }
                }
            }
            for inp in inputs
        ],
        "outputs": [
            {
                "namespace": "s3",
                "name": out.get("name", out),
                "facets": {
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                        "fields": out.get("schema", [])
                    }
                }
            }
            for out in outputs
        ]
    }
    
    if metadata:
        event["run"]["facets"] = metadata
    
    try:
        response = requests.post(url, json=event, timeout=5)
        response.raise_for_status()
        logger.info(f"Lineage event emitted: {event_type} for {job_name}")
        return True
    except Exception as e:
        logger.warning(f"Failed to emit lineage event: {e}")
        return False


# Convenience functions
def emit_start(
    job_name: str,
    inputs: list,
    outputs: list,
    config: Dict[str, Any],
    run_id: Optional[str] = None
) -> bool:
    """Emit START lineage event."""
    return emit_lineage_event("START", job_name, inputs, outputs, config, run_id)


def emit_complete(
    job_name: str,
    inputs: list,
    outputs: list,
    config: Dict[str, Any],
    run_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """Emit COMPLETE lineage event."""
    return emit_lineage_event("COMPLETE", job_name, inputs, outputs, config, run_id, metadata)


def emit_fail(
    job_name: str,
    inputs: list,
    outputs: list,
    config: Dict[str, Any],
    run_id: Optional[str] = None,
    error: Optional[str] = None
) -> bool:
    """Emit FAIL lineage event."""
    metadata = {"error": {"message": error}} if error else None
    return emit_lineage_event("FAIL", job_name, inputs, outputs, config, run_id, metadata)

