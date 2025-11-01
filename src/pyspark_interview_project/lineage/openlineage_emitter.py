"""OpenLineage emitter for dataset-level lineage.

Reads endpoint and namespace from environment if present:
  - OPENLINEAGE_URL
  - OPENLINEAGE_NAMESPACE
Falls back to config lineage.url and lineage.namespace.
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests

logger = logging.getLogger(__name__)


def _get_endpoint(cfg: dict) -> Optional[str]:
    url = os.getenv("OPENLINEAGE_URL") or cfg.get("lineage", {}).get("url")
    return url.rstrip("/") if url else None


def _get_namespace(cfg: dict) -> str:
    return os.getenv("OPENLINEAGE_NAMESPACE", cfg.get("lineage", {}).get("namespace", "default"))


def emit(event: dict, cfg: dict) -> None:
    if not cfg.get("lineage", {}).get("enabled", True):
        return
    url = _get_endpoint(cfg)
    if not url:
        logger.warning("OpenLineage URL not configured; skipping emit")
        return
    try:
        requests.post(f"{url}/api/v1/lineage", json=event, timeout=3)
    except Exception as e:
        logger.debug(f"OpenLineage emit failed: {e}")


def build_dataset(name: str, namespace: str) -> Dict[str, Any]:
    return {"namespace": namespace, "name": name}


def emit_job_event(
    cfg: dict,
    job_name: str,
    inputs: List[str],
    outputs: List[str],
    event_type: str,
    run_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    ns = _get_namespace(cfg)
    event = {
        "eventType": event_type.upper(),
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "run": {"runId": run_id or f"{job_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"},
        "job": {"namespace": ns, "name": job_name},
        "inputs": [build_dataset(i, ns) for i in inputs],
        "outputs": [build_dataset(o, ns) for o in outputs],
        "producer": "pyspark_interview_project",
    }
    if metadata:
        event["facets"] = {"job": metadata}
    emit(event, cfg)


def emit_bronze_to_silver(cfg: dict, table: str, run_id: str) -> None:
    ns = _get_namespace(cfg)
    emit_job_event(
        cfg,
        job_name=f"bronze_to_silver_{table}",
        inputs=[f"s3://my-etl-lake-demo/bronze/{table}"],
        outputs=[f"s3://my-etl-lake-demo/silver/{table}"],
        event_type="COMPLETE",
        run_id=run_id,
    )


def emit_silver_to_gold(cfg: dict, table: str, run_id: str) -> None:
    emit_job_event(
        cfg,
        job_name=f"silver_to_gold_{table}",
        inputs=[f"s3://my-etl-lake-demo/silver/{table}"],
        outputs=[f"s3://my-etl-lake-demo/gold/{table}"],
        event_type="COMPLETE",
        run_id=run_id,
    )









