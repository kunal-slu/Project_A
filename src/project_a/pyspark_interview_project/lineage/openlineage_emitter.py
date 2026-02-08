"""OpenLineage emitter for dataset-level lineage.

Reads endpoint and namespace from environment if present:
  - OPENLINEAGE_URL
  - OPENLINEAGE_NAMESPACE
Falls back to config lineage.url and lineage.namespace.
"""

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _get_endpoint(cfg: dict) -> str | None:
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
        data = json.dumps(event).encode("utf-8")
        endpoint = f"{url}/api/v1/lineage"
        req = urllib.request.Request(
            endpoint,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            _ = resp.read()
    except urllib.error.URLError as exc:
        logger.debug("OpenLineage emit failed: %s", exc)


def build_dataset(name: str, namespace: str) -> dict[str, Any]:
    return {"namespace": namespace, "name": name}


def emit_job_event(
    cfg: dict,
    job_name: str,
    inputs: list[str],
    outputs: list[str],
    event_type: str,
    run_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    ns = _get_namespace(cfg)
    event = {
        "eventType": event_type.upper(),
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "run": {"runId": run_id or f"{job_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"},
        "job": {"namespace": ns, "name": job_name},
        "inputs": [build_dataset(i, ns) for i in inputs],
        "outputs": [build_dataset(o, ns) for o in outputs],
        "producer": "project_a",
    }
    if metadata:
        event["facets"] = {"job": metadata}
    emit(event, cfg)


def emit_bronze_to_silver(cfg: dict, table: str, run_id: str) -> None:
    _get_namespace(cfg)
    emit_job_event(
        cfg,
        job_name=f"bronze_to_silver_{table}",
        # Use config-based paths (caller should provide full paths)
        inputs=[f"bronze/{table}"],  # Logical path - caller resolves to physical
        outputs=[f"silver/{table}"],  # Logical path - caller resolves to physical
        event_type="COMPLETE",
        run_id=run_id,
    )


def emit_silver_to_gold(cfg: dict, table: str, run_id: str) -> None:
    emit_job_event(
        cfg,
        job_name=f"silver_to_gold_{table}",
        # Use config-based paths (caller should provide full paths)
        inputs=[f"silver/{table}"],  # Logical path - caller resolves to physical
        outputs=[f"gold/{table}"],  # Logical path - caller resolves to physical
        event_type="COMPLETE",
        run_id=run_id,
    )
