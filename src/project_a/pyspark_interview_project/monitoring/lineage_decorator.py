"""
OpenLineage decorator for automatic lineage tracking.

Provides @lineage_job decorator to wrap job functions with lineage emission.
Reads config/lineage.yaml and posts to HTTP endpoint.
"""

import functools
import json
import logging
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def _load_lineage_config(config: dict[str, Any]) -> dict[str, Any]:
    """Load lineage configuration from config/lineage.yaml."""
    try:
        config_path = Path("config/lineage.yaml")
        if config_path.exists():
            with open(config_path) as f:
                return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"Could not load lineage config: {e}")
    return {}


def _extract_df_metadata(spark, df: DataFrame) -> dict[str, Any]:
    """Extract schema and row count from DataFrame."""
    try:
        # Get schema as dict of {name: dtype}
        schema = {f.name: str(f.dataType) for f in df.schema.fields}

        # Get row count
        row_count = df.count()

        return {"schema": schema, "row_count": row_count}
    except Exception as e:
        logger.warning(f"Could not extract DataFrame metadata: {e}")
        return {}


def _post_lineage_event(
    job_name: str,
    event_type: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    config: dict[str, Any],
    run_id: str = None,
    error: str = None,
) -> bool:
    """POST lineage event to HTTP endpoint using stdlib HTTP client."""
    # Load lineage config
    lineage_config = _load_lineage_config(config)

    if not lineage_config.get("enabled", False):
        return False

    url = lineage_config.get("url", "").rstrip("/") + "/api/v1/lineage"

    if not url or url == "/api/v1/lineage":
        logger.warning("Lineage URL not configured")
        return False

    # Build OpenLineage event
    namespace = lineage_config.get("namespace", "data-platform")

    event = {
        "eventType": event_type,
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "run": {"runId": run_id or f"{job_name}_{datetime.utcnow().timestamp()}", "facets": {}},
        "job": {
            "namespace": namespace,
            "name": job_name,
            "facets": {
                "ownership": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json",
                    "owners": [{"name": "data-engineering-team", "type": "TEAM"}],
                }
            },
        },
        "inputs": [
            {
                "namespace": inp.get("namespace", namespace),
                "name": inp.get("name", inp),
                "facets": {
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                        "fields": [
                            {"name": k, "type": v} for k, v in inp.get("schema", {}).items()
                        ],
                    },
                    "dataQuality": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityDatasetFacet.json",
                        "rowCount": inp.get("row_count", 0),
                    },
                },
            }
            for inp in inputs
        ],
        "outputs": [
            {
                "namespace": out.get("namespace", namespace),
                "name": out.get("name", out),
                "facets": {
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                        "fields": [
                            {"name": k, "type": v} for k, v in out.get("schema", {}).items()
                        ],
                    },
                    "dataQuality": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityDatasetFacet.json",
                        "rowCount": out.get("row_count", 0),
                    },
                },
            }
            for out in outputs
        ],
    }

    # Add error if present
    if error:
        event["run"]["facets"]["errorMessage"] = {"message": error, "programmingLanguage": "python"}

    try:
        data = json.dumps(event).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            _ = resp.read()
        logger.info("✅ Lineage event emitted: %s for %s", event_type, job_name)
        return True
    except urllib.error.URLError as exc:
        logger.warning("Failed to emit lineage event: %s", exc)
        return False


def lineage_job(name: str, inputs: list[str], outputs: list[str], namespace: str | None = None):
    """
    Decorator to automatically emit lineage events for a job.

    Reads config/lineage.yaml, wraps spark job, captures schema/row counts,
    and POSTs to HTTP endpoint.

    Args:
        name: Job name (e.g., 'extract_snowflake_orders')
        inputs: List of input dataset names (e.g., ['snowflake://ORDERS'])
        outputs: List of output dataset names (e.g., ['s3://bucket/bronze/orders'])
        namespace: OpenLineage namespace (defaults to config or env var)

    Example:
        @lineage_job(
            name="extract_snowflake_orders",
            inputs=["snowflake://ORDERS"],
            outputs=["s3://bucket/bronze/snowflake_orders"]
        )
        def extract_orders(spark, config):
            # ... extraction logic ...
            return df
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract config and spark from args/kwargs
            config = None
            spark = None

            # Try to find config in kwargs first
            if "config" in kwargs:
                config = kwargs["config"]
            elif len(args) > 1 and isinstance(args[1], dict):
                config = args[1]

            # Try to find spark in kwargs or args
            if "spark" in kwargs:
                spark = kwargs["spark"]
            elif len(args) > 0:
                # Check if first arg is SparkSession
                first_arg = args[0]
                if hasattr(first_arg, "read") and hasattr(first_arg, "createDataFrame"):
                    spark = first_arg

            # Generate run ID
            run_id = f"{name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"

            # Emit start event
            try:
                _post_lineage_event(
                    job_name=name,
                    event_type="START",
                    inputs=[{"name": inp} for inp in inputs],
                    outputs=[{"name": out} for out in outputs],
                    config=config or {},
                    run_id=run_id,
                )

                # Execute function
                result = func(*args, **kwargs)

                # Extract metadata from result if DataFrame
                input_metadata = {}
                output_metadata = {}

                if isinstance(result, DataFrame) and spark:
                    output_metadata = _extract_df_metadata(spark, result)

                # Emit complete event
                _post_lineage_event(
                    job_name=name,
                    event_type="COMPLETE",
                    inputs=[{"name": inp, **input_metadata} for inp in inputs],
                    outputs=[{"name": out, **output_metadata} for out in outputs],
                    config=config or {},
                    run_id=run_id,
                )

                return result

            except Exception as e:
                # Emit failure event
                logger.error(f"Job {name} failed: {e}")
                _post_lineage_event(
                    job_name=name,
                    event_type="ABORT",
                    inputs=[{"name": inp} for inp in inputs],
                    outputs=[{"name": out} for out in outputs],
                    config=config or {},
                    run_id=run_id,
                    error=str(e),
                )
                raise

        return wrapper

    return decorator
