"""
Data Lineage Tracking System for Project_A

Tracks data movement and transformations across the entire pipeline:
bronze → silver → gold
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class LineageEvent:
    """Represents a single data lineage event"""

    event_id: str
    source_dataset: str
    target_dataset: str
    transformation: str
    timestamp: datetime
    job_id: str
    records_processed: int
    duration_ms: int
    success: bool
    error_message: str | None = None
    metadata: dict[str, Any] | None = None


class LineageTracker:
    """Centralized data lineage tracker"""

    def __init__(self, lineage_storage_path: str = "data/lineage"):
        self.remote_root = str(lineage_storage_path).startswith("s3://") or str(
            lineage_storage_path
        ).startswith("s3a://")
        if self.remote_root:
            self.lineage_storage_path = None
            self.lineage_storage_path_str = lineage_storage_path.replace("s3a://", "s3://")
        else:
            self.lineage_storage_path = Path(lineage_storage_path)
            self.lineage_storage_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def track_transformation(
        self,
        source_dataset: str,
        target_dataset: str,
        transformation: str,
        job_id: str,
        records_processed: int,
        duration_ms: int,
        success: bool,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Track a data transformation event"""
        event_id = str(uuid.uuid4())
        event = LineageEvent(
            event_id=event_id,
            source_dataset=source_dataset,
            target_dataset=target_dataset,
            transformation=transformation,
            timestamp=datetime.utcnow(),
            job_id=job_id,
            records_processed=records_processed,
            duration_ms=duration_ms,
            success=success,
            error_message=error_message,
            metadata=metadata or {},
        )

        # Save lineage event
        self._save_lineage_event(event)

        # Update dataset lineage
        self._update_dataset_lineage(source_dataset, target_dataset, event_id)

        self.logger.info(f"Lineage tracked: {source_dataset} → {target_dataset}")
        return event_id

    def _save_lineage_event(self, event: LineageEvent):
        """Save lineage event to storage"""
        filename = f"lineage_{event.timestamp.strftime('%Y%m%d_%H%M%S')}_{event.event_id}.json"
        if self.remote_root:
            import boto3

            _, _, bucket_key = self.lineage_storage_path_str.partition("s3://")
            bucket, _, key = bucket_key.partition("/")
            key_prefix = key or "lineage"
            object_key = f"{key_prefix.rstrip('/')}/{filename}"
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=object_key,
                Body=json.dumps(asdict(event), default=str, indent=2).encode("utf-8"),
            )
            return

        filepath = self.lineage_storage_path / filename
        with open(filepath, "w") as f:
            json.dump(asdict(event), f, default=str, indent=2)

    def _update_dataset_lineage(self, source_dataset: str, target_dataset: str, event_id: str):
        """Update dataset-level lineage tracking"""
        if self.remote_root:
            return
        # Track source dataset lineage
        source_file = (
            self.lineage_storage_path / f"dataset_{source_dataset.replace('/', '_')}_upstream.json"
        )
        source_lineage = self._load_dataset_lineage(source_file)
        source_lineage.setdefault("downstream_datasets", []).append(target_dataset)
        source_lineage["last_updated"] = datetime.utcnow().isoformat()

        with open(source_file, "w") as f:
            json.dump(source_lineage, f, indent=2)

        # Track target dataset lineage
        target_file = (
            self.lineage_storage_path
            / f"dataset_{target_dataset.replace('/', '_')}_downstream.json"
        )
        target_lineage = self._load_dataset_lineage(target_file)
        target_lineage.setdefault("upstream_datasets", []).append(source_dataset)
        target_lineage["last_updated"] = datetime.utcnow().isoformat()

        with open(target_file, "w") as f:
            json.dump(target_lineage, f, indent=2)

    def _load_dataset_lineage(self, filepath: Path) -> dict[str, Any]:
        """Load existing dataset lineage or return empty dict"""
        if filepath.exists():
            with open(filepath) as f:
                return json.load(f)
        return {"upstream_datasets": [], "downstream_datasets": []}

    def get_lineage_graph(self, dataset_name: str) -> dict[str, Any]:
        """Get complete lineage graph for a dataset"""
        upstream_file = (
            self.lineage_storage_path / f"dataset_{dataset_name.replace('/', '_')}_upstream.json"
        )
        downstream_file = (
            self.lineage_storage_path / f"dataset_{dataset_name.replace('/', '_')}_downstream.json"
        )

        result = {"dataset": dataset_name, "upstream": [], "downstream": [], "last_updated": None}

        if upstream_file.exists():
            with open(upstream_file) as f:
                lineage = json.load(f)
                result["upstream"] = lineage.get("upstream_datasets", [])
                result["last_updated"] = lineage.get("last_updated")

        if downstream_file.exists():
            with open(downstream_file) as f:
                lineage = json.load(f)
                result["downstream"] = lineage.get("downstream_datasets", [])

        return result


class LineageDecorator:
    """Decorator to automatically track lineage for transformation functions"""

    def __init__(self, lineage_tracker: LineageTracker):
        self.tracker = lineage_tracker

    def __call__(self, source_dataset: str, target_dataset: str, transformation: str):
        def decorator(func):
            def wrapper(*args, **kwargs):
                job_id = kwargs.get("job_id", str(uuid.uuid4()))
                start_time = datetime.utcnow()

                try:
                    result = func(*args, **kwargs)
                    end_time = datetime.utcnow()
                    duration_ms = int((end_time - start_time).total_seconds() * 1000)

                    # Extract record count from result if possible
                    records_processed = self._extract_record_count(result)

                    self.tracker.track_transformation(
                        source_dataset=source_dataset,
                        target_dataset=target_dataset,
                        transformation=transformation,
                        job_id=job_id,
                        records_processed=records_processed,
                        duration_ms=duration_ms,
                        success=True,
                    )

                    return result
                except Exception as e:
                    end_time = datetime.utcnow()
                    duration_ms = int((end_time - start_time).total_seconds() * 1000)

                    self.tracker.track_transformation(
                        source_dataset=source_dataset,
                        target_dataset=target_dataset,
                        transformation=transformation,
                        job_id=job_id,
                        records_processed=0,
                        duration_ms=duration_ms,
                        success=False,
                        error_message=str(e),
                    )
                    raise

            return wrapper

        return decorator

    def _extract_record_count(self, result) -> int:
        """Extract record count from transformation result"""
        if hasattr(result, "count"):
            # Assume it's a DataFrame
            try:
                return result.count()
            except Exception:
                pass
        elif isinstance(result, list | tuple):
            return len(result)
        elif isinstance(result, dict) and "count" in result:
            return result["count"]
        return 0


# Global lineage tracker instance
_lineage_tracker = None


def get_lineage_tracker() -> LineageTracker:
    """Get the global lineage tracker instance."""
    global _lineage_tracker
    if _lineage_tracker is None:
        from project_a.utils.config import load_config_resolved

        config = load_config_resolved(None, "local")
        lineage_path = config.get("paths", {}).get("lineage_root", "data/lineage")
        _lineage_tracker = LineageTracker(lineage_path)
    return _lineage_tracker


def track_lineage(source_dataset: str, target_dataset: str, transformation: str):
    """Decorator to track lineage for a transformation function"""
    tracker = get_lineage_tracker()
    decorator = LineageDecorator(tracker)
    return decorator(source_dataset, target_dataset, transformation)
