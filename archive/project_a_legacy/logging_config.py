import json
import logging
import sys
import time
import uuid


class JsonFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "ts": int(time.time() * 1000),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            base.update(record.extra)
        return json.dumps(base)


def configure_logging(run_id: str = None):
    """Configure structured JSON logging with run_id tracking"""
    if run_id is None:
        run_id = new_run_id()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [handler]

    # Add run_id to all log records
    logging.getLogger().info("Logging configured", extra={"run_id": run_id})
    return root, run_id


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def log_metric(
    logger: logging.Logger, metric_name: str, value: float, run_id: str = None, **kwargs
):
    """Log a metric with structured format"""
    extra = {"metric_name": metric_name, "metric_value": value, "metric_type": "gauge"}
    if run_id:
        extra["run_id"] = run_id
    extra.update(kwargs)
    logger.info(f"METRIC: {metric_name}={value}", extra=extra)


def log_pipeline_event(
    logger: logging.Logger, event: str, stage: str = None, run_id: str = None, **kwargs
):
    """Log pipeline events with structured format"""
    extra = {"event": event, "event_type": "pipeline"}
    if stage:
        extra["stage"] = stage
    if run_id:
        extra["run_id"] = run_id
    extra.update(kwargs)
    logger.info(f"PIPELINE: {event}", extra=extra)
