"""
Monitoring and observability package for ETL pipeline.
"""
from .metrics import (
    track_job_execution,
    track_stage_duration,
    record_dq_check,
    record_records_processed,
    record_records_failed,
    record_delta_table_metrics,
    record_delta_write,
    record_schema_drift,
    record_error,
    push_metrics_to_gateway,
    get_metrics_text,
    export_metrics_to_file,
    update_resource_metrics,
)
from .lineage_emitter import (
    emit_lineage_event,
    emit_start,
    emit_complete,
    emit_fail,
)
from .alerts import (
    send_slack_alert,
    send_email_alert,
    alert_on_dq_failure,
    alert_on_sla_breach,
)

__all__ = [
    # Metrics
    "track_job_execution",
    "track_stage_duration",
    "record_dq_check",
    "record_records_processed",
    "record_records_failed",
    "record_delta_table_metrics",
    "record_delta_write",
    "record_schema_drift",
    "record_error",
    "push_metrics_to_gateway",
    "get_metrics_text",
    "export_metrics_to_file",
    "update_resource_metrics",
    # Lineage
    "emit_lineage_event",
    "emit_start",
    "emit_complete",
    "emit_fail",
    # Alerts
    "send_slack_alert",
    "send_email_alert",
    "alert_on_dq_failure",
    "alert_on_sla_breach",
]

