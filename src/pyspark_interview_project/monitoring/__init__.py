"""
Monitoring and observability package for ETL pipeline.
"""
from .metrics import (
    put_metric,
    emit_duration,
    emit_count,
    emit_size,
    emit_dq_metrics,
)
try:
    from .lineage_emitter import (
        emit_lineage_event,
        emit_start,
        emit_complete,
        emit_fail,
    )
    from .lineage_decorator import lineage_job
except ImportError:
    # Fallback if lineage not available
    lineage_job = lambda *args, **kwargs: lambda f: f
    def emit_lineage_event(*args, **kwargs): pass
    def emit_start(*args, **kwargs): pass
    def emit_complete(*args, **kwargs): pass
    def emit_fail(*args, **kwargs): pass

try:
    from .alerts import (
        send_slack_alert,
        send_email_alert,
        alert_on_dq_failure,
        alert_on_sla_breach,
    )
except ImportError:
    # Fallback if alerts not available
    def send_slack_alert(*args, **kwargs): pass
    def send_email_alert(*args, **kwargs): pass
    def alert_on_dq_failure(*args, **kwargs): pass
    def alert_on_sla_breach(*args, **kwargs): pass

__all__ = [
    # Metrics
    "put_metric",
    "emit_duration",
    "emit_count",
    "emit_size",
    "emit_dq_metrics",
    # Lineage
    "emit_lineage_event",
    "emit_start",
    "emit_complete",
    "emit_fail",
    "lineage_job",
    # Alerts
    "send_slack_alert",
    "send_email_alert",
    "alert_on_dq_failure",
    "alert_on_sla_breach",
]

