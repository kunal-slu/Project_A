"""
Lineage tracking for data pipeline operations.
"""

import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class LineageEvent:
    """Represents a lineage event in the data pipeline."""

    event_id: str
    event_type: str  # 'ingest', 'transform', 'load', 'validate'
    source_path: str
    target_path: str
    timestamp: datetime
    metadata: dict[str, Any]
    status: str = "started"
    error_message: str | None = None


class LineageTracker:
    """
    Tracks data lineage across the pipeline.
    """

    def __init__(self):
        self.events: list[LineageEvent] = []
        self.current_event: LineageEvent | None = None

    def start_event(
        self, event_type: str, source_path: str, target_path: str, metadata: dict[str, Any] = None
    ) -> str:
        """Start tracking a new lineage event."""
        event_id = f"{event_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self.current_event = LineageEvent(
            event_id=event_id,
            event_type=event_type,
            source_path=source_path,
            target_path=target_path,
            timestamp=datetime.now(),
            metadata=metadata or {},
        )

        self.events.append(self.current_event)
        logger.info(f"Started lineage tracking: {event_id}")
        return event_id

    def complete_event(self, event_id: str, status: str = "completed", error_message: str = None):
        """Mark a lineage event as completed."""
        for event in self.events:
            if event.event_id == event_id:
                event.status = status
                event.error_message = error_message
                logger.info(f"Completed lineage event: {event_id} with status: {status}")
                break

    def get_lineage_summary(self) -> dict[str, Any]:
        """Get a summary of all lineage events."""
        return {
            "total_events": len(self.events),
            "events": [asdict(event) for event in self.events],
            "summary_by_type": self._get_summary_by_type(),
        }

    def _get_summary_by_type(self) -> dict[str, int]:
        """Get count of events by type."""
        summary = {}
        for event in self.events:
            summary[event.event_type] = summary.get(event.event_type, 0) + 1
        return summary

    def export_lineage(self, output_path: str):
        """Export lineage data to a file."""
        try:
            import json

            with open(output_path, "w") as f:
                json.dump(self.get_lineage_summary(), f, indent=2, default=str)
            logger.info(f"Lineage exported to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export lineage: {e}")
