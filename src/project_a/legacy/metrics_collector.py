"""
Metrics Collector for Prometheus/OpenTelemetry Integration
Parses pipeline metrics and exposes them for monitoring systems.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from pathlib import Path
import time

# Prometheus client for metrics export
try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary,
        generate_latest, CONTENT_TYPE_LATEST,
        CollectorRegistry, multiprocess
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Mock classes for when prometheus_client is not available
    class MockMetric:
        def __init__(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def time(self): return MockContext()

    class MockContext:
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass

    Counter = Gauge = Histogram = Summary = MockMetric

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Collects and exposes metrics from pipeline runs for monitoring systems.
    Supports Prometheus and OpenTelemetry formats.
    """

    def __init__(self, metrics_dir: str = "data/checkpoints/ingestion"):
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Prometheus metrics
        if PROMETHEUS_AVAILABLE:
            self.registry = CollectorRegistry()
            self._init_prometheus_metrics()
        else:
            logger.warning("Prometheus client not available, using mock metrics")
            self._init_mock_metrics()

    def _init_prometheus_metrics(self):
        """Initialize Prometheus metrics."""
        # Pipeline execution metrics
        self.pipeline_runs_total = Counter(
            'pipeline_runs_total',
            'Total number of pipeline runs',
            ['pipeline_name', 'status'],
            registry=self.registry
        )

        self.pipeline_duration_seconds = Histogram(
            'pipeline_duration_seconds',
            'Pipeline execution duration in seconds',
            ['pipeline_name'],
            registry=self.registry
        )

        # Data quality metrics
        self.data_quality_issues_total = Counter(
            'data_quality_issues_total',
            'Total number of data quality issues',
            ['table_name', 'issue_type'],
            registry=self.registry
        )

        self.records_processed_total = Counter(
            'records_processed_total',
            'Total number of records processed',
            ['table_name', 'layer'],
            registry=self.registry
        )

        # Performance metrics
        self.table_optimization_duration_seconds = Histogram(
            'table_optimization_duration_seconds',
            'Table optimization duration in seconds',
            ['table_name'],
            registry=self.registry
        )

        self.ingestion_latency_seconds = Histogram(
            'ingestion_latency_seconds',
            'Data ingestion latency in seconds',
            ['source', 'target_layer'],
            registry=self.registry
        )

        # Business metrics
        self.customer_count = Gauge(
            'customer_count',
            'Total number of customers',
            ['segment'],
            registry=self.registry
        )

        self.order_count = Gauge(
            'order_count',
            'Total number of orders',
            ['status'],
            registry=self.registry
        )

        self.return_rate = Gauge(
            'return_rate',
            'Product return rate percentage',
            ['category'],
            registry=self.registry
        )

        self.inventory_level = Gauge(
            'inventory_level',
            'Current inventory levels',
            ['product_id', 'stock_level'],
            registry=self.registry
        )

    def _init_mock_metrics(self):
        """Initialize mock metrics when Prometheus is not available."""
        self.pipeline_runs_total = MockMetric()
        self.pipeline_duration_seconds = MockMetric()
        self.data_quality_issues_total = MockMetric()
        self.records_processed_total = MockMetric()
        self.table_optimization_duration_seconds = MockMetric()
        self.ingestion_latency_seconds = MockMetric()
        self.customer_count = MockMetric()
        self.order_count = MockMetric()
        self.return_rate = MockMetric()
        self.inventory_level = MockMetric()

    def collect_pipeline_metrics(self) -> Dict[str, Any]:
        """Collect all pipeline metrics from JSON files."""
        metrics_summary = {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "total_duration": 0,
            "avg_duration": 0,
            "data_quality_issues": 0,
            "records_processed": 0,
            "last_run": None,
            "pipeline_performance": {}
        }

        try:
            # Find all metrics files
            metrics_files = list(self.metrics_dir.glob("pipeline_metrics_*.json"))

            if not metrics_files:
                logger.info("No pipeline metrics files found")
                return metrics_summary

            # Sort by modification time (newest first)
            metrics_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

            total_duration = 0
            successful_runs = 0
            failed_runs = 0
            total_quality_issues = 0
            total_records = 0

            for metrics_file in metrics_files:
                try:
                    with open(metrics_file, 'r') as f:
                        metrics = json.load(f)

                    # Update summary metrics
                    metrics_summary["total_runs"] += 1
                    total_duration += metrics.get("pipeline_duration_seconds", 0)

                    if metrics.get("status") == "success":
                        successful_runs += 1
                    else:
                        failed_runs += 1

                    # Data quality issues
                    bronze_metrics = metrics.get("bronze", {})
                    total_quality_issues += bronze_metrics.get("quality_issues", 0)
                    total_records += bronze_metrics.get("records_processed", 0)

                    # Update Prometheus metrics
                    self._update_prometheus_metrics(metrics)

                    # Track last run
                    if not metrics_summary["last_run"]:
                        metrics_summary["last_run"] = metrics_file.name

                except Exception as e:
                    logger.warning(f"Failed to parse metrics file {metrics_file}: {e}")
                    continue

            # Calculate averages
            if metrics_summary["total_runs"] > 0:
                metrics_summary["successful_runs"] = successful_runs
                metrics_summary["failed_runs"] = failed_runs
                metrics_summary["total_duration"] = total_duration
                metrics_summary["avg_duration"] = total_duration / metrics_summary["total_runs"]
                metrics_summary["data_quality_issues"] = total_quality_issues
                metrics_summary["records_processed"] = total_records

            logger.info(f"Collected metrics from {len(metrics_files)} pipeline runs")

        except Exception as e:
            logger.error(f"Failed to collect pipeline metrics: {e}")

        return metrics_summary

    def _update_prometheus_metrics(self, metrics: Dict[str, Any]):
        """Update Prometheus metrics from pipeline metrics."""
        try:
            pipeline_name = "automated_ingestion"
            status = metrics.get("status", "unknown")
            duration = metrics.get("pipeline_duration_seconds", 0)

            # Pipeline execution metrics
            self.pipeline_runs_total.labels(
                pipeline_name=pipeline_name,
                status=status
            ).inc()

            if duration > 0:
                self.pipeline_duration_seconds.labels(
                    pipeline_name=pipeline_name
                ).observe(duration)

            # Data quality metrics
            bronze_metrics = metrics.get("bronze", {})
            quality_issues = bronze_metrics.get("quality_issues", 0)
            records_processed = bronze_metrics.get("records_processed", 0)

            if quality_issues > 0:
                self.data_quality_issues_total.labels(
                    table_name="all_tables",
                    issue_type="validation_failure"
                ).inc(quality_issues)

            if records_processed > 0:
                self.records_processed_total.labels(
                    table_name="all_tables",
                    layer="bronze"
                ).inc(records_processed)

            # Silver layer metrics
            silver_metrics = metrics.get("silver", {})
            silver_records = silver_metrics.get("records_transformed", 0)
            if silver_records > 0:
                self.records_processed_total.labels(
                    table_name="all_tables",
                    layer="silver"
                ).inc(silver_records)

            # Gold layer metrics
            gold_metrics = metrics.get("gold", {})
            gold_metrics_count = gold_metrics.get("metrics_created", 0)
            if gold_metrics_count > 0:
                self.records_processed_total.labels(
                    table_name="all_tables",
                    layer="gold"
                ).inc(gold_metrics_count)

            # Performance metrics
            optimization_metrics = metrics.get("optimization", {})
            optimization_time = optimization_metrics.get("optimization_time", 0)
            if optimization_time > 0:
                self.table_optimization_duration_seconds.labels(
                    table_name="all_tables"
                ).observe(optimization_time)

        except Exception as e:
            logger.warning(f"Failed to update Prometheus metrics: {e}")

    def get_prometheus_metrics(self) -> str:
        """Get metrics in Prometheus format."""
        if not PROMETHEUS_AVAILABLE:
            return "# Prometheus client not available"

        try:
            # Collect latest metrics
            self.collect_pipeline_metrics()

            # Generate Prometheus format
            return generate_latest(self.registry)
        except Exception as e:
            logger.error(f"Failed to generate Prometheus metrics: {e}")
            return f"# Error generating metrics: {e}"

    def get_open_telemetry_metrics(self) -> Dict[str, Any]:
        """Get metrics in OpenTelemetry format."""
        try:
            # Collect latest metrics
            summary = self.collect_pipeline_metrics()

            # Convert to OpenTelemetry format
            otel_metrics = {
                "resourceMetrics": [{
                    "resource": {
                        "attributes": [{
                            "key": "service.name",
                            "value": {"stringValue": "pyspark-data-engineer"}
                        }]
                    },
                    "scopeMetrics": [{
                        "scope": {
                            "name": "pyspark.interview.project",
                            "version": "1.0.0"
                        },
                        "metrics": [
                            {
                                "name": "pipeline.runs.total",
                                "description": "Total pipeline runs",
                                "unit": "1",
                                "sum": {
                                    "dataPoints": [{
                                        "attributes": [{
                                            "key": "status",
                                            "value": {"stringValue": "total"}
                                        }],
                                        "value": summary["total_runs"],
                                        "timeUnixNano": int(time.time() * 1e9)
                                    }]
                                }
                            },
                            {
                                "name": "pipeline.duration.seconds",
                                "description": "Pipeline duration",
                                "unit": "s",
                                "histogram": {
                                    "dataPoints": [{
                                        "attributes": [],
                                        "count": summary["total_runs"],
                                        "sum": summary["total_duration"],
                                        "explicitBounds": [60, 300, 900, 1800, 3600],
                                        "bucketCounts": [0, 0, 0, 0, 0, 0],
                                        "timeUnixNano": int(time.time() * 1e9)
                                    }]
                                }
                            }
                        ]
                    }]
                }]
            }

            return otel_metrics

        except Exception as e:
            logger.error(f"Failed to generate OpenTelemetry metrics: {e}")
            return {"error": str(e)}

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get a human-readable metrics summary."""
        summary = self.collect_pipeline_metrics()

        # Add business insights
        summary["business_insights"] = {
            "success_rate": f"{(summary['successful_runs'] / max(summary['total_runs'], 1)) * 100:.1f}%" if summary['total_runs'] > 0 else "0%",
            "avg_processing_time": f"{summary['avg_duration']:.2f} seconds",
            "data_quality_score": f"{max(0, 100 - (summary['data_quality_issues'] / max(summary['records_processed'], 1)) * 100):.1f}%" if summary['records_processed'] > 0 else "100%"
        }

        return summary

    def export_metrics_to_file(self, output_path: str, format_type: str = "json"):
        """Export metrics to a file in specified format."""
        try:
            if format_type.lower() == "prometheus":
                content = self.get_prometheus_metrics()
                file_extension = ".prom"
            elif format_type.lower() == "opentelemetry":
                content = self.get_open_telemetry_metrics()
                file_extension = ".json"
            else:
                content = self.get_metrics_summary()
                file_extension = ".json"

            # Ensure output path has correct extension
            if not output_path.endswith(file_extension):
                output_path += file_extension

            with open(output_path, 'w') as f:
                if format_type.lower() == "prometheus":
                    f.write(content)
                else:
                    json.dump(content, f, indent=2, default=str)

            logger.info(f"Metrics exported to {output_path}")

        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")

    def create_metrics_dashboard_data(self) -> Dict[str, Any]:
        """Create data for a metrics dashboard."""
        try:
            summary = self.collect_pipeline_metrics()

            # Create time series data for the last 30 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            dashboard_data = {
                "summary": summary,
                "daily_metrics": [],
                "performance_trends": {
                    "avg_duration_trend": [],
                    "success_rate_trend": [],
                    "quality_score_trend": []
                }
            }

            # Generate sample daily metrics (in real implementation, this would read from time-series storage)
            current_date = start_date
            while current_date <= end_date:
                daily_metrics = {
                    "date": current_date.strftime("%Y-%m-%d"),
                    "runs": 0,
                    "successful_runs": 0,
                    "failed_runs": 0,
                    "avg_duration": 0,
                    "quality_issues": 0
                }

                # In a real implementation, you'd aggregate metrics by date
                # For now, we'll use the overall summary
                if current_date == end_date:
                    daily_metrics.update({
                        "runs": summary["total_runs"],
                        "successful_runs": summary["successful_runs"],
                        "failed_runs": summary["failed_runs"],
                        "avg_duration": summary["avg_duration"],
                        "quality_issues": summary["data_quality_issues"]
                    })

                dashboard_data["daily_metrics"].append(daily_metrics)
                current_date += timedelta(days=1)

            return dashboard_data

        except Exception as e:
            logger.error(f"Failed to create dashboard data: {e}")
            return {"error": str(e)}


def main():
    """Main entry point for metrics collection."""
    import argparse

    parser = argparse.ArgumentParser(description="Collect and export pipeline metrics")
    parser.add_argument("--format", choices=["json", "prometheus", "opentelemetry"],
                       default="json", help="Output format")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--dashboard", action="store_true", help="Generate dashboard data")

    args = parser.parse_args()

    # Initialize metrics collector
    collector = MetricsCollector()

    if args.dashboard:
        # Generate dashboard data
        dashboard_data = collector.create_metrics_dashboard_data()
        print(json.dumps(dashboard_data, indent=2, default=str))
    else:
        # Export metrics
        if args.output:
            collector.export_metrics_to_file(args.output, args.format)
        else:
            # Print to stdout
            if args.format == "prometheus":
                print(collector.get_prometheus_metrics())
            elif args.format == "opentelemetry":
                print(json.dumps(collector.get_open_telemetry_metrics(), indent=2))
            else:
                print(json.dumps(collector.get_metrics_summary(), indent=2, default=str))


if __name__ == "__main__":
    main()
