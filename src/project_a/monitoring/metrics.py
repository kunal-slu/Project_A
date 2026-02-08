"""
Metrics Collection and Monitoring System for Project_A

Collects performance metrics, monitors pipeline health, and provides alerting.
"""
import time
import threading
from datetime import datetime
from typing import Dict, Any, Callable, Optional, List
from dataclasses import dataclass
from enum import Enum
import json
import logging
from pathlib import Path
import psutil
import os


class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class Metric:
    """Represents a single metric"""
    name: str
    type: MetricType
    value: float
    labels: Dict[str, str]
    timestamp: datetime


class MetricsCollector:
    """Collects and stores metrics"""
    
    def __init__(self, metrics_path: str = "data/metrics"):
        self.metrics_path = Path(metrics_path)
        self.metrics_path.mkdir(parents=True, exist_ok=True)
        self.metrics_file = self.metrics_path / "metrics.jsonl"
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
    
    def record_metric(self, name: str, value: float, labels: Dict[str, str] = None, metric_type: MetricType = MetricType.GAUGE):
        """Record a metric"""
        metric = Metric(
            name=name,
            type=metric_type,
            value=value,
            labels=labels or {},
            timestamp=datetime.utcnow()
        )
        
        with self.lock:
            with open(self.metrics_file, 'a') as f:
                f.write(json.dumps({
                    'name': metric.name,
                    'type': metric.type.value,
                    'value': metric.value,
                    'labels': metric.labels,
                    'timestamp': metric.timestamp.isoformat()
                }) + '\n')
    
    def increment_counter(self, name: str, labels: Dict[str, str] = None, amount: float = 1.0):
        """Increment a counter metric"""
        self.record_metric(name, amount, labels, MetricType.COUNTER)
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric"""
        self.record_metric(name, value, labels, MetricType.GAUGE)
    
    def observe_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """Observe a histogram metric"""
        self.record_metric(name, value, labels, MetricType.HISTOGRAM)
    
    def collect_system_metrics(self):
        """Collect system-level metrics"""
        self.set_gauge('system_cpu_percent', psutil.cpu_percent())
        self.set_gauge('system_memory_percent', psutil.virtual_memory().percent)
        self.set_gauge('system_disk_percent', psutil.disk_usage('/').percent)
        self.set_gauge('process_memory_mb', psutil.Process().memory_info().rss / 1024 / 1024)


class PipelineMonitor:
    """Monitors pipeline execution and performance"""
    
    def __init__(self, collector: MetricsCollector):
        self.collector = collector
        self.logger = logging.getLogger(__name__)
    
    def monitor_execution(self, job_name: str, func: Callable, *args, **kwargs) -> Any:
        """Monitor execution of a function"""
        start_time = time.time()
        labels = {'job_name': job_name}
        
        try:
            # Increment execution counter
            self.collector.increment_counter('pipeline_executions_total', labels)
            
            # Record start time
            self.collector.set_gauge(f'pipeline_start_time_seconds', start_time, labels)
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Record success metrics
            duration = time.time() - start_time
            self.collector.observe_histogram('pipeline_duration_seconds', duration, labels)
            self.collector.set_gauge('pipeline_success', 1, labels)
            
            self.logger.info(f"Pipeline {job_name} completed successfully in {duration:.2f}s")
            return result
            
        except Exception as e:
            # Record failure metrics
            duration = time.time() - start_time
            self.collector.observe_histogram('pipeline_duration_seconds', duration, labels)
            self.collector.set_gauge('pipeline_success', 0, labels)
            self.collector.increment_counter('pipeline_failures_total', labels)
            
            self.logger.error(f"Pipeline {job_name} failed after {duration:.2f}s: {str(e)}")
            raise


class AlertManager:
    """Manages alerts based on metrics thresholds"""
    
    def __init__(self, metrics_path: str = "data/alerts"):
        self.alerts_path = Path(metrics_path)
        self.alerts_path.mkdir(parents=True, exist_ok=True)
        self.rules = []
        self.logger = logging.getLogger(__name__)
    
    def add_alert_rule(self, name: str, metric_name: str, threshold: float, 
                      comparison: str, labels: Dict[str, str] = None):
        """Add an alert rule"""
        rule = {
            'name': name,
            'metric_name': metric_name,
            'threshold': threshold,
            'comparison': comparison,  # 'gt', 'lt', 'ge', 'le', 'eq', 'ne'
            'labels': labels or {}
        }
        self.rules.append(rule)
    
    def evaluate_alerts(self, metrics_file: str = None):
        """Evaluate all alert rules against recent metrics"""
        if metrics_file is None:
            metrics_file = str(MetricsCollector().metrics_file)
        
        # Read recent metrics
        recent_metrics = self._read_recent_metrics(metrics_file)
        
        triggered_alerts = []
        
        for rule in self.rules:
            for metric in recent_metrics:
                if (metric['name'] == rule['metric_name'] and 
                    self._matches_labels(metric['labels'], rule['labels'])):
                    
                    if self._evaluate_condition(metric['value'], rule['threshold'], rule['comparison']):
                        alert = {
                            'rule_name': rule['name'],
                            'metric_name': metric['name'],
                            'value': metric['value'],
                            'threshold': rule['threshold'],
                            'timestamp': metric['timestamp'],
                            'severity': 'HIGH' if metric['value'] > rule['threshold'] * 1.5 else 'MEDIUM'
                        }
                        triggered_alerts.append(alert)
                        
                        # Log alert
                        self.logger.warning(f"Alert triggered: {alert}")
        
        # Save triggered alerts
        if triggered_alerts:
            self._save_alerts(triggered_alerts)
        
        return triggered_alerts
    
    def _read_recent_metrics(self, metrics_file: str, minutes_back: int = 5):
        """Read recent metrics from file"""
        import datetime
        from dateutil.parser import parse
        
        cutoff_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes_back)
        recent_metrics = []
        
        with open(metrics_file, 'r') as f:
            for line in f:
                try:
                    metric = json.loads(line.strip())
                    metric_time = parse(metric['timestamp'])
                    if metric_time >= cutoff_time:
                        recent_metrics.append(metric)
                except:
                    continue  # Skip malformed lines
        
        return recent_metrics
    
    def _matches_labels(self, metric_labels: Dict[str, str], rule_labels: Dict[str, str]) -> bool:
        """Check if metric labels match rule labels"""
        for key, value in rule_labels.items():
            if metric_labels.get(key) != value:
                return False
        return True
    
    def _evaluate_condition(self, value: float, threshold: float, comparison: str) -> bool:
        """Evaluate comparison condition"""
        if comparison == 'gt':
            return value > threshold
        elif comparison == 'lt':
            return value < threshold
        elif comparison == 'ge':
            return value >= threshold
        elif comparison == 'le':
            return value <= threshold
        elif comparison == 'eq':
            return value == threshold
        elif comparison == 'ne':
            return value != threshold
        return False
    
    def _save_alerts(self, alerts: List[Dict[str, Any]]):
        """Save alerts to file"""
        alert_file = self.alerts_path / f"alerts_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(alert_file, 'w') as f:
            json.dump(alerts, f, indent=2)


# Global instances
_metrics_collector = None
_pipeline_monitor = None
_alert_manager = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance"""
    global _metrics_collector
    if _metrics_collector is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        metrics_path = config.get('paths', {}).get('metrics_root', 'data/metrics')
        _metrics_collector = MetricsCollector(metrics_path)
    return _metrics_collector


def get_pipeline_monitor() -> PipelineMonitor:
    """Get the global pipeline monitor instance"""
    global _pipeline_monitor
    if _pipeline_monitor is None:
        collector = get_metrics_collector()
        _pipeline_monitor = PipelineMonitor(collector)
    return _pipeline_monitor


def get_alert_manager() -> AlertManager:
    """Get the global alert manager instance"""
    global _alert_manager
    if _alert_manager is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        alerts_path = config.get('paths', {}).get('alerts_root', 'data/alerts')
        _alert_manager = AlertManager(alerts_path)
    return _alert_manager


def monitor_pipeline(job_name: str, func: Callable, *args, **kwargs) -> Any:
    """Monitor execution of a pipeline job"""
    monitor = get_pipeline_monitor()
    return monitor.monitor_execution(job_name, func, *args, **kwargs)


def add_alert_rule(name: str, metric_name: str, threshold: float, comparison: str, labels: Dict[str, str] = None):
    """Add an alert rule"""
    alert_manager = get_alert_manager()
    alert_manager.add_alert_rule(name, metric_name, threshold, comparison, labels)


def evaluate_alerts():
    """Evaluate all alert rules"""
    alert_manager = get_alert_manager()
    return alert_manager.evaluate_alerts()
