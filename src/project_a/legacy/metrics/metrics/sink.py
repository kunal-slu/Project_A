import time
from typing import Dict, Optional, List


class MetricsSink:
    def __init__(self, cfg: Dict, run_id: str = None):
        self.cfg = cfg
        self.run_id = run_id
        self.start_time = time.time()

    def incr(self, name: str, value: float, dims: Optional[Dict[str, str]] = None):
        pass

    def gauge(self, name: str, value: float, dims: Optional[Dict[str, str]] = None):
        pass
    
    def timing(self, name: str, duration_ms: float, dims: Optional[Dict[str, str]] = None):
        pass
    
    def pipeline_metric(self, stage: str, metric_type: str, value: float, 
                       dims: Optional[Dict[str, str]] = None):
        """Log pipeline-specific metrics"""
        metric_name = f"pipeline.{stage}.{metric_type}"
        self.gauge(metric_name, value, dims)
    
    def data_quality_metric(self, table: str, metric_type: str, value: float,
                           dims: Optional[Dict[str, str]] = None):
        """Log data quality metrics"""
        metric_name = f"dq.{table}.{metric_type}"
        self.gauge(metric_name, value, dims)


class StdoutSink(MetricsSink):
    def incr(self, name, value, dims=None):
        metric_data = {
            "metric": name, 
            "type": "counter", 
            "value": value, 
            "dims": dims or {},
            "run_id": self.run_id,
            "timestamp": int(time.time() * 1000)
        }
        print(f"METRIC: {metric_data}")

    def gauge(self, name, value, dims=None):
        metric_data = {
            "metric": name, 
            "type": "gauge", 
            "value": value, 
            "dims": dims or {},
            "run_id": self.run_id,
            "timestamp": int(time.time() * 1000)
        }
        print(f"METRIC: {metric_data}")
    
    def timing(self, name, duration_ms, dims=None):
        metric_data = {
            "metric": name,
            "type": "timing",
            "value": duration_ms,
            "dims": dims or {},
            "run_id": self.run_id,
            "timestamp": int(time.time() * 1000)
        }
        print(f"METRIC: {metric_data}")
    
    def pipeline_metric(self, stage, metric_type, value, dims=None):
        """Log pipeline-specific metrics with enhanced context"""
        metric_data = {
            "metric": f"pipeline.{stage}.{metric_type}",
            "type": "gauge",
            "value": value,
            "dims": {
                "stage": stage,
                "metric_type": metric_type,
                **(dims or {})
            },
            "run_id": self.run_id,
            "timestamp": int(time.time() * 1000)
        }
        print(f"METRIC: {metric_data}")
    
    def data_quality_metric(self, table, metric_type, value, dims=None):
        """Log data quality metrics with enhanced context"""
        metric_data = {
            "metric": f"dq.{table}.{metric_type}",
            "type": "gauge",
            "value": value,
            "dims": {
                "table": table,
                "metric_type": metric_type,
                **(dims or {})
            },
            "run_id": self.run_id,
            "timestamp": int(time.time() * 1000)
        }
        print(f"METRIC: {metric_data}")


def create_metrics(cfg: Dict, run_id: str = None) -> MetricsSink:
    sink = cfg.get("observability", {}).get("sink", "stdout")
    if sink == "cloudwatch":
        # TODO: implement EMF or PutMetricData adapter
        return StdoutSink(cfg, run_id)
    if sink == "log_analytics":
        # TODO: implement Azure Monitor/LA adapter
        return StdoutSink(cfg, run_id)
    return StdoutSink(cfg, run_id)
