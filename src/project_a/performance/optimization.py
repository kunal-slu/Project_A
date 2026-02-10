"""
Performance Optimization System for Project_A

Manages performance benchmarking, optimization, and monitoring.
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd
import psutil


class PerformanceMetric(Enum):
    EXECUTION_TIME = "execution_time"
    MEMORY_USAGE = "memory_usage"
    CPU_USAGE = "cpu_usage"
    IO_THROUGHPUT = "io_throughput"
    PARTITION_COUNT = "partition_count"
    RECORD_PROCESSING_RATE = "record_processing_rate"


@dataclass
class PerformanceResult:
    """Result of a performance measurement"""

    metric: PerformanceMetric
    value: float
    unit: str
    timestamp: datetime
    context: dict[str, Any]  # Additional context about the measurement


@dataclass
class OptimizationRecommendation:
    """A performance optimization recommendation"""

    recommendation_id: str
    title: str
    description: str
    severity: str  # critical, high, medium, low
    estimated_improvement: float  # percentage improvement
    implementation_effort: str  # high, medium, low
    affected_components: list[str]


class PerformanceMonitor:
    """Monitors performance metrics during execution"""

    def __init__(self, metrics_path: str = "data/performance_metrics"):
        self.metrics_path = Path(metrics_path)
        self.metrics_path.mkdir(parents=True, exist_ok=True)
        self.metrics_file = self.metrics_path / "performance.jsonl"
        self.logger = logging.getLogger(__name__)
        self.monitoring = False
        self.monitor_thread = None
        self.system_metrics = []
        self.collection_interval = 1  # seconds

    def start_monitoring(self):
        """Start system-wide performance monitoring"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._collect_system_metrics)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        self.logger.info("Performance monitoring started")

    def stop_monitoring(self):
        """Stop performance monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
        self.logger.info("Performance monitoring stopped")

    def _collect_system_metrics(self):
        """Collect system metrics in background thread"""
        while self.monitoring:
            try:
                metric = PerformanceResult(
                    metric=PerformanceMetric.CPU_USAGE,
                    value=psutil.cpu_percent(),
                    unit="%",
                    timestamp=datetime.utcnow(),
                    context={"measurement_type": "system_cpu"},
                )
                self.system_metrics.append(metric)

                metric = PerformanceResult(
                    metric=PerformanceMetric.MEMORY_USAGE,
                    value=psutil.virtual_memory().percent,
                    unit="%",
                    timestamp=datetime.utcnow(),
                    context={"measurement_type": "system_memory"},
                )
                self.system_metrics.append(metric)

                time.sleep(self.collection_interval)
            except Exception as e:
                self.logger.error(f"Error collecting system metrics: {e}")
                time.sleep(self.collection_interval)

    def measure_execution_time(self, func: Callable, *args, **kwargs) -> dict[str, Any]:
        """Measure execution time of a function"""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

        result = func(*args, **kwargs)

        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

        execution_time = end_time - start_time
        memory_delta = end_memory - start_memory

        # Log metrics
        self._log_metric(
            PerformanceMetric.EXECUTION_TIME,
            execution_time,
            "seconds",
            {"function_name": func.__name__, "args_count": len(args)},
        )
        self._log_metric(
            PerformanceMetric.MEMORY_USAGE,
            memory_delta,
            "MB",
            {"function_name": func.__name__, "type": "delta"},
        )

        return {
            "result": result,
            "execution_time": execution_time,
            "memory_used_mb": memory_delta,
            "start_memory_mb": start_memory,
            "end_memory_mb": end_memory,
        }

    def benchmark_data_processing(
        self, df: pd.DataFrame, operation: str = "transform"
    ) -> dict[str, Any]:
        """Benchmark data processing operations"""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024

        # Perform the operation
        if operation == "transform":
            # Example transformation
            result_df = df.copy()
        elif operation == "aggregate":
            result_df = df.groupby(df.columns[0]).size().reset_index(name="count")
        elif operation == "join":
            # Self-join for demonstration
            result_df = df.merge(df.head(100), on=df.columns[0], suffixes=("", "_right"))
        else:
            result_df = df  # default

        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024

        execution_time = end_time - start_time
        memory_delta = end_memory - start_memory

        # Calculate throughput
        record_count = len(df)
        throughput = record_count / execution_time if execution_time > 0 else 0

        # Log metrics
        self._log_metric(
            PerformanceMetric.EXECUTION_TIME,
            execution_time,
            "seconds",
            {"operation": operation, "record_count": record_count},
        )
        self._log_metric(
            PerformanceMetric.RECORD_PROCESSING_RATE,
            throughput,
            "records/second",
            {"operation": operation, "record_count": record_count},
        )

        return {
            "execution_time": execution_time,
            "throughput_records_per_second": throughput,
            "memory_delta_mb": memory_delta,
            "input_record_count": record_count,
            "output_record_count": len(result_df),
            "operation": operation,
        }

    def _log_metric(
        self, metric: PerformanceMetric, value: float, unit: str, context: dict[str, Any]
    ):
        """Log a performance metric"""
        perf_result = PerformanceResult(
            metric=metric, value=value, unit=unit, timestamp=datetime.utcnow(), context=context
        )

        with open(self.metrics_file, "a") as f:
            f.write(
                json.dumps(
                    {
                        "metric": perf_result.metric.value,
                        "value": perf_result.value,
                        "unit": perf_result.unit,
                        "timestamp": perf_result.timestamp.isoformat(),
                        "context": perf_result.context,
                    }
                )
                + "\n"
            )

    def get_performance_summary(self, hours_back: int = 24) -> dict[str, Any]:
        """Get performance summary for recent period"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)

        # Read recent metrics
        metrics = []
        with open(self.metrics_file) as f:
            for line in f:
                try:
                    metric = json.loads(line.strip())
                    metric_time = datetime.fromisoformat(metric["timestamp"])
                    if metric_time >= cutoff_time:
                        metrics.append(metric)
                except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                    continue  # Skip malformed lines

        if not metrics:
            return {"error": "No metrics available for analysis"}

        # Group metrics by type
        metric_groups = {}
        for metric in metrics:
            metric_type = metric["metric"]
            if metric_type not in metric_groups:
                metric_groups[metric_type] = []
            metric_groups[metric_type].append(metric)

        summary = {
            "period_hours": hours_back,
            "total_metrics": len(metrics),
            "metric_types": list(metric_groups.keys()),
            "averages": {},
            "peaks": {},
            "recommendations": [],
        }

        # Calculate averages and peaks
        for metric_type, metric_list in metric_groups.items():
            values = [m["value"] for m in metric_list]
            summary["averages"][metric_type] = sum(values) / len(values)
            summary["peaks"][metric_type] = max(values)

        # Generate recommendations based on performance
        if summary["averages"].get("execution_time", 0) > 10:  # More than 10 seconds average
            summary["recommendations"].append(
                {
                    "category": "execution_time",
                    "issue": "Slow execution times",
                    "suggestion": "Consider optimizing algorithms or increasing resources",
                    "severity": "high",
                }
            )

        if summary["averages"].get("memory_usage", 0) > 80:  # More than 80% memory usage
            summary["recommendations"].append(
                {
                    "category": "memory",
                    "issue": "High memory usage",
                    "suggestion": "Optimize memory usage or increase available memory",
                    "severity": "high",
                }
            )

        return summary


class SparkOptimizer:
    """Optimizes Spark configurations and operations"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.optimization_settings = {
            "auto_broadcast_join_threshold": "100MB",
            "adaptive_query_execution": True,
            "coalesce_partitions_factor": 0.8,
            "shuffle_partitions": 200,
            "compression_codec": "snappy",
        }

    def optimize_dataframe_operations(self, df, target_partitions: int = None):
        """Optimize DataFrame operations"""
        # Repartition if needed
        if target_partitions and df.rdd.getNumPartitions() != target_partitions:
            df = df.repartition(target_partitions)
            self.logger.info(f"Repartitioned DataFrame to {target_partitions} partitions")

        # Cache if DataFrame will be reused
        # df.cache()  # Uncomment in actual Spark environment

        return df

    def suggest_partitioning_strategy(
        self, df, operation_type: str = "transform"
    ) -> dict[str, Any]:
        """Suggest optimal partitioning strategy"""
        record_count = (
            df.count() if hasattr(df, "count") else len(df)
        )  # Handle both Spark and Pandas
        avg_partition_size = 128 * 1024 * 1024  # 128MB per partition (typical recommendation)

        # Estimate optimal number of partitions
        if hasattr(df, "rdd"):  # Spark DataFrame
            # Estimate size per record
            sample_size = min(1000, record_count)
            if sample_size > 0:
                df.limit(sample_size)
                # In a real Spark environment, we'd calculate actual size
                estimated_total_size = record_count * 1000  # Placeholder
                optimal_partitions = max(1, int(estimated_total_size / avg_partition_size))
            else:
                optimal_partitions = 1
        else:  # Pandas DataFrame
            # Estimate based on memory usage
            import sys

            estimated_size = sys.getsizeof(df)
            optimal_partitions = max(1, int(estimated_size / avg_partition_size))

        current_partitions = df.rdd.getNumPartitions() if hasattr(df, "rdd") else 1

        suggestion = {
            "current_partitions": current_partitions,
            "recommended_partitions": optimal_partitions,
            "record_count": record_count,
            "operation_type": operation_type,
            "should_repartition": abs(current_partitions - optimal_partitions)
            > optimal_partitions * 0.3,
            "reasoning": f"Optimal partition size is ~{avg_partition_size / (1024 * 1024):.0f}MB per partition",
        }

        return suggestion

    def optimize_join_operations(self, left_df, right_df, join_type: str = "inner"):
        """Optimize join operations"""
        left_count = left_df.count() if hasattr(left_df, "count") else len(left_df)
        right_count = right_df.count() if hasattr(right_df, "count") else len(right_df)

        # Determine which side should be broadcast
        broadcast_threshold = 100000  # 100k records

        if min(left_count, right_count) < broadcast_threshold:
            # Use broadcast join
            if left_count < right_count:
                # Broadcast left side
                # In real Spark: left_df.hint("broadcast").join(right_df, ...)
                optimization_type = "broadcast_left"
            else:
                # Broadcast right side
                # In real Spark: left_df.join(broadcast(right_df), ...)
                optimization_type = "broadcast_right"
        else:
            # Use regular join, ensure proper partitioning
            optimization_type = "regular_join"

        return {
            "optimization_type": optimization_type,
            "left_count": left_count,
            "right_count": right_count,
            "broadcast_threshold": broadcast_threshold,
            "recommendation": f"Use {optimization_type} for optimal performance",
        }


class PerformanceAnalyzer:
    """Analyzes performance and provides optimization recommendations"""

    def __init__(self, monitor: PerformanceMonitor, optimizer: SparkOptimizer):
        self.monitor = monitor
        self.optimizer = optimizer
        self.logger = logging.getLogger(__name__)

    def analyze_dataframe_performance(
        self, df, operation_type: str = "transform"
    ) -> list[OptimizationRecommendation]:
        """Analyze DataFrame performance and suggest optimizations"""
        recommendations = []

        # Analyze partitioning
        partition_analysis = self.optimizer.suggest_partitioning_strategy(df, operation_type)

        if partition_analysis["should_repartition"]:
            recommendations.append(
                OptimizationRecommendation(
                    recommendation_id=f"partition_opt_{operation_type}",
                    title="Optimize Partitioning",
                    description=f"Current partitions: {partition_analysis['current_partitions']}, Recommended: {partition_analysis['recommended_partitions']}",
                    severity="medium",
                    estimated_improvement=15.0,  # 15% improvement estimate
                    implementation_effort="low",
                    affected_components=["spark_dataframe"],
                )
            )

        # Analyze join operations if applicable
        if operation_type == "join":
            # This would require two DataFrames in a real implementation
            pass

        return recommendations

    def generate_performance_report(self, hours_back: int = 24) -> dict[str, Any]:
        """Generate comprehensive performance report"""
        # Get performance summary
        perf_summary = self.monitor.get_performance_summary(hours_back)

        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "period_hours": hours_back,
            "performance_summary": perf_summary,
            "optimization_recommendations": [],
            "benchmark_results": {},
        }

        # Add more detailed recommendations
        if "recommendations" in perf_summary:
            for rec in perf_summary["recommendations"]:
                report["optimization_recommendations"].append(
                    {
                        "category": rec["category"],
                        "issue": rec["issue"],
                        "suggestion": rec["suggestion"],
                        "severity": rec.get("severity", "medium"),
                    }
                )

        return report


# Global instances
_performance_monitor = None
_spark_optimizer = None
_performance_analyzer = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance"""
    global _performance_monitor
    if _performance_monitor is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        metrics_path = config.get("paths", {}).get(
            "performance_metrics_root", "data/performance_metrics"
        )
        _performance_monitor = PerformanceMonitor(metrics_path)
    return _performance_monitor


def get_spark_optimizer() -> SparkOptimizer:
    """Get the global Spark optimizer instance"""
    global _spark_optimizer
    if _spark_optimizer is None:
        _spark_optimizer = SparkOptimizer()
    return _spark_optimizer


def get_performance_analyzer() -> PerformanceAnalyzer:
    """Get the global performance analyzer instance"""
    global _performance_analyzer
    if _performance_analyzer is None:
        monitor = get_performance_monitor()
        optimizer = get_spark_optimizer()
        _performance_analyzer = PerformanceAnalyzer(monitor, optimizer)
    return _performance_analyzer


def measure_execution_time(func: Callable, *args, **kwargs) -> dict[str, Any]:
    """Measure execution time of a function"""
    monitor = get_performance_monitor()
    return monitor.measure_execution_time(func, *args, **kwargs)


def benchmark_data_processing(df: pd.DataFrame, operation: str = "transform") -> dict[str, Any]:
    """Benchmark data processing operations"""
    monitor = get_performance_monitor()
    return monitor.benchmark_data_processing(df, operation)


def analyze_dataframe_performance(
    df, operation_type: str = "transform"
) -> list[OptimizationRecommendation]:
    """Analyze DataFrame performance and suggest optimizations"""
    analyzer = get_performance_analyzer()
    return analyzer.analyze_dataframe_performance(df, operation_type)


def generate_performance_report(hours_back: int = 24) -> dict[str, Any]:
    """Generate comprehensive performance report"""
    analyzer = get_performance_analyzer()
    return analyzer.generate_performance_report(hours_back)


def suggest_partitioning_strategy(df, operation_type: str = "transform") -> dict[str, Any]:
    """Suggest optimal partitioning strategy"""
    optimizer = get_spark_optimizer()
    return optimizer.suggest_partitioning_strategy(df, operation_type)


def optimize_join_operations(left_df, right_df, join_type: str = "inner"):
    """Optimize join operations"""
    optimizer = get_spark_optimizer()
    return optimizer.optimize_join_operations(left_df, right_df, join_type)
