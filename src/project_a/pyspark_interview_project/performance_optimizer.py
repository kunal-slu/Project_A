"""
Performance Optimization Module for Enterprise Data Platform.

This module provides:
- Intelligent caching strategies
- Performance benchmarking
- Scale optimization for returns_raw dataset
- Shuffle partition optimization
- Broadcast join optimization
- File compaction and I/O tuning
- Performance metrics collection
"""

import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, broadcast
from pyspark.storagelevel import StorageLevel

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics for a dataset."""
    dataset_name: str
    row_count: int
    size_mb: float
    partition_count: int
    cache_hit_ratio: float
    processing_time_seconds: float
    memory_usage_mb: float
    shuffle_read_mb: float
    shuffle_write_mb: float
    storage_level: str
    optimization_applied: List[str]
    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


class CacheManager:
    """Intelligent cache management for DataFrames."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.cached_datasets: Dict[str, DataFrame] = {}
        self.cache_stats: Dict[str, Dict[str, Any]] = {}
        self.cache_hits: Dict[str, int] = {}
        self.cache_misses: Dict[str, int] = {}

        # Storage level thresholds (in MB)
        self.storage_thresholds = {
            "MEMORY_ONLY": 100,      # < 100MB
            "MEMORY_AND_DISK": 500,  # 100MB - 500MB
            "DISK_ONLY": 2000,       # 500MB - 2GB
            "OFF_HEAP": float('inf') # > 2GB
        }

    def smart_cache(self, df: DataFrame, dataset_name: str, strategy: str = "auto") -> DataFrame:
        """Intelligently cache DataFrame based on size and access pattern."""
        try:
            # Estimate dataset size
            estimated_size_mb = self._estimate_size_mb(df)

            # Select storage level based on strategy
            if strategy == "auto":
                storage_level = self._select_optimal_storage_level(estimated_size_mb)
            elif strategy == "memory":
                storage_level = StorageLevel.MEMORY_ONLY
            elif strategy == "disk":
                storage_level = StorageLevel.DISK_ONLY
            elif strategy == "hybrid":
                storage_level = StorageLevel.MEMORY_AND_DISK
            else:
                storage_level = StorageLevel.MEMORY_ONLY

            # Cache the DataFrame
            cached_df = df.persist(storage_level)

            # Store cache information
            self.cached_datasets[dataset_name] = cached_df
            self.cache_stats[dataset_name] = {
                "row_count": df.count(),
                "size_mb": estimated_size_mb,
                "storage_level": self._get_storage_level_name(storage_level),
                "cached_at": datetime.now().isoformat(),
                "access_count": 0
            }

            # Initialize hit/miss counters
            self.cache_hits[dataset_name] = 0
            self.cache_misses[dataset_name] = 0

            logger.info(f"Cached dataset {dataset_name} with {storage_level} (estimated size: {estimated_size_mb:.2f}MB)")

            return cached_df

        except Exception as e:
            logger.warning(f"Failed to cache dataset {dataset_name}: {e}")
            return df

    def _estimate_size_mb(self, df: DataFrame) -> float:
        """Estimate DataFrame size in MB."""
        try:
            # Get row count and column count
            row_count = df.count()
            col_count = len(df.columns)

            # Rough estimation: 8 bytes per value + overhead
            estimated_bytes = row_count * col_count * 8 * 1.2  # 20% overhead
            return estimated_bytes / (1024 * 1024)
        except:
            return 100.0  # Default estimate

    def _select_optimal_storage_level(self, size_mb: float) -> StorageLevel:
        """Select optimal storage level based on dataset size."""
        if size_mb <= self.storage_thresholds["MEMORY_ONLY"]:
            return StorageLevel.MEMORY_ONLY
        elif size_mb <= self.storage_thresholds["MEMORY_AND_DISK"]:
            return StorageLevel.MEMORY_AND_DISK
        elif size_mb <= self.storage_thresholds["DISK_ONLY"]:
            return StorageLevel.DISK_ONLY
        else:
            return StorageLevel.OFF_HEAP

    def _get_storage_level_name(self, storage_level: StorageLevel) -> str:
        """Convert StorageLevel to simple name string."""
        if storage_level == StorageLevel.MEMORY_ONLY:
            return "MEMORY_ONLY"
        elif storage_level == StorageLevel.MEMORY_AND_DISK:
            return "MEMORY_AND_DISK"
        elif storage_level == StorageLevel.DISK_ONLY:
            return "DISK_ONLY"
        elif storage_level == StorageLevel.OFF_HEAP:
            return "OFF_HEAP"
        else:
            return str(storage_level)

    def get_cached_dataset(self, dataset_name: str) -> Optional[DataFrame]:
        """Get cached dataset and update access statistics."""
        if dataset_name in self.cached_datasets:
            self.cache_hits[dataset_name] += 1
            self.cache_stats[dataset_name]["access_count"] += 1
            return self.cached_datasets[dataset_name]
        else:
            self.cache_misses[dataset_name] += 1
            return None

    def get_cache_hit_ratio(self, dataset_name: str) -> float:
        """Calculate cache hit ratio for a dataset."""
        hits = self.cache_hits.get(dataset_name, 0)
        misses = self.cache_misses.get(dataset_name, 0)
        total = hits + misses

        if total == 0:
            return 0.0
        return hits / total

    def unpersist_dataset(self, dataset_name: str):
        """Unpersist a cached dataset."""
        if dataset_name in self.cached_datasets:
            try:
                self.cached_datasets[dataset_name].unpersist()
                del self.cached_datasets[dataset_name]
                del self.cache_stats[dataset_name]
                logger.info(f"Unpersisted dataset {dataset_name}")
            except Exception as e:
                logger.warning(f"Failed to unpersist dataset {dataset_name}: {e}")

    def clear_all_caches(self):
        """Clear all cached datasets."""
        for dataset_name in list(self.cached_datasets.keys()):
            self.unpersist_dataset(dataset_name)
        logger.info("Cleared all caches")

    def get_cache_summary(self) -> Dict[str, Any]:
        """Get summary of cache usage."""
        return {
            "cached_datasets": list(self.cached_datasets.keys()),
            "total_cached_size_mb": sum(stats["size_mb"] for stats in self.cache_stats.values()),
            "cache_stats": self.cache_stats,
            "hit_ratios": {name: self.get_cache_hit_ratio(name) for name in self.cache_stats.keys()}
        }


class PerformanceBenchmark:
    """Performance benchmarking for datasets."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.benchmark_results: List[PerformanceMetrics] = []

    def benchmark_dataset(self, df: DataFrame, dataset_name: str,
                         operations: List[str] = None) -> PerformanceMetrics:
        """Benchmark dataset performance with specified operations."""
        if operations is None:
            operations = ["count", "filter", "groupBy", "join", "orderBy"]

        start_time = time.time()

        try:
            # Basic dataset information
            row_count = df.count()
            partition_count = df.rdd.getNumPartitions()

            # Estimate size
            estimated_size_mb = self._estimate_size_mb(df)

            # Run benchmark operations
            operation_times = {}
            for operation in operations:
                op_start = time.time()
                self._run_benchmark_operation(df, operation)
                operation_times[operation] = time.time() - op_start

            # Calculate total processing time
            total_time = time.time() - start_time

            # Create metrics
            metrics = PerformanceMetrics(
                dataset_name=dataset_name,
                row_count=row_count,
                size_mb=estimated_size_mb,
                partition_count=partition_count,
                cache_hit_ratio=0.0,  # Will be updated by cache manager
                processing_time_seconds=total_time,
                memory_usage_mb=estimated_size_mb * 0.8,  # Rough estimate
                shuffle_read_mb=0.0,  # Would need Spark UI metrics
                shuffle_write_mb=0.0,  # Would need Spark UI metrics
                storage_level="NONE",
                optimization_applied=[]
            )

            # Store benchmark results
            self.benchmark_results.append(metrics)

            logger.info(f"Benchmark completed for {dataset_name}: {total_time:.2f}s")
            return metrics

        except Exception as e:
            logger.error(f"Benchmark failed for {dataset_name}: {e}")
            # Return basic metrics even if benchmark fails
            return PerformanceMetrics(
                dataset_name=dataset_name,
                row_count=0,
                size_mb=0.0,
                partition_count=0,
                cache_hit_ratio=0.0,
                processing_time_seconds=0.0,
                memory_usage_mb=0.0,
                shuffle_read_mb=0.0,
                shuffle_write_mb=0.0,
                storage_level="NONE",
                optimization_applied=["benchmark_failed"]
            )

    def _run_benchmark_operation(self, df: DataFrame, operation: str):
        """Run a specific benchmark operation."""
        try:
            if operation == "count":
                df.count()
            elif operation == "filter":
                df.filter(col(df.columns[0]).isNotNull()).count()
            elif operation == "groupBy":
                if len(df.columns) > 1:
                    df.groupBy(df.columns[0]).count()
            elif operation == "join":
                # Self-join for benchmarking
                df.alias("a").join(df.alias("b"), col("a." + df.columns[0]) == col("b." + df.columns[0])).count()
            elif operation == "orderBy":
                df.orderBy(df.columns[0]).limit(100).count()
        except Exception as e:
            logger.warning(f"Benchmark operation {operation} failed: {e}")

    def _estimate_size_mb(self, df: DataFrame) -> float:
        """Estimate DataFrame size in MB."""
        try:
            row_count = df.count()
            col_count = len(df.columns)
            estimated_bytes = row_count * col_count * 8 * 1.2
            return estimated_bytes / (1024 * 1024)
        except:
            return 100.0

    def export_benchmark_results(self, output_path: str):
        """Export benchmark results to JSON file."""
        try:
            results = [asdict(metrics) for metrics in self.benchmark_results]

            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)

            logger.info(f"Benchmark results exported to {output_path}")

        except Exception as e:
            logger.error(f"Failed to export benchmark results: {e}")


class PerformanceOptimizer:
    """Main performance optimization orchestrator."""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.cache_manager = CacheManager(spark)
        self.benchmark = PerformanceBenchmark(spark)

        # Performance configuration
        self.shuffle_partitions = config.get("spark", {}).get("shuffle_partitions", 200)
        self.broadcast_threshold = config.get("spark", {}).get("broadcast_threshold", 10485760)  # 10MB

        # Set Spark configurations
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark for optimal performance."""
        try:
            # Set shuffle partitions
            self.spark.conf.set("spark.sql.shuffle.partitions", self.shuffle_partitions)

            # Set broadcast threshold
            self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", self.broadcast_threshold)

            # Enable adaptive query execution
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

            # Enable dynamic partition pruning
            self.spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

            logger.info("Spark configured for optimal performance")

        except Exception as e:
            logger.warning(f"Failed to configure Spark: {e}")

    def optimize_returns_raw(self, df: DataFrame) -> DataFrame:
        """Optimize returns_raw dataset specifically."""
        logger.info("Optimizing returns_raw dataset...")

        optimized_df = df

        # Apply optimizations
        optimizations = []

        # 1. Optimize shuffle partitions
        optimized_df = self._optimize_shuffle_partitions(optimized_df)
        optimizations.append("shuffle_partition_optimization")

        # 2. Apply Z-ordering for common query patterns
        optimized_df = self._apply_z_ordering(optimized_df)
        optimizations.append("z_ordering")

        # 3. Optimize file compaction
        optimized_df = self._optimize_file_compaction(optimized_df)
        optimizations.append("file_compaction")

        # 4. Cache the dataset
        optimized_df = self.cache_manager.smart_cache(optimized_df, "returns_raw", "auto")
        optimizations.append("intelligent_caching")

        # 5. Run benchmark
        benchmark_metrics = self.benchmark.benchmark_dataset(optimized_df, "returns_raw")
        benchmark_metrics.optimization_applied = optimizations

        logger.info(f"Returns_raw optimization completed. Applied: {', '.join(optimizations)}")

        return optimized_df

    def _optimize_shuffle_partitions(self, df: DataFrame) -> DataFrame:
        """Optimize shuffle partitions based on data size."""
        try:
            current_partitions = df.rdd.getNumPartitions()
            row_count = df.count()

            # Calculate optimal partitions (rule of thumb: 1 partition per 100MB)
            estimated_size_mb = row_count * len(df.columns) * 8 / (1024 * 1024)
            optimal_partitions = max(1, min(int(estimated_size_mb / 100), self.shuffle_partitions))

            if current_partitions != optimal_partitions:
                if current_partitions > optimal_partitions:
                    df = df.coalesce(optimal_partitions)
                    logger.info(f"Reduced partitions from {current_partitions} to {optimal_partitions}")
                else:
                    df = df.repartition(optimal_partitions)
                    logger.info(f"Increased partitions from {current_partitions} to {optimal_partitions}")

            return df

        except Exception as e:
            logger.warning(f"Shuffle partition optimization failed: {e}")
            return df

    def _apply_z_ordering(self, df: DataFrame) -> DataFrame:
        """Apply Z-ordering for common query patterns."""
        try:
            # For returns_raw, Z-order by commonly queried columns
            z_order_columns = ["return_date", "return_reason", "return_status"]

            # Check if columns exist
            existing_columns = [col for col in z_order_columns if col in df.columns]

            if existing_columns:
                # In a real implementation, you would use Delta Lake's Z-ORDER BY
                # For now, we'll just repartition by these columns
                df = df.repartition(*existing_columns)
                logger.info(f"Applied Z-ordering on columns: {existing_columns}")

            return df

        except Exception as e:
            logger.warning(f"Z-ordering failed: {e}")
            return df

    def _optimize_file_compaction(self, df: DataFrame) -> DataFrame:
        """Optimize file compaction to avoid small files."""
        try:
            current_partitions = df.rdd.getNumPartitions()
            row_count = df.count()

            # Calculate optimal partition size (target: 128MB per partition)
            target_partition_size = 128 * 1024 * 1024  # 128MB in bytes
            estimated_row_size = len(df.columns) * 8  # 8 bytes per column
            optimal_partitions = max(1, int(row_count * estimated_row_size / target_partition_size))

            if current_partitions != optimal_partitions:
                df = df.repartition(optimal_partitions)
                logger.info(f"Optimized file compaction: {current_partitions} -> {optimal_partitions} partitions")

            return df

        except Exception as e:
            logger.warning(f"File compaction optimization failed: {e}")
            return df

    def optimize_broadcast_joins(self, df: DataFrame, join_df: DataFrame,
                                join_columns: List[str]) -> DataFrame:
        """Optimize joins using broadcast joins when appropriate."""
        try:
            # Estimate size of join DataFrame
            join_size_mb = self._estimate_size_mb(join_df)

            if join_size_mb <= self.broadcast_threshold / (1024 * 1024):  # Convert to MB
                # Use broadcast join for small DataFrames
                broadcast_df = broadcast(join_df)
                logger.info(f"Applied broadcast join for DataFrame of size {join_size_mb:.2f}MB")
                return df.join(broadcast_df, join_columns)
            else:
                # Use regular join for large DataFrames
                logger.info(f"Using regular join for DataFrame of size {join_size_mb:.2f}MB")
                return df.join(join_df, join_columns)

        except Exception as e:
            logger.warning(f"Broadcast join optimization failed: {e}")
            return df.join(join_df, join_columns)

    def run_full_optimization_pipeline(self, datasets: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Run full optimization pipeline on multiple datasets."""
        logger.info("Starting full optimization pipeline...")

        optimized_datasets = {}

        for dataset_name, df in datasets.items():
            try:
                if dataset_name == "returns_raw":
                    optimized_df = self.optimize_returns_raw(df)
                else:
                    # Apply general optimizations
                    optimized_df = self._apply_general_optimizations(df, dataset_name)

                optimized_datasets[dataset_name] = optimized_df
                logger.info(f"Optimized dataset: {dataset_name}")

            except Exception as e:
                logger.error(f"Failed to optimize dataset {dataset_name}: {e}")
                optimized_datasets[dataset_name] = df  # Return original if optimization fails

        # Run comprehensive benchmark
        self._run_comprehensive_benchmark(optimized_datasets)

        logger.info("Full optimization pipeline completed")
        return optimized_datasets

    def _apply_general_optimizations(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Apply general optimizations to any dataset."""
        optimized_df = df

        # Cache the dataset
        optimized_df = self.cache_manager.smart_cache(optimized_df, dataset_name, "auto")

        # Optimize partitions
        optimized_df = self._optimize_shuffle_partitions(optimized_df)

        # Optimize file compaction
        optimized_df = self._optimize_file_compaction(optimized_df)

        return optimized_df

    def _run_comprehensive_benchmark(self, datasets: Dict[str, DataFrame]):
        """Run comprehensive benchmark on all datasets."""
        logger.info("Running comprehensive benchmark...")

        for dataset_name, df in datasets.items():
            try:
                self.benchmark.benchmark_dataset(df, f"{dataset_name}_optimized")
            except Exception as e:
                logger.warning(f"Benchmark failed for {dataset_name}: {e}")

    def _estimate_size_mb(self, df: DataFrame) -> float:
        """Estimate DataFrame size in MB."""
        try:
            row_count = df.count()
            col_count = len(df.columns)
            estimated_bytes = row_count * col_count * 8 * 1.2
            return estimated_bytes / (1024 * 1024)
        except:
            return 100.0

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        cache_summary = self.cache_manager.get_cache_summary()

        return {
            "cached_datasets": cache_summary["cached_datasets"],
            "total_cached_size_mb": cache_summary["total_cached_size_mb"],
            "cache_hit_ratios": cache_summary["hit_ratios"],
            "benchmark_results": len(self.benchmark.benchmark_results),
            "optimization_applied": list(set([
                opt for metrics in self.benchmark.benchmark_results
                for opt in metrics.optimization_applied
            ])),
            "spark_config": {
                "shuffle_partitions": self.shuffle_partitions,
                "broadcast_threshold_mb": self.broadcast_threshold / (1024 * 1024)
            }
        }


def create_performance_optimizer(spark: SparkSession, config: Dict[str, Any]) -> PerformanceOptimizer:
    """Factory function to create PerformanceOptimizer."""
    return PerformanceOptimizer(spark, config)


if __name__ == "__main__":
    # Example usage
    from pyspark.sql import SparkSession

    # Create Spark session
    spark = SparkSession.builder \
        .appName("PerformanceOptimizer") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.sql.autoBroadcastJoinThreshold", 10485760) \
        .getOrCreate()

    # Configuration
    config = {
        "spark": {
            "shuffle_partitions": 200,
            "broadcast_threshold": 10485760
        }
    }

    # Create optimizer
    optimizer = create_performance_optimizer(spark, config)

    # Create sample data
    sample_data = [
        ("R001", "O001", "defective", 100.0, "2021-01-01"),
        ("R002", "O002", "wrong_size", 50.0, "2021-01-02"),
        ("R003", "O003", "damaged", 75.0, "2021-01-03")
    ]

    df = spark.createDataFrame(sample_data, ["return_id", "order_id", "return_reason", "amount", "return_date"])

    # Optimize dataset
    optimized_df = optimizer.optimize_returns_raw(df)

    # Get performance summary
    summary = optimizer.get_performance_summary()
    print("Performance Summary:", json.dumps(summary, indent=2, default=str))

    spark.stop()
