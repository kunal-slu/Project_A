"""
Tests for Performance Optimization Module.

This module tests:
- Caching strategies and management
- Performance benchmarking
- Scale optimization for returns_raw dataset
- Shuffle partition optimization
- Broadcast join optimization
- File compaction and I/O tuning
"""

import json
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from project_a.performance_optimizer import (
    CacheManager,
    PerformanceBenchmark,
    PerformanceMetrics,
    PerformanceOptimizer,
    create_performance_optimizer,
)


@pytest.fixture
def mock_spark_session():
    """Create a mock SparkSession for testing."""
    spark = Mock(spec=SparkSession)

    # Mock Spark configuration
    spark.conf = Mock()
    spark.conf.get = Mock(return_value="200")

    # Mock Spark context
    spark.sparkContext = Mock()
    spark.sparkContext.getConf = Mock(return_value=Mock())

    return spark


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    # Create a mock DataFrame with realistic structure
    df = Mock(spec=DataFrame)

    # Mock DataFrame methods
    df.count = Mock(return_value=10000)
    df.columns = ["order_id", "customer_id", "product_id", "quantity", "amount"]
    df.rdd = Mock()
    df.rdd.getNumPartitions = Mock(return_value=4)

    # Mock DataFrame operations
    df.filter = Mock(return_value=df)
    df.join = Mock(return_value=df)
    df.groupBy = Mock(return_value=df)
    df.orderBy = Mock(return_value=df)
    df.limit = Mock(return_value=df)
    df.select = Mock(return_value=df)
    df.withColumn = Mock(return_value=df)
    df.persist = Mock(return_value=df)
    df.unpersist = Mock(return_value=df)
    df.coalesce = Mock(return_value=df)
    df.repartition = Mock(return_value=df)

    return df


@pytest.fixture
def test_config():
    """Create test configuration."""
    return {
        "spark": {
            "shuffle_partitions": 200,
            "broadcast_threshold": 10485760,  # 10MB
        },
        "output": {
            "bronze_path": "data/lakehouse/bronze",
            "silver_path": "data/lakehouse/silver",
            "gold_path": "data/lakehouse/gold",
        },
    }


class TestCacheManager:
    """Test CacheManager functionality."""

    def test_smart_cache_auto_strategy(self, mock_spark_session, sample_dataframe):
        """Test automatic caching strategy selection."""
        cache_manager = CacheManager(mock_spark_session)

        # Test with small dataset
        cached_df = cache_manager.smart_cache(sample_dataframe, "test_dataset", "auto")

        assert cached_df == sample_dataframe
        assert "test_dataset" in cache_manager.cached_datasets
        assert "test_dataset" in cache_manager.cache_stats

        # Verify cache statistics
        stats = cache_manager.cache_stats["test_dataset"]
        assert stats["row_count"] == 10000
        assert stats["storage_level"] == "MEMORY_ONLY"  # Small dataset

    def test_smart_cache_memory_strategy(self, mock_spark_session, sample_dataframe):
        """Test memory-only caching strategy."""
        cache_manager = CacheManager(mock_spark_session)

        cached_df = cache_manager.smart_cache(sample_dataframe, "test_dataset", "memory")

        assert cached_df == sample_dataframe
        assert cache_manager.cache_stats["test_dataset"]["storage_level"] == "MEMORY_ONLY"

    def test_cache_hit_ratio(self, mock_spark_session, sample_dataframe):
        """Test cache hit ratio calculation."""
        cache_manager = CacheManager(mock_spark_session)

        # Cache a dataset
        cache_manager.smart_cache(sample_dataframe, "test_dataset")

        # Get hit ratio
        ratio = cache_manager.get_cache_hit_ratio("test_dataset")
        assert ratio >= 0.0

    def test_unpersist_dataset(self, mock_spark_session, sample_dataframe):
        """Test dataset unpersisting."""
        cache_manager = CacheManager(mock_spark_session)

        # Cache a dataset
        cache_manager.smart_cache(sample_dataframe, "test_dataset")
        assert "test_dataset" in cache_manager.cached_datasets

        # Unpersist it
        cache_manager.unpersist_dataset("test_dataset")
        assert "test_dataset" not in cache_manager.cached_datasets

    def test_clear_all_caches(self, mock_spark_session, sample_dataframe):
        """Test clearing all caches."""
        cache_manager = CacheManager(mock_spark_session)

        # Cache multiple datasets
        cache_manager.smart_cache(sample_dataframe, "dataset1")
        cache_manager.smart_cache(sample_dataframe, "dataset2")

        assert len(cache_manager.cached_datasets) == 2

        # Clear all
        cache_manager.clear_all_caches()
        assert len(cache_manager.cached_datasets) == 0


class TestPerformanceBenchmark:
    """Test PerformanceBenchmark functionality."""

    def test_benchmark_dataset_basic_operations(self, mock_spark_session, sample_dataframe):
        """Test basic benchmark operations."""
        benchmark = PerformanceBenchmark(mock_spark_session)

        # Run benchmark with default operations
        metrics = benchmark.benchmark_dataset(sample_dataframe, "test_dataset")

        assert isinstance(metrics, PerformanceMetrics)
        assert metrics.dataset_name == "test_dataset"
        assert metrics.row_count == 10000
        assert metrics.partition_count == 4
        assert metrics.processing_time_seconds >= 0.0

    def test_benchmark_dataset_custom_operations(self, mock_spark_session, sample_dataframe):
        """Test benchmark with custom operations."""
        benchmark = PerformanceBenchmark(mock_spark_session)

        custom_ops = ["count", "filter"]
        metrics = benchmark.benchmark_dataset(sample_dataframe, "test_dataset", custom_ops)

        assert metrics.dataset_name == "test_dataset"
        assert len(benchmark.benchmark_results) == 1

    def test_benchmark_dataset_error_handling(self, mock_spark_session):
        """Test benchmark error handling."""
        benchmark = PerformanceBenchmark(mock_spark_session)

        # Create a problematic DataFrame
        problem_df = Mock(spec=DataFrame)
        problem_df.count = Mock(side_effect=Exception("Test error"))
        problem_df.columns = []

        # Should handle errors gracefully
        metrics = benchmark.benchmark_dataset(problem_df, "problem_dataset")
        assert metrics.dataset_name == "problem_dataset"

    def test_export_benchmark_results(self, mock_spark_session, sample_dataframe, tmp_path):
        """Test benchmark results export."""
        benchmark = PerformanceBenchmark(mock_spark_session)

        # Run a benchmark
        benchmark.benchmark_dataset(sample_dataframe, "test_dataset")

        # Export results
        output_path = tmp_path / "benchmarks.json"
        benchmark.export_benchmark_results(str(output_path))

        # Verify file was created
        assert output_path.exists()

        # Verify JSON content
        with open(output_path) as f:
            results = json.load(f)

        assert len(results) == 1
        assert results[0]["dataset_name"] == "test_dataset"


class TestPerformanceOptimizer:
    """Test PerformanceOptimizer functionality."""

    def test_optimize_returns_raw(self, mock_spark_session, sample_dataframe, test_config):
        """Test returns_raw dataset optimization."""
        optimizer = PerformanceOptimizer(mock_spark_session, test_config)

        # Mock the benchmark to avoid actual operations
        with patch.object(optimizer.benchmark, "benchmark_dataset") as mock_benchmark:
            mock_benchmark.return_value = PerformanceMetrics(
                dataset_name="test",
                row_count=10000,
                size_mb=100.0,
                partition_count=4,
                cache_hit_ratio=0.0,
                processing_time_seconds=1.0,
                memory_usage_mb=50.0,
                shuffle_read_mb=0.0,
                shuffle_write_mb=0.0,
                storage_level="NONE",
                optimization_applied=[],
            )

            # Test optimization
            optimized_df = optimizer.optimize_returns_raw(sample_dataframe)

            assert optimized_df == sample_dataframe
            assert "returns_raw" in optimizer.cache_manager.cached_datasets

    def test_optimize_shuffle_partitions(self, mock_spark_session, test_config):
        """Test shuffle partition optimization."""
        optimizer = PerformanceOptimizer(mock_spark_session, test_config)

        # Create DataFrame with many partitions
        df = Mock(spec=DataFrame)
        df.count = Mock(return_value=1000000)  # 1M rows
        df.columns = ["col1", "col2", "col3"]  # Mock columns
        df.rdd = Mock()
        df.rdd.getNumPartitions = Mock(return_value=100)
        df.coalesce = Mock(return_value=df)

        optimizer._optimize_shuffle_partitions(df)

        # Should call coalesce to reduce partitions
        df.coalesce.assert_called_once()

    def test_optimize_file_compaction(self, mock_spark_session, test_config):
        """Test file compaction optimization."""
        optimizer = PerformanceOptimizer(mock_spark_session, test_config)

        df = Mock(spec=DataFrame)
        df.count = Mock(return_value=50000)  # Mock row count
        df.columns = ["col1", "col2", "col3"]  # Mock columns
        df.rdd = Mock()
        df.rdd.getNumPartitions = Mock(return_value=10)
        df.repartition = Mock(return_value=df)

        optimizer._optimize_file_compaction(df)

        # Should call repartition for compaction
        # With 50k rows and 3 columns, optimal partitions = 1 (very small dataset)
        df.repartition.assert_called_once_with(1)

    def test_run_full_optimization_pipeline(
        self, mock_spark_session, sample_dataframe, test_config
    ):
        """Test full optimization pipeline."""
        optimizer = PerformanceOptimizer(mock_spark_session, test_config)

        datasets = {
            "returns_raw": sample_dataframe,
            "customers": sample_dataframe,
            "orders": sample_dataframe,
        }

        # Mock benchmark to avoid actual operations
        with patch.object(optimizer.benchmark, "benchmark_dataset"):
            with patch.object(optimizer.benchmark, "export_benchmark_results"):
                optimized = optimizer.run_full_optimization_pipeline(datasets)

                assert len(optimized) == 3
                assert "returns_raw" in optimized
                assert "customers" in optimized
                assert "orders" in optimized

    def test_get_performance_summary(self, mock_spark_session, test_config):
        """Test performance summary generation."""
        optimizer = PerformanceOptimizer(mock_spark_session, test_config)

        # Add some cached datasets
        optimizer.cache_manager.cached_datasets["test1"] = Mock()
        optimizer.cache_manager.cached_datasets["test2"] = Mock()

        # Add some benchmark results
        optimizer.benchmark.benchmark_results = [
            PerformanceMetrics(
                dataset_name="test",
                row_count=1000,
                size_mb=10.0,
                partition_count=2,
                cache_hit_ratio=0.0,
                processing_time_seconds=0.5,
                memory_usage_mb=25.0,
                shuffle_read_mb=0.0,
                shuffle_write_mb=0.0,
                storage_level="MEMORY_ONLY",
                optimization_applied=["caching", "partitioning"],
            )
        ]

        summary = optimizer.get_performance_summary()

        assert "test1" in summary["cached_datasets"]
        assert "test2" in summary["cached_datasets"]
        assert summary["benchmark_results"] == 1
        assert "caching" in summary["optimization_applied"]
        assert "partitioning" in summary["optimization_applied"]


class TestPerformanceOptimizerIntegration:
    """Integration tests for performance optimization."""

    def test_create_performance_optimizer(self, mock_spark_session, test_config):
        """Test factory function."""
        optimizer = create_performance_optimizer(mock_spark_session, test_config)

        assert isinstance(optimizer, PerformanceOptimizer)
        assert optimizer.shuffle_partitions == 200
        assert optimizer.broadcast_threshold == 10485760

    def test_returns_raw_scale_benchmark(self, mock_spark_session, test_config):
        """Test scale benchmarking specifically for returns_raw."""
        optimizer = PerformanceOptimizer(mock_spark_session, test_config)

        # Create a larger dataset to simulate scale
        large_df = Mock(spec=DataFrame)
        large_df.count = Mock(return_value=1000000)  # 1M rows
        large_df.columns = ["return_id", "order_id", "reason", "amount", "return_date"]
        large_df.rdd = Mock()
        large_df.rdd.getNumPartitions = Mock(return_value=50)

        # Mock all DataFrame operations
        for method in [
            "filter",
            "join",
            "groupBy",
            "orderBy",
            "limit",
            "persist",
            "coalesce",
            "repartition",
        ]:
            setattr(large_df, method, Mock(return_value=large_df))

        # Mock benchmark
        with patch.object(optimizer.benchmark, "benchmark_dataset") as mock_benchmark:
            mock_benchmark.return_value = PerformanceMetrics(
                dataset_name="returns_raw_large",
                row_count=1000000,
                size_mb=1000.0,
                partition_count=50,
                cache_hit_ratio=0.0,
                processing_time_seconds=5.0,
                memory_usage_mb=500.0,
                shuffle_read_mb=100.0,
                shuffle_write_mb=100.0,
                storage_level="DISK_ONLY",
                optimization_applied=["caching", "partitioning", "compaction"],
            )

            # Test optimization
            optimized_df = optimizer.optimize_returns_raw(large_df)

            # Verify large dataset handling
            assert optimized_df == large_df
            assert "returns_raw" in optimizer.cache_manager.cached_datasets

    def test_caching_strategy_selection(self, mock_spark_session, test_config):
        """Test intelligent caching strategy selection."""
        optimizer = PerformanceOptimizer(mock_spark_session, test_config)

        # Test different dataset sizes with realistic calculations
        # 3 columns * 8 bytes * row_count / (1024*1024) = size in MB
        test_cases = [
            (1000, "MEMORY_ONLY"),  # ~0.023 MB (< 100MB)
            (5000000, "MEMORY_AND_DISK"),  # ~114 MB (100-500MB)
            (20000000, "DISK_ONLY"),  # ~458 MB (500MB-2GB)
            (100000000, "OFF_HEAP"),  # ~2.3 GB (> 2GB)
        ]

        for row_count, expected_strategy in test_cases:
            df = Mock(spec=DataFrame)
            df.count = Mock(return_value=row_count)
            df.columns = ["col1", "col2", "col3"]
            df.persist = Mock(return_value=df)

            optimizer.cache_manager.smart_cache(df, f"dataset_{row_count}", "auto")

            # Verify storage level selection
            stats = optimizer.cache_manager.cache_stats[f"dataset_{row_count}"]
            assert stats["storage_level"] == expected_strategy


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
