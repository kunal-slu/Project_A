"""
Tests for collect_run_summary job.
"""

import pytest

from jobs.collect_run_summary import collect_run_summary


@pytest.fixture
def mock_config():
    """Mock configuration dictionary."""
    return {
        "data_lake": {
            "bronze_path": "data/lakehouse_delta/bronze",
            "silver_path": "data/lakehouse_delta/silver",
            "gold_path": "data/lakehouse_delta/gold",
            "metrics_path": "data/metrics",
        }
    }


def test_collect_run_summary_success(spark, mock_config, tmp_path):
    """Test successful run summary collection."""
    # Create empty Delta tables for testing
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    gold_path = tmp_path / "gold"

    for path in [bronze_path, silver_path, gold_path]:
        path.mkdir(parents=True)

    # Create empty DataFrames and save
    empty_df = spark.createDataFrame([], "id STRING")
    empty_df.write.format("delta").mode("overwrite").save(
        str(bronze_path / "redshift" / "behavior")
    )
    empty_df.write.format("delta").mode("overwrite").save(str(silver_path / "behavior"))
    empty_df.write.format("delta").mode("overwrite").save(str(gold_path / "customer_360"))

    mock_config["data_lake"]["bronze_path"] = str(bronze_path)
    mock_config["data_lake"]["silver_path"] = str(silver_path)
    mock_config["data_lake"]["gold_path"] = str(gold_path)

    # Execute
    summary = collect_run_summary(spark, mock_config, "test_run_123", "2025-01-01")

    # Assertions
    assert summary is not None
    assert summary["run_id"] == "test_run_123"
    assert summary["execution_date"] == "2025-01-01"
    assert "stages" in summary
    assert "metrics" in summary


def test_collect_run_summary_handles_errors(spark, mock_config, tmp_path):
    """Test run summary handles missing data gracefully."""
    mock_config["data_lake"]["bronze_path"] = str(tmp_path / "nonexistent" / "bronze")
    mock_config["data_lake"]["silver_path"] = str(tmp_path / "nonexistent" / "silver")
    mock_config["data_lake"]["gold_path"] = str(tmp_path / "nonexistent" / "gold")

    # Should handle missing data
    summary = collect_run_summary(spark, mock_config, "test_run_456", "2025-01-02")

    # Should still return summary with errors
    assert summary is not None
    assert len(summary.get("errors", [])) > 0 or summary["status"] != "success"
