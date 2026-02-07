#!/usr/bin/env python3
"""
Main entry point for PySpark Data Engineering Project
"""

import os
import sys
import yaml
import click
import time
from pathlib import Path

from .logging_config import configure_logging, new_run_id, log_pipeline_event, log_metric
from .metrics import create_metrics
from .utils.spark_session import build_spark
from .pipeline import run_pipeline
from .lineage import emit


def load_config(env: str = "local") -> dict:
    """Load configuration based on environment"""
    config_dir = Path("config")
    
    # Load default config
    with open(config_dir / "default.yaml") as f:
        config = yaml.safe_load(f)
    
    # Overlay environment-specific config
    env_config_path = config_dir / f"{env}.yaml"
    if env_config_path.exists():
        with open(env_config_path) as f:
            env_config = yaml.safe_load(f)
            config.update(env_config)
    
    # Override with environment variables
    for key, value in os.environ.items():
        if key.startswith("PDI_"):
            config_key = key[4:].lower()
            config[config_key] = value
    
    return config


@click.command()
@click.option("--proc-date", default=None, help="Processing date (YYYY-MM-DD)")
@click.option("--env", default="local", help="Environment (local/aws)")
@click.option("--config-file", default=None, help="Custom config file path")
def main(proc_date: str, env: str, config_file: str):
    """PySpark Data Engineering Pipeline"""
    
    # Generate run ID and configure logging
    run_id = new_run_id()
    logger, _ = configure_logging(run_id)
    
    # Load configuration
    if config_file:
        with open(config_file) as f:
            config = yaml.safe_load(f)
    else:
        config = load_config(env)
    
    # Log pipeline start with enhanced context
    log_pipeline_event(logger, "pipeline_started", "main", run_id, 
                      env=env, proc_date=proc_date)
    
    # Create metrics sink with run_id
    metrics = create_metrics(config, run_id)
    
    # Track pipeline start time
    start_time = time.time()
    
    # Emit lineage start event
    emit({
        "eventType": "START",
        "run": {"id": run_id},
        "job": {"name": "pyspark-etl-pipeline"},
        "inputs": [],
        "outputs": []
    }, config)
    
    try:
        # Build Spark session
        log_pipeline_event(logger, "spark_session_initializing", "setup", run_id)
        spark = build_spark(config)
        log_pipeline_event(logger, "spark_session_ready", "setup", run_id)
        
        # Run pipeline
        log_pipeline_event(logger, "pipeline_execution_started", "execution", run_id)
        run_pipeline(spark, config, run_id)
        
        # Calculate and log duration
        duration = time.time() - start_time
        log_metric(logger, "pipeline.duration.seconds", duration, run_id)
        metrics.timing("pipeline.duration", duration * 1000, {"stage": "total"})
        
        # Emit lineage complete event
        emit({
            "eventType": "COMPLETE",
            "run": {"id": run_id},
            "job": {"name": "pyspark-etl-pipeline"},
            "inputs": [],
            "outputs": []
        }, config)
        
        log_pipeline_event(logger, "pipeline_completed_successfully", "completion", run_id,
                          duration_seconds=duration, status="success")
        
    except Exception as e:
        import traceback
        duration = time.time() - start_time
        
        # Log error with enhanced context
        log_pipeline_event(logger, "pipeline_failed", "error", run_id,
                          error=str(e), duration_seconds=duration, status="failed")
        
        # Log error metrics
        log_metric(logger, "pipeline.errors.count", 1, run_id)
        metrics.incr("pipeline.errors", 1, {"error_type": type(e).__name__})
        
        # Emit lineage fail event
        emit({
            "eventType": "FAIL",
            "run": {"id": run_id},
            "job": {"name": "pyspark-etl-pipeline"},
            "inputs": [],
            "outputs": [],
            "errorMessage": str(e)
        }, config)
        
        sys.exit(1)
    
    finally:
        if 'spark' in locals():
            log_pipeline_event(logger, "spark_session_stopping", "cleanup", run_id)
            spark.stop()
            log_pipeline_event(logger, "spark_session_stopped", "cleanup", run_id)


if __name__ == "__main__":
    main()
