"""
OpenLineage decorator for automatic lineage tracking.

Provides @lineage_job decorator to wrap job functions with lineage emission.
"""

import os
import functools
import logging
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime

from .lineage_emitter import emit_start, emit_complete, emit_fail

logger = logging.getLogger(__name__)


def lineage_job(
    name: str,
    inputs: List[str],
    outputs: List[str],
    namespace: Optional[str] = None
):
    """
    Decorator to automatically emit lineage events for a job.
    
    Args:
        name: Job name (e.g., 'extract_snowflake_orders')
        inputs: List of input dataset names (e.g., ['snowflake://ORDERS'])
        outputs: List of output dataset names (e.g., ['s3://bucket/bronze/orders'])
        namespace: OpenLineage namespace (defaults to env var or 'default')
    
    Example:
        @lineage_job(
            name="extract_snowflake_orders",
            inputs=["snowflake://ORDERS"],
            outputs=["s3://bucket/bronze/snowflake_orders"]
        )
        def extract_orders(spark, config):
            # ... extraction logic ...
            return df
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract config and spark from args/kwargs
            config = None
            spark = None
            
            # Try to find config in kwargs first
            if 'config' in kwargs:
                config = kwargs['config']
            elif len(args) > 1 and isinstance(args[1], dict):
                config = args[1]
            
            # Try to find spark in kwargs or args
            if 'spark' in kwargs:
                spark = kwargs['spark']
            elif len(args) > 0:
                # Check if first arg is SparkSession
                first_arg = args[0]
                if hasattr(first_arg, 'read') and hasattr(first_arg, 'createDataFrame'):
                    spark = first_arg
            
            # Get namespace
            lineage_namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "default")
            
            # Generate run ID
            run_id = f"{name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            # Emit start event
            try:
                emit_start(
                    job_name=name,
                    inputs=[{"name": inp, "namespace": lineage_namespace} for inp in inputs],
                    outputs=[{"name": out, "namespace": lineage_namespace} for out in outputs],
                    config=config,
                    run_id=run_id
                )
                
                # Execute function
                result = func(*args, **kwargs)
                
                # Emit complete event
                emit_complete(
                    job_name=name,
                    inputs=[{"name": inp, "namespace": lineage_namespace} for inp in inputs],
                    outputs=[{"name": out, "namespace": lineage_namespace} for out in outputs],
                    config=config,
                    run_id=run_id,
                    metadata={"status": "success"}
                )
                
                return result
                
            except Exception as e:
                # Emit failure event
                logger.error(f"Job {name} failed: {e}")
                emit_fail(
                    job_name=name,
                    inputs=[{"name": inp, "namespace": lineage_namespace} for inp in inputs],
                    outputs=[{"name": out, "namespace": lineage_namespace} for out in outputs],
                    config=config,
                    run_id=run_id,
                    error=str(e)
                )
                raise
        
        return wrapper
    return decorator

