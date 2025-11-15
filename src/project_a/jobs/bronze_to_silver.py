"""
Bronze to Silver Transformation Job

Transform Bronze → Silver with all 5 sources (CRM, Redshift, Snowflake, FX, Kafka).
"""
import sys
import logging
import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.utils.logging import setup_json_logging, get_trace_id
from pyspark_interview_project.utils.run_audit import write_run_audit
from pyspark_interview_project.utils.cloudwatch_metrics import emit_job_success, emit_job_failure
from pyspark_interview_project.monitoring.lineage_emitter import LineageEmitter, load_lineage_config
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(args):
    """
    Transform Bronze → Silver with all sources.
    
    Args:
        args: argparse.Namespace with --env, --config, --run-date
    """
    # Setup structured logging
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"Job started (trace_id={trace_id}, env={args.env})")
    
    # Load config
    if args.config:
        config_path = args.config
        if config_path.startswith("s3://"):
            from pyspark.sql import SparkSession
            spark_temp = SparkSession.builder \
                .appName("config_loader") \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()
            try:
                config_lines = spark_temp.sparkContext.textFile(config_path).collect()
                config_content = "\n".join(config_lines)
                tmp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8")
                tmp_file.write(config_content)
                tmp_file.close()
                config_path = tmp_file.name
            finally:
                spark_temp.stop()
        else:
            config_path = str(Path(config_path))
    else:
        config_path = Path(f"config/{args.env}.yaml")
        if not config_path.exists():
            config_path = Path("config/prod.yaml")
        if not config_path.exists():
            config_path = Path("config/local.yaml")
        config_path = str(config_path)
    
    config = load_config_resolved(config_path)
    
    if not config.get("environment"):
        config["environment"] = "emr"
    
    run_id = str(uuid.uuid4())
    start_time = time.time()
    spark = build_spark(config)
    
    try:
        # Import the actual transformation logic
        # The transformation logic is in jobs/transform/bronze_to_silver.py
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
        from jobs.transform.bronze_to_silver import bronze_to_silver_complete
        
        run_date = args.run_date if hasattr(args, 'run_date') and args.run_date else datetime.utcnow().strftime("%Y-%m-%d")
        
        # Count input rows (approximate from bronze)
        bronze_root = config["paths"]["bronze_root"]
        sources = config["sources"]
        rows_in_approx = 0
        try:
            # Try to count from bronze sources
            for source_name in ["crm", "redshift", "snowflake", "fx", "kafka_sim"]:
                if source_name in sources:
                    source_path = f"{bronze_root}/{source_name}"
                    try:
                        df_temp = spark.read.format("delta").load(source_path)
                        rows_in_approx += df_temp.count()
                    except:
                        pass
        except:
            rows_in_approx = 0
        
        results = bronze_to_silver_complete(spark, config, run_date=run_date)
        
        # Count output rows
        rows_out = sum([
            results['customers'].count(),
            results['orders'].count(),
            results['products'].count(),
            results['behavior'].count(),
            results['fx_rates'].count(),
            results['order_events'].count()
        ])
        
        duration_seconds = time.time() - start_time
        duration_ms = duration_seconds * 1000
        
        logger.info("✅ Bronze to Silver transformation completed")
        logger.info(f"  - Customers: {results['customers'].count():,} rows")
        logger.info(f"  - Orders: {results['orders'].count():,} rows")
        logger.info(f"  - Products: {results['products'].count():,} rows")
        logger.info(f"  - Behavior: {results['behavior'].count():,} rows")
        logger.info(f"  - FX Rates: {results['fx_rates'].count():,} rows")
        logger.info(f"  - Order Events: {results['order_events'].count():,} rows")
        
        # Emit CloudWatch metrics
        try:
            emit_job_success(
                job_name="bronze_to_silver",
                duration_seconds=duration_seconds,
                env=config.get("environment", "dev"),
                rows_processed=rows_out
            )
        except Exception as e:
            logger.warning(f"⚠️  Failed to emit metrics: {e}")
        
        # Emit lineage event
        try:
            lineage_config_path = config.get("lineage", {}).get("config_path") or \
                                 f"s3://{config.get('buckets', {}).get('artifacts', '')}/config/lineage.yaml"
            lineage_config = load_lineage_config(lineage_config_path)
            emitter = LineageEmitter(lineage_config)
            
            # Define inputs and outputs
            inputs = [
                "bronze.crm.accounts",
                "bronze.crm.contacts",
                "bronze.snowflake.customers",
                "bronze.snowflake.orders",
                "bronze.redshift.behavior",
                "bronze.fx.rates"
            ]
            outputs = [
                "silver.customers",
                "silver.orders",
                "silver.products",
                "silver.behavior",
                "silver.fx_rates",
                "silver.order_events"
            ]
            
            emitter.emit_job(
                job_name="bronze_to_silver",
                run_id=run_id,
                inputs=inputs,
                outputs=outputs,
                status="SUCCESS",
                metadata={
                    "rows_in": rows_in_approx,
                    "rows_out": rows_out,
                    "duration_seconds": duration_seconds
                }
            )
        except Exception as e:
            logger.warning(f"⚠️  Failed to emit lineage: {e}")
        
        # Write run audit
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="bronze_to_silver",
                    env=config.get("environment", "dev"),
                    source=bronze_root,
                    target=config["paths"]["silver_root"],
                    rows_in=rows_in_approx,
                    rows_out=rows_out,
                    status="SUCCESS",
                    run_id=run_id,
                    duration_ms=duration_ms,
                    config=config
                )
            except Exception as e:
                logger.warning(f"⚠️  Failed to write run audit: {e}")
        
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True, extra={"trace_id": trace_id})
        
        # Emit failure metrics
        try:
            emit_job_failure(
                job_name="bronze_to_silver",
                env=config.get("environment", "dev"),
                error_type=type(e).__name__
            )
        except:
            pass
        
        # Emit failure lineage event
        try:
            lineage_config_path = config.get("lineage", {}).get("config_path") or \
                                 f"s3://{config.get('buckets', {}).get('artifacts', '')}/config/lineage.yaml"
            lineage_config = load_lineage_config(lineage_config_path)
            emitter = LineageEmitter(lineage_config)
            
            inputs = [
                "bronze.crm.accounts",
                "bronze.snowflake.orders",
                "bronze.redshift.behavior"
            ]
            outputs = ["silver.customers", "silver.orders"]
            
            emitter.emit_job(
                job_name="bronze_to_silver",
                run_id=run_id,
                inputs=inputs,
                outputs=outputs,
                status="FAILED",
                error_message=str(e)
            )
        except:
            pass
        
        # Write failure audit
        lake_bucket = config.get("buckets", {}).get("lake", "")
        if lake_bucket:
            try:
                write_run_audit(
                    bucket=lake_bucket,
                    job_name="bronze_to_silver",
                    env=config.get("environment", "dev"),
                    source=config.get("paths", {}).get("bronze_root", ""),
                    target=config.get("paths", {}).get("silver_root", ""),
                    rows_in=0,
                    rows_out=0,
                    status="FAILED",
                    run_id=run_id,
                    error_message=str(e),
                    config=config
                )
            except:
                pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # Keep this for local debugging
    import argparse
    
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation job")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    parser.add_argument("--run-date", help="Processing date (YYYY-MM-DD)")
    main(parser.parse_args())

