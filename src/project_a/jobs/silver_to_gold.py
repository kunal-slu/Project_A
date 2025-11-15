"""
Silver to Gold Transformation Job

Transform Silver → Gold with star schema (fact_orders, dim_customer, dim_product, dim_date).
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
    Transform Silver → Gold with star schema.
    
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
        # The transformation logic is in jobs/gold/silver_to_gold.py
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
        from jobs.gold.silver_to_gold import silver_to_gold_complete
        
        run_date = args.run_date if hasattr(args, 'run_date') and args.run_date else datetime.utcnow().strftime("%Y-%m-%d")
        
        # Count input rows from silver
        silver_root = config["paths"]["silver_root"]
        rows_in_approx = 0
        try:
            tables = config["tables"]["silver"]
            for table_name in ["customers", "orders", "products", "behavior"]:
                if table_name in tables:
                    try:
                        df_temp = spark.read.format("delta").load(f"{silver_root}/{tables[table_name]}")
                        rows_in_approx += df_temp.count()
                    except:
                        pass
        except:
            rows_in_approx = 0
        
        results = silver_to_gold_complete(spark, config, run_date=run_date)
        
        # Count output rows
        rows_out = sum([
            results['fact_orders'].count(),
            results['dim_customer'].count(),
            results['dim_product'].count(),
            results['dim_date'].count(),
            results['customer_360'].count(),
            results['product_performance'].count()
        ])
        
        duration_seconds = time.time() - start_time
        duration_ms = duration_seconds * 1000
        
        logger.info("✅ Silver to Gold transformation completed")
        logger.info(f"  - fact_orders: {results['fact_orders'].count():,} rows")
        logger.info(f"  - dim_customer: {results['dim_customer'].count():,} rows")
        logger.info(f"  - dim_product: {results['dim_product'].count():,} rows")
        logger.info(f"  - dim_date: {results['dim_date'].count():,} rows")
        logger.info(f"  - customer_360: {results['customer_360'].count():,} rows")
        logger.info(f"  - product_performance: {results['product_performance'].count():,} rows")
        
        # Emit CloudWatch metrics
        try:
            emit_job_success(
                job_name="silver_to_gold",
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
            
            inputs = [
                "silver.customers",
                "silver.orders",
                "silver.products",
                "silver.behavior",
                "silver.fx_rates"
            ]
            outputs = [
                "gold.fact_orders",
                "gold.dim_customer",
                "gold.dim_product",
                "gold.dim_date",
                "gold.customer_360",
                "gold.product_performance"
            ]
            
            emitter.emit_job(
                job_name="silver_to_gold",
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
                    job_name="silver_to_gold",
                    env=config.get("environment", "dev"),
                    source=silver_root,
                    target=config["paths"]["gold_root"],
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
                job_name="silver_to_gold",
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
            
            inputs = ["silver.customers", "silver.orders"]
            outputs = ["gold.fact_orders", "gold.dim_customer"]
            
            emitter.emit_job(
                job_name="silver_to_gold",
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
                    job_name="silver_to_gold",
                    env=config.get("environment", "dev"),
                    source=config.get("paths", {}).get("silver_root", ""),
                    target=config.get("paths", {}).get("gold_root", ""),
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
    
    parser = argparse.ArgumentParser(description="Silver to Gold transformation job")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    parser.add_argument("--run-date", help="Processing date (YYYY-MM-DD)")
    main(parser.parse_args())

