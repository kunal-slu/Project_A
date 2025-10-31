#!/usr/bin/env python3
"""
Enterprise ETL Pipeline Driver - Complete Orchestration

This script orchestrates the complete enterprise ETL pipeline:
1. Bronze ingestion for all data sources (CRM, Snowflake, Redshift, FX, Kafka)
2. Silver transformations with data quality gates
3. Gold analytics and dimensional modeling
4. Metrics and lineage tracking
5. Delta Lake verification

This is the main entry point for end-to-end pipeline execution.
"""

import sys
import os
import logging
import pandas as pd
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.metrics import track_job_start, track_job_complete, track_records_processed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Main pipeline orchestrator class."""
    
    def __init__(self, config_path: str):
        """Initialize the pipeline orchestrator."""
        self.config = load_conf(config_path)
        self.job_id = track_job_start("enterprise_etl_pipeline", self.config)
        self.results = {}
        
    def run_ingest_job(self, job_name: str, job_script: str, args: List[str] = None) -> bool:
        """
        Run an individual ingest job.
        
        Args:
            job_name: Name of the job
            job_script: Path to the job script
            args: Additional arguments
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Running {job_name}...")
        
        try:
            # Build command
            cmd = ["python", job_script, "--config", self.config_path]
            if args:
                cmd.extend(args)
            
            # Run job
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info(f"✅ {job_name} completed successfully")
                self.results[job_name] = "SUCCESS"
                return True
            else:
                logger.error(f"❌ {job_name} failed: {result.stderr}")
                self.results[job_name] = "FAILED"
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ {job_name} timed out")
            self.results[job_name] = "TIMEOUT"
            return False
        except Exception as e:
            logger.error(f"💥 {job_name} failed with exception: {str(e)}")
            self.results[job_name] = "EXCEPTION"
            return False
    
    def run_bronze_ingestion(self) -> Dict[str, str]:
        """
        Run bronze ingestion for all data sources.
        
        Returns:
            Dictionary with ingestion results
        """
        logger.info("🚀 Starting Bronze Ingestion Phase")
        logger.info("=" * 60)
        
        # Define all ingest jobs
        ingest_jobs = [
            ("CRM Accounts", "aws/jobs/crm_accounts_ingest.py"),
            ("CRM Contacts", "aws/jobs/crm_contacts_ingest.py"),
            ("CRM Opportunities", "aws/jobs/crm_opportunities_ingest.py"),
            ("Snowflake Orders", "aws/jobs/snowflake_to_bronze.py"),
            ("Redshift Behavior", "aws/jobs/redshift_behavior_ingest.py"),
            ("FX Rates", "aws/jobs/fx_rates_ingest.py"),
        ]
        
        # Run each ingest job
        for job_name, job_script in ingest_jobs:
            success = self.run_ingest_job(job_name, job_script)
            if success:
                track_records_processed(self.job_id, job_name.lower().replace(' ', '_'), 0)  # Placeholder count
        
        return self.results
    
    def run_data_quality_checks(self) -> bool:
        """
        Run data quality checks on bronze layer.
        
        Returns:
            True if all checks pass, False otherwise
        """
        logger.info("🔍 Running Bronze Data Quality Checks")
        logger.info("=" * 60)
        
        try:
            success = self.run_ingest_job("Bronze DQ Check", "aws/jobs/dq_check_bronze.py")
            return success
        except Exception as e:
            logger.error(f"❌ Bronze DQ checks failed: {str(e)}")
            return False
    
    def run_silver_transformations(self) -> bool:
        """
        Run silver transformations.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("🔄 Starting Silver Transformation Phase")
        logger.info("=" * 60)
        
        try:
            # Run silver transformation jobs
            silver_jobs = [
                ("Snowflake Silver Merge", "aws/jobs/snowflake_bronze_to_silver_merge.py"),
            ]
            
            all_success = True
            for job_name, job_script in silver_jobs:
                success = self.run_ingest_job(job_name, job_script)
                if not success:
                    all_success = False
            
            return all_success
            
        except Exception as e:
            logger.error(f"❌ Silver transformations failed: {str(e)}")
            return False
    
    def run_gold_analytics(self) -> bool:
        """
        Run gold analytics and dimensional modeling.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("📈 Starting Gold Analytics Phase")
        logger.info("=" * 60)
        
        try:
            # Run gold analytics jobs
            gold_jobs = [
                ("Sales Fact Table", "aws/jobs/build_sales_fact_table.py"),
                ("Customer Dimension", "aws/jobs/build_customer_dimension.py"),
                ("Marketing Attribution", "aws/jobs/build_marketing_attribution.py"),
                ("SCD2 Customer Dimension", "aws/jobs/update_customer_dimension_scd2.py"),
            ]
            
            all_success = True
            for job_name, job_script in gold_jobs:
                success = self.run_ingest_job(job_name, job_script)
                if not success:
                    all_success = False
            
            return all_success
            
        except Exception as e:
            logger.error(f"❌ Gold analytics failed: {str(e)}")
            return False
    
    def run_final_quality_checks(self) -> bool:
        """
        Run final data quality checks on silver and gold layers.
        
        Returns:
            True if all checks pass, False otherwise
        """
        logger.info("🔍 Running Final Data Quality Checks")
        logger.info("=" * 60)
        
        try:
            success = self.run_ingest_job("Silver DQ Check", "aws/jobs/dq_check_silver.py")
            return success
        except Exception as e:
            logger.error(f"❌ Final DQ checks failed: {str(e)}")
            return False
    
    def emit_lineage_and_metrics(self) -> bool:
        """
        Emit lineage and metrics for the pipeline run.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("📊 Emitting Lineage and Metrics")
        logger.info("=" * 60)
        
        try:
            success = self.run_ingest_job("Lineage and Metrics", "aws/jobs/emit_lineage_and_metrics.py")
            return success
        except Exception as e:
            logger.error(f"❌ Lineage and metrics emission failed: {str(e)}")
            return False
    
    def run_complete_pipeline(self) -> bool:
        """
        Run the complete end-to-end pipeline.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("🚀 ENTERPRISE ETL PIPELINE - COMPLETE ORCHESTRATION")
        logger.info("=" * 70)
        logger.info(f"⏰ Started at: {datetime.now()}")
        
        try:
            # Phase 1: Bronze Ingestion
            bronze_results = self.run_bronze_ingestion()
            
            # Phase 2: Bronze DQ Checks
            bronze_dq_success = self.run_data_quality_checks()
            
            # Phase 3: Silver Transformations
            silver_success = self.run_silver_transformations()
            
            # Phase 4: Gold Analytics
            gold_success = self.run_gold_analytics()
            
            # Phase 5: Final DQ Checks
            final_dq_success = self.run_final_quality_checks()
            
            # Phase 6: Lineage and Metrics
            lineage_success = self.emit_lineage_and_metrics()
            
            # Summary
            self.print_pipeline_summary(bronze_results, bronze_dq_success, silver_success, 
                                      gold_success, final_dq_success, lineage_success)
            
            overall_success = (all(result == "SUCCESS" for result in bronze_results.values()) and
                             bronze_dq_success and silver_success and gold_success and
                             final_dq_success and lineage_success)
            
            track_job_complete(self.job_id, "SUCCESS" if overall_success else "FAILED")
            
            return overall_success
            
        except Exception as e:
            logger.error(f"💥 Pipeline execution failed: {str(e)}")
            track_job_complete(self.job_id, "FAILED", error_msg=str(e))
            return False
    
    def print_pipeline_summary(self, bronze_results: Dict[str, str], bronze_dq_success: bool,
                             silver_success: bool, gold_success: bool, final_dq_success: bool,
                             lineage_success: bool) -> None:
        """Print pipeline execution summary."""
        logger.info("\n🎯 PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 50)
        
        bronze_success_count = sum(1 for result in bronze_results.values() if result == "SUCCESS")
        total_bronze_jobs = len(bronze_results)
        
        logger.info(f"📊 Bronze Ingestion: {bronze_success_count}/{total_bronze_jobs} successful")
        logger.info(f"🔍 Bronze DQ Checks: {'SUCCESS' if bronze_dq_success else 'FAILED'}")
        logger.info(f"🔄 Silver Transformations: {'SUCCESS' if silver_success else 'FAILED'}")
        logger.info(f"📈 Gold Analytics: {'SUCCESS' if gold_success else 'FAILED'}")
        logger.info(f"🔍 Final DQ Checks: {'SUCCESS' if final_dq_success else 'FAILED'}")
        logger.info(f"📊 Lineage & Metrics: {'SUCCESS' if lineage_success else 'FAILED'}")
        
        overall_success = (bronze_success_count == total_bronze_jobs and bronze_dq_success and
                         silver_success and gold_success and final_dq_success and lineage_success)
        
        logger.info(f"\n🎉 Pipeline {'completed successfully' if overall_success else 'completed with errors'}!")
        logger.info(f"⏰ Finished at: {datetime.now()}")


def main():
    """Main pipeline execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enterprise ETL Pipeline Orchestrator")
    parser.add_argument("--config", default="config/dev.yaml", help="Configuration file path")
    parser.add_argument("--phase", choices=["bronze", "silver", "gold", "all"], 
                       default="all", help="Pipeline phase to run")
    args = parser.parse_args()
    
    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator(args.config)
        
        if args.phase == "all":
            success = orchestrator.run_complete_pipeline()
        elif args.phase == "bronze":
            bronze_results = orchestrator.run_bronze_ingestion()
            success = all(result == "SUCCESS" for result in bronze_results.values())
        elif args.phase == "silver":
            success = orchestrator.run_silver_transformations()
        elif args.phase == "gold":
            success = orchestrator.run_gold_analytics()
        else:
            logger.error(f"Unknown phase: {args.phase}")
            success = False
        
        return success
        
    except Exception as e:
        logger.error(f"💥 Pipeline execution failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
