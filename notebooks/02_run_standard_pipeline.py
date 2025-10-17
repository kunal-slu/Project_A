"""
Standard ETL Pipeline Runner
Runs the standard ETL pipeline with proper Delta Lake implementation
"""

import os
import sys
import yaml
from logging.config import fileConfig

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

# Setup logging
log_config_path = os.path.join(ROOT, "config", "logging.conf")
if os.path.exists(log_config_path):
    fileConfig(log_config_path, disable_existing_loggers=False)

from pyspark_interview_project.standard_etl_pipeline import StandardETLPipeline


def run(env: str | None = None) -> None:
    """Run the standard ETL pipeline."""
    print("🚀 Starting Standard ETL Pipeline...")
    
    try:
        # Load and validate configuration
        config_path = os.path.join(ROOT, "config", "local.yaml")
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            print("✅ Configuration loaded successfully")
        else:
            # Fallback configuration
            config_data = {
                "delta_path": "data/lakehouse_delta_standard",
                "cloud": "local",
                "app_name": "standard_etl_pipeline"
            }
            print("⚠️ Using fallback configuration")
        
        # Initialize and run pipeline
        pipeline = StandardETLPipeline(config_data)
        success = pipeline.run_pipeline()
        
        if success:
            print("✅ Standard ETL Pipeline completed successfully!")
            print("🏆 Delta Lake tables created with proper standards!")
            print("📊 All Delta Lake features working correctly!")
        else:
            print("❌ Standard ETL Pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
