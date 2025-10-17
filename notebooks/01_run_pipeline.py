"""Production ETL Pipeline Runner.
Runs the production ETL pipeline with Delta Lake support.
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

from pyspark_interview_project.production_pipeline import ProductionETLPipeline
from pyspark_interview_project.config_model import AppConfig


def run(env: str | None = None) -> None:
    """Run the production ETL pipeline."""
    print("🚀 Starting Production ETL Pipeline...")
    
    try:
        # Load and validate configuration
        config_path = os.path.join(ROOT, "config", "local.yaml")
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            config = AppConfig(**config_data)
            config.validate_paths()
            print("✅ Configuration validated successfully")
        else:
            # Fallback configuration
            config = {
                "delta_path": "data/lakehouse_delta"
            }
            print("⚠️ Using fallback configuration")
        
        # Initialize and run pipeline
        # Convert Pydantic model to dict for compatibility
        config_dict = config.model_dump() if hasattr(config, 'model_dump') else config
        pipeline = ProductionETLPipeline(config_dict)
        success = pipeline.run_pipeline()
        
        if success:
            print("✅ Production ETL Pipeline completed successfully!")
            print("🏆 Delta Lake tables created with time travel capabilities!")
        else:
            print("❌ Production ETL Pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
