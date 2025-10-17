"""Production ETL Pipeline Runner.
Runs the production ETL pipeline with Delta Lake support.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

from pyspark_interview_project.production_pipeline import ProductionETLPipeline


def run(env: str | None = None) -> None:
    """Run the production ETL pipeline."""
    print("🚀 Starting Production ETL Pipeline...")
    
    # Configuration
    config = {
        "base_path": "data/lakehouse",
        "delta_path": "data/lakehouse_delta"
    }
    
    try:
        # Initialize and run pipeline
        pipeline = ProductionETLPipeline(config)
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
