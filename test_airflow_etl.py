#!/usr/bin/env python3
"""
Simple test script to run ETL pipeline and show Airflow integration
"""

import os
import sys
import yaml
from datetime import datetime

def run_etl_with_airflow_style():
    """Run ETL pipeline in Airflow style"""
    print("🚀 AIRFLOW-STYLE ETL EXECUTION")
    print("==============================")
    print()
    
    # Change to project directory
    project_dir = "/Users/kunal/IdeaProjects/pyspark_data_engineer_project"
    os.chdir(project_dir)
    sys.path.insert(0, os.path.join(project_dir, 'src'))
    
    print(f"📁 Working directory: {os.getcwd()}")
    print(f"📅 Execution time: {datetime.now()}")
    print()
    
    try:
        # Load configuration
        with open('config/local.yaml', 'r') as f:
            config_data = yaml.safe_load(f)
        
        print("✅ Configuration loaded successfully")
        
        # Import and run production pipeline
        from pyspark_interview_project.production_pipeline import ProductionETLPipeline
        
        print("🔄 Starting Production ETL Pipeline...")
        pipeline = ProductionETLPipeline(config_data)
        success = pipeline.run_pipeline()
        
        if success:
            print("✅ ETL Pipeline completed successfully!")
            
            # Check files created
            delta_dir = "data/lakehouse_delta"
            if os.path.exists(delta_dir):
                total_files = 0
                recent_files = []
                
                for root, dirs, files in os.walk(delta_dir):
                    for file in files:
                        if file.endswith('.parquet'):
                            total_files += 1
                            file_path = os.path.join(root, file)
                            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                            recent_files.append((file_path, file_time))
                
                print(f"📊 Total Delta Lake files created: {total_files}")
                
                # Show most recent files
                recent_files.sort(key=lambda x: x[1], reverse=True)
                print("📄 Most recent files:")
                for file_path, file_time in recent_files[:5]:
                    relative_path = os.path.relpath(file_path, project_dir)
                    print(f"   📄 {relative_path} (modified: {file_time})")
                
                return {
                    "status": "success",
                    "total_files": total_files,
                    "execution_time": datetime.now().isoformat(),
                    "files_created": len(recent_files)
                }
            else:
                print("❌ Delta Lake directory not found!")
                return {"status": "error", "message": "Delta Lake directory not found"}
        else:
            print("❌ ETL Pipeline failed!")
            return {"status": "error", "message": "ETL Pipeline failed"}
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    result = run_etl_with_airflow_style()
    print()
    print("📊 EXECUTION RESULT:")
    print("===================")
    for key, value in result.items():
        print(f"   {key}: {value}")
