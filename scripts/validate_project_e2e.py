#!/usr/bin/env python3
"""
End-to-End Project Validation

Validates the entire project structure, code, configuration, and readiness.
"""
import sys
import json
from pathlib import Path
from typing import Dict, List, Tuple

class ProjectValidator:
    def __init__(self, project_root: str = "."):
        self.root = Path(project_root)
        self.errors = []
        self.warnings = []
        self.passed = []
    
    def check(self, name: str, condition: bool, error_msg: str = None, warning: bool = False):
        """Record check result."""
        if condition:
            self.passed.append(name)
            print(f"  ✅ {name}")
        else:
            if warning:
                self.warnings.append(f"{name}: {error_msg or 'Check failed'}")
                print(f"  ⚠️  {name}: {error_msg or 'Check failed'}")
            else:
                self.errors.append(f"{name}: {error_msg or 'Check failed'}")
                print(f"  ❌ {name}: {error_msg or 'Check failed'}")
    
    def validate_structure(self):
        """Validate project structure."""
        print("\n📁 Validating Project Structure...")
        
        required_dirs = [
            "src/project_a",
            "src/project_a/jobs",
            "src/project_a/pipeline",
            "src/pyspark_interview_project",
            "config",
            "config/schema_definitions/bronze",
            "jobs/transform",
            "jobs/gold",
            "jobs/publish",
            "aws/dags",
            "docs",
            "scripts",
        ]
        
        for dir_path in required_dirs:
            self.check(
                f"Directory: {dir_path}",
                (self.root / dir_path).exists(),
                f"Missing directory: {dir_path}"
            )
    
    def validate_unified_entrypoint(self):
        """Validate unified entrypoint."""
        print("\n🚀 Validating Unified Entrypoint...")
        
        entrypoint = self.root / "src/project_a/pipeline/run_pipeline.py"
        self.check(
            "Entrypoint exists",
            entrypoint.exists(),
            "run_pipeline.py not found"
        )
        
        if entrypoint.exists():
            content = entrypoint.read_text()
            self.check(
                "Entrypoint has JOB_MAP",
                "JOB_MAP" in content,
                "JOB_MAP not found"
            )
            self.check(
                "Entrypoint has argparse",
                "argparse" in content,
                "argparse not found"
            )
            self.check(
                "Entrypoint has main()",
                "def main()" in content,
                "main() function not found"
            )
    
    def validate_jobs(self):
        """Validate all job modules."""
        print("\n💼 Validating Job Modules...")
        
        required_jobs = [
            "src/project_a/jobs/fx_json_to_bronze.py",
            "src/project_a/jobs/bronze_to_silver.py",
            "src/project_a/jobs/silver_to_gold.py",
            "src/project_a/jobs/publish_gold_to_snowflake.py",
        ]
        
        for job_path in required_jobs:
            job_file = self.root / job_path
            self.check(
                f"Job: {Path(job_path).name}",
                job_file.exists(),
                f"Job file not found: {job_path}"
            )
            
            if job_file.exists():
                content = job_file.read_text()
                self.check(
                    f"{Path(job_path).name} has main(args)",
                    "def main(args)" in content,
                    "main(args) function not found"
                )
        
        # Check jobs/__init__.py
        init_file = self.root / "src/project_a/jobs/__init__.py"
        if init_file.exists():
            content = init_file.read_text()
            self.check(
                "jobs/__init__.py exports modules",
                "from . import" in content or "import" in content,
                "jobs/__init__.py doesn't export modules"
            )
    
    def validate_schemas(self):
        """Validate schema definitions."""
        print("\n📋 Validating Schema Definitions...")
        
        required_schemas = [
            "config/schema_definitions/bronze/fx_rates.json",
            "config/schema_definitions/bronze/kafka_events.json",
            "config/schema_definitions/bronze/crm_accounts.json",
            "config/schema_definitions/bronze/snowflake_customers.json",
            "config/schema_definitions/bronze/snowflake_orders.json",
            "config/schema_definitions/bronze/redshift_behavior.json",
        ]
        
        for schema_path in required_schemas:
            schema_file = self.root / schema_path
            self.check(
                f"Schema: {Path(schema_path).name}",
                schema_file.exists(),
                f"Schema not found: {schema_path}"
            )
            
            if schema_file.exists():
                try:
                    schema = json.loads(schema_file.read_text())
                    self.check(
                        f"{Path(schema_path).name} is valid JSON",
                        isinstance(schema, dict),
                        "Invalid JSON structure"
                    )
                    self.check(
                        f"{Path(schema_path).name} has columns",
                        "columns" in schema or "name" in schema,
                        "Missing columns/name in schema"
                    )
                except json.JSONDecodeError as e:
                    self.check(
                        f"{Path(schema_path).name} is valid JSON",
                        False,
                        f"JSON decode error: {e}"
                    )
    
    def validate_config(self):
        """Validate configuration files."""
        print("\n⚙️  Validating Configuration...")
        
        config_file = self.root / "config/dev.yaml"
        self.check(
            "dev.yaml exists",
            config_file.exists(),
            "config/dev.yaml not found"
        )
        
        pyproject = self.root / "pyproject.toml"
        self.check(
            "pyproject.toml exists",
            pyproject.exists(),
            "pyproject.toml not found"
        )
        
        if pyproject.exists():
            content = pyproject.read_text()
            self.check(
                "pyproject.toml has console script",
                "run-pipeline" in content or "project.scripts" in content,
                "Console script not configured"
            )
    
    def validate_data_sources(self):
        """Validate data sources."""
        print("\n📊 Validating Data Sources...")
        
        required_files = {
            "CRM": [
                "aws/data/samples/crm/accounts.csv",
                "aws/data/samples/crm/contacts.csv",
            ],
            "Snowflake": [
                "aws/data/samples/snowflake/snowflake_customers_50000.csv",
                "aws/data/samples/snowflake/snowflake_orders_100000.csv",
            ],
            "FX": [
                "aws/data/samples/fx/fx_rates_historical.json",
            ],
            "Kafka": [
                "aws/data/samples/kafka/stream_kafka_events_100000.csv",
            ],
        }
        
        for source, files in required_files.items():
            for file_path in files:
                file = self.root / file_path
                self.check(
                    f"{source}: {Path(file_path).name}",
                    file.exists(),
                    f"File not found: {file_path}",
                    warning=True  # May not be uploaded to S3 yet
                )
    
    def validate_transformations(self):
        """Validate transformation jobs."""
        print("\n🔄 Validating Transformations...")
        
        bronze_to_silver = self.root / "jobs/transform/bronze_to_silver.py"
        self.check(
            "bronze_to_silver.py exists",
            bronze_to_silver.exists(),
            "bronze_to_silver.py not found"
        )
        
        silver_to_gold = self.root / "jobs/gold/silver_to_gold.py"
        self.check(
            "silver_to_gold.py exists",
            silver_to_gold.exists(),
            "silver_to_gold.py not found"
        )
    
    def validate_utilities(self):
        """Validate utility modules."""
        print("\n🛠️  Validating Utilities...")
        
        utilities = [
            "src/pyspark_interview_project/utils/run_audit.py",
            "src/pyspark_interview_project/utils/checkpoint.py",
            "src/pyspark_interview_project/config_loader.py",
            "src/pyspark_interview_project/utils/spark_session.py",
        ]
        
        for util_path in utilities:
            util_file = self.root / util_path
            self.check(
                f"Utility: {Path(util_path).name}",
                util_file.exists(),
                f"Utility not found: {util_path}",
                warning=True  # Some may be optional
            )
    
    def validate_documentation(self):
        """Validate documentation."""
        print("\n📚 Validating Documentation...")
        
        docs = [
            "docs/DATA_CONTRACTS.md",
            "docs/BRONZE_DIRECTORY_STRUCTURE.md",
            "README.md",
        ]
        
        for doc_path in docs:
            doc_file = self.root / doc_path
            self.check(
                f"Doc: {Path(doc_path).name}",
                doc_file.exists(),
                f"Documentation not found: {doc_path}",
                warning=True
            )
    
    def validate_airflow(self):
        """Validate Airflow DAG."""
        print("\n☁️  Validating Airflow DAG...")
        
        dag_file = self.root / "aws/dags/daily_pipeline_dag_complete.py"
        self.check(
            "Airflow DAG exists",
            dag_file.exists(),
            "daily_pipeline_dag_complete.py not found"
        )
        
        if dag_file.exists():
            content = dag_file.read_text()
            self.check(
                "DAG uses unified entrypoint",
                "run-pipeline" in content or "project_a-0.1.0" in content,
                "DAG doesn't use unified entrypoint"
            )
            self.check(
                "DAG has retries",
                "retries" in content or "retry" in content,
                "DAG missing retry configuration"
            )
    
    def validate_imports(self):
        """Validate Python imports."""
        print("\n🐍 Validating Python Imports...")
        
        try:
            sys.path.insert(0, str(self.root / "src"))
            
            # Test entrypoint import
            try:
                from project_a.pipeline.run_pipeline import main, JOB_MAP
                self.check(
                    "Entrypoint imports successfully",
                    True,
                    None
                )
                self.check(
                    "JOB_MAP has 4 jobs",
                    len(JOB_MAP) >= 4,
                    f"JOB_MAP has {len(JOB_MAP)} jobs, expected 4+"
                )
            except ImportError as e:
                self.check(
                    "Entrypoint imports successfully",
                    False,
                    f"Import error: {e}"
                )
            
            # Test job imports
            try:
                from project_a.jobs import fx_json_to_bronze, bronze_to_silver, silver_to_gold
                self.check(
                    "Job modules import successfully",
                    True,
                    None
                )
            except ImportError as e:
                self.check(
                    "Job modules import successfully",
                    False,
                    f"Import error: {e}"
                )
                
        except Exception as e:
            self.check(
                "Python imports",
                False,
                f"Import validation error: {e}"
            )
    
    def validate_wheel(self):
        """Validate wheel build."""
        print("\n📦 Validating Wheel Build...")
        
        dist_dir = self.root / "dist"
        if dist_dir.exists():
            wheels = list(dist_dir.glob("project_a-*.whl"))
            self.check(
                "Wheel file exists",
                len(wheels) > 0,
                "No wheel file found in dist/"
            )
        else:
            self.check(
                "Wheel file exists",
                False,
                "dist/ directory not found (run: python -m build)",
                warning=True
            )
    
    def run_all(self):
        """Run all validations."""
        print("=" * 60)
        print("END-TO-END PROJECT VALIDATION")
        print("=" * 60)
        
        self.validate_structure()
        self.validate_unified_entrypoint()
        self.validate_jobs()
        self.validate_schemas()
        self.validate_config()
        self.validate_data_sources()
        self.validate_transformations()
        self.validate_utilities()
        self.validate_documentation()
        self.validate_airflow()
        self.validate_imports()
        self.validate_wheel()
        
        # Summary
        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)
        print(f"✅ Passed: {len(self.passed)}")
        print(f"⚠️  Warnings: {len(self.warnings)}")
        print(f"❌ Errors: {len(self.errors)}")
        print()
        
        if self.errors:
            print("ERRORS:")
            for error in self.errors:
                print(f"  ❌ {error}")
            print()
        
        if self.warnings:
            print("WARNINGS:")
            for warning in self.warnings:
                print(f"  ⚠️  {warning}")
            print()
        
        if not self.errors:
            print("✅ PROJECT IS READY FOR DEPLOYMENT!")
            return 0
        else:
            print("❌ PROJECT HAS ERRORS - PLEASE FIX BEFORE DEPLOYMENT")
            return 1


def main():
    validator = ProjectValidator()
    return validator.run_all()


if __name__ == "__main__":
    sys.exit(main())

