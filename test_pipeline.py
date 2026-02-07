#!/usr/bin/env python3
"""
Quick test of the ETL pipeline
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

print("="*60)
print("🚀 PROJECT_A ETL PIPELINE TEST")
print("="*60)
print()

# Step 1: Load configuration
print("Step 1: Loading configuration...")
try:
    from project_a.core.config import ProjectConfig
    config = ProjectConfig('config/dev.yaml', env='dev')
    print(f"✅ Configuration loaded successfully")
    print(f"   Environment: {config.environment}")
    print(f"   Bronze root: {config.get('paths', {}).get('bronze_root', 'data/bronze')}")
    print()
except Exception as e:
    print(f"❌ Failed to load configuration: {e}")
    sys.exit(1)

# Step 2: Run Snowflake → Bronze
print("="*60)
print("📥 INGESTION: Snowflake → Bronze")
print("="*60)
try:
    from jobs.ingest.snowflake_to_bronze import SnowflakeToBronzeJob
    
    job = SnowflakeToBronzeJob(config)
    print("Job created successfully")
    
    result = job.execute()
    print(f"✅ Snowflake ingestion completed")
    print(f"   Records processed: {result.get('records_processed', {})}")
    print(f"   Output path: {result.get('output_path', 'N/A')}")
    print()
except Exception as e:
    print(f"❌ Snowflake ingestion failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 3: Run CRM → Bronze
print("="*60)
print("📥 INGESTION: CRM → Bronze")
print("="*60)
try:
    from jobs.ingest.crm_to_bronze import CrmToBronzeJob
    
    job = CrmToBronzeJob(config)
    result = job.execute()
    print(f"✅ CRM ingestion completed")
    print(f"   Records processed: {result.get('records_processed', {})}")
    print()
except Exception as e:
    print(f"❌ CRM ingestion failed: {e}")
    import traceback
    traceback.print_exc()

# Step 4: Run Redshift → Bronze
print("="*60)
print("📥 INGESTION: Redshift → Bronze")
print("="*60)
try:
    from jobs.ingest.redshift_to_bronze import RedshiftToBronzeJob
    
    job = RedshiftToBronzeJob(config)
    result = job.execute()
    print(f"✅ Redshift ingestion completed")
    print(f"   Records processed: {result.get('records_processed', {})}")
    print()
except Exception as e:
    print(f"❌ Redshift ingestion failed: {e}")
    import traceback
    traceback.print_exc()

# Step 5: Run FX → Bronze
print("="*60)
print("📥 INGESTION: FX → Bronze")
print("="*60)
try:
    from jobs.ingest.fx_to_bronze import FxToBronzeJob
    
    job = FxToBronzeJob(config)
    result = job.execute()
    print(f"✅ FX ingestion completed")
    print(f"   Records processed: {result.get('records_processed', {})}")
    print()
except Exception as e:
    print(f"❌ FX ingestion failed: {e}")
    import traceback
    traceback.print_exc()

# Step 6: Run Kafka → Bronze
print("="*60)
print("📥 INGESTION: Kafka Events → Bronze")
print("="*60)
try:
    from jobs.ingest.kafka_events_to_bronze import KafkaEventsToBronzeJob
    
    job = KafkaEventsToBronzeJob(config)
    result = job.execute()
    print(f"✅ Kafka ingestion completed")
    print(f"   Records processed: {result.get('records_processed', {})}")
    print()
except Exception as e:
    print(f"❌ Kafka ingestion failed: {e}")
    import traceback
    traceback.print_exc()

# Step 7: Run Bronze → Silver
print("="*60)
print("🔄 TRANSFORMATION: Bronze → Silver")
print("="*60)
try:
    from jobs.transform.bronze_to_silver import BronzeToSilverJob
    
    job = BronzeToSilverJob(config)
    result = job.execute()
    print(f"✅ Bronze to Silver transformation completed")
    print(f"   Layers processed: {result.get('layers_processed', [])}")
    print(f"   Output path: {result.get('output_path', 'N/A')}")
    print()
except Exception as e:
    print(f"❌ Bronze to Silver transformation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 8: Run Silver → Gold
print("="*60)
print("🔄 TRANSFORMATION: Silver → Gold")
print("="*60)
try:
    from jobs.transform.silver_to_gold import SilverToGoldJob
    
    job = SilverToGoldJob(config)
    result = job.execute()
    print(f"✅ Silver to Gold transformation completed")
    print(f"   Tables built: {result.get('tables_built', [])}")
    print(f"   Output path: {result.get('output_path', 'N/A')}")
    print()
except Exception as e:
    print(f"❌ Silver to Gold transformation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Final summary
print("="*60)
print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
print("="*60)
print()
print("📊 Data Summary:")

import os
def count_dirs(path):
    try:
        return len([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])
    except:
        return 0

bronze_tables = count_dirs("data/bronze")
silver_tables = count_dirs("data/silver")
gold_tables = count_dirs("data/gold")

print(f"   Bronze tables: {bronze_tables}")
print(f"   Silver tables: {silver_tables}")
print(f"   Gold tables: {gold_tables}")
print()
print("🔍 Verify data:")
print("   ls -lh data/bronze/")
print("   ls -lh data/silver/")
print("   ls -lh data/gold/")
print()
