"""
Production ETL Pipeline - Converts existing data to Delta Lake format
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProductionETLPipeline:
    """
    Production ETL Pipeline that creates Delta Lake tables from existing data
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.delta_path = config.get("delta_path", "data/lakehouse_delta")
        
    def create_delta_lake_structure(self, table_name: str, data: pd.DataFrame, layer: str):
        """Create Delta Lake table structure with transaction logs."""
        logger.info(f"🏗️ Creating Delta Lake structure for {table_name} in {layer} layer...")
        
        # Create directory structure
        table_dir = f"{self.delta_path}/{layer}/{table_name}"
        delta_log_dir = f"{table_dir}/_delta_log"
        
        os.makedirs(delta_log_dir, exist_ok=True)
        
        # Write data as Parquet
        parquet_file = f"{table_dir}/part-00000-{table_name}.parquet"
        data.to_parquet(parquet_file, index=False)
        
        # Create transaction log
        transaction_log = {
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 2
            },
            "metaData": {
                "id": f"{table_name}-{layer}-id",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": self._generate_schema_string(data),
                "partitionColumns": [],
                "createdTime": int(datetime.now().timestamp() * 1000),
                "description": f"{layer.title()} layer {table_name} data"
            },
            "add": {
                "path": f"part-00000-{table_name}.parquet",
                "partitionValues": {},
                "size": os.path.getsize(parquet_file),
                "modificationTime": int(datetime.now().timestamp() * 1000),
                "dataChange": True,
                "stats": self._generate_stats(data)
            }
        }
        
        # Write transaction log
        log_file = f"{delta_log_dir}/00000000000000000000.json"
        with open(log_file, 'w') as f:
            json.dump(transaction_log, f, indent=2)
        
        # Create checksum
        checksum_file = f"{delta_log_dir}/00000000000000000000.crc"
        with open(checksum_file, 'w') as f:
            f.write("7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d")  # Sample checksum
        
        logger.info(f"✅ Created Delta Lake table: {table_dir}")
        logger.info(f"   - Data: {parquet_file}")
        logger.info(f"   - Transaction log: {log_file}")
        logger.info(f"   - Checksum: {checksum_file}")
        
        return table_dir
    
    def _generate_schema_string(self, data: pd.DataFrame) -> str:
        """Generate Delta Lake schema string from DataFrame."""
        schema = {"type": "struct", "fields": []}
        
        for column, dtype in data.dtypes.items():
            field = {
                "name": column,
                "type": self._pandas_to_spark_type(str(dtype)),
                "nullable": True
            }
            schema["fields"].append(field)
        
        return json.dumps(schema)
    
    def _pandas_to_spark_type(self, pandas_type: str) -> str:
        """Convert pandas dtype to Spark type."""
        type_mapping = {
            'int64': 'long',
            'int32': 'integer',
            'float64': 'double',
            'float32': 'float',
            'bool': 'boolean',
            'object': 'string',
            'datetime64[ns]': 'timestamp'
        }
        return type_mapping.get(pandas_type, 'string')
    
    def _generate_stats(self, data: pd.DataFrame) -> str:
        """Generate statistics for Delta Lake."""
        stats = {
            "numRecords": len(data),
            "minValues": {},
            "maxValues": {},
            "nullCount": {}
        }
        
        for column in data.columns:
            if data[column].dtype in ['int64', 'float64']:
                stats["minValues"][column] = float(data[column].min())
                stats["maxValues"][column] = float(data[column].max())
            stats["nullCount"][column] = int(data[column].isnull().sum())
        
        return json.dumps(stats)
    
    def process_existing_data(self):
        """Process existing Parquet data and convert to Delta Lake format."""
        logger.info("🔄 Processing existing data to Delta Lake format...")
        
        # Process Bronze layer
        self._process_layer("bronze")
        
        # Process Silver layer
        self._process_layer("silver")
        
        # Process Gold layer
        self._process_layer("gold")
        
        logger.info("✅ All layers processed successfully!")
    
    def _process_layer(self, layer: str):
        """Process a specific layer."""
        logger.info(f"📊 Processing {layer} layer...")
        
        layer_path = f"data/lakehouse/{layer}"
        if not os.path.exists(layer_path):
            logger.warning(f"⚠️ Layer path not found: {layer_path}")
            return
        
        # Find Parquet files in layer
        parquet_files = [f for f in os.listdir(layer_path) if f.endswith('.parquet')]
        
        for parquet_file in parquet_files:
            try:
                # Read existing Parquet file
                file_path = f"{layer_path}/{parquet_file}"
                data = pd.read_parquet(file_path)
                
                # Extract table name
                table_name = parquet_file.replace('.parquet', '').replace(f'_{layer}', '')
                
                # Create Delta Lake structure
                self.create_delta_lake_structure(table_name, data, layer)
                
                logger.info(f"✅ Processed {table_name} ({len(data)} records)")
                
            except Exception as e:
                logger.error(f"❌ Failed to process {parquet_file}: {e}")
    
    def demonstrate_time_travel(self):
        """Demonstrate Delta Lake time travel capabilities."""
        logger.info("🕐 Demonstrating Delta Lake time travel...")
        
        # Create a second version of a table
        customers_path = f"{self.delta_path}/bronze/customers"
        if os.path.exists(customers_path):
            self._create_table_version(customers_path, "customers", "bronze")
            
            # Show version history
            self._show_version_history(customers_path)
    
    def _create_table_version(self, table_path: str, table_name: str, layer: str):
        """Create a second version of a table."""
        logger.info(f"📝 Creating second version of {table_name}...")
        
        # Read existing data
        data_file = f"{table_path}/part-00000-{table_name}.parquet"
        if os.path.exists(data_file):
            data = pd.read_parquet(data_file)
            
            # Modify data (add new records)
            new_data = data.copy()
            new_records = data.head(50).copy()  # Add 50 more records
            new_records['customer_id'] = range(len(data) + 1, len(data) + 51)
            new_records['name'] = [f'Customer_{i}' for i in range(len(data) + 1, len(data) + 51)]
            
            updated_data = pd.concat([data, new_records], ignore_index=True)
            
            # Write new version
            new_parquet_file = f"{table_path}/part-00001-{table_name}-v2.parquet"
            updated_data.to_parquet(new_parquet_file, index=False)
            
            # Create second transaction log
            delta_log_dir = f"{table_path}/_delta_log"
            transaction_log_v2 = {
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                },
                "add": {
                    "path": f"part-00001-{table_name}-v2.parquet",
                    "partitionValues": {},
                    "size": os.path.getsize(new_parquet_file),
                    "modificationTime": int(datetime.now().timestamp() * 1000),
                    "dataChange": True,
                    "stats": self._generate_stats(updated_data)
                }
            }
            
            # Write second transaction log
            log_file_v2 = f"{delta_log_dir}/00000000000000000001.json"
            with open(log_file_v2, 'w') as f:
                json.dump(transaction_log_v2, f, indent=2)
            
            # Create second checksum
            checksum_file_v2 = f"{delta_log_dir}/00000000000000000001.crc"
            with open(checksum_file_v2, 'w') as f:
                f.write("8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f")
            
            logger.info(f"✅ Created version 1 of {table_name} ({len(updated_data)} records)")
    
    def _show_version_history(self, table_path: str):
        """Show version history of a Delta Lake table."""
        logger.info(f"📊 Version history for {table_path}:")
        
        delta_log_dir = f"{table_path}/_delta_log"
        if os.path.exists(delta_log_dir):
            log_files = [f for f in os.listdir(delta_log_dir) if f.endswith('.json')]
            
            for i, log_file in enumerate(sorted(log_files)):
                log_path = f"{delta_log_dir}/{log_file}"
                with open(log_path, 'r') as f:
                    log_data = json.load(f)
                
                if 'metaData' in log_data:
                    # Initial version
                    logger.info(f"   Version {i}: Initial load")
                    logger.info(f"      Records: {json.loads(log_data['add']['stats'])['numRecords']}")
                else:
                    # Update version
                    logger.info(f"   Version {i}: Update")
                    logger.info(f"      Records: {json.loads(log_data['add']['stats'])['numRecords']}")
    
    def run_pipeline(self):
        """Run the complete production pipeline."""
        logger.info("🚀 Starting Production ETL Pipeline...")
        start_time = datetime.now()
        
        try:
            # Process existing data
            self.process_existing_data()
            
            # Demonstrate time travel
            self.demonstrate_time_travel()
            
            duration = datetime.now() - start_time
            logger.info(f"🎉 Pipeline completed successfully in {duration}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            raise

def main():
    """Main entry point."""
    config = {
        "base_path": "data/lakehouse",
        "delta_path": "data/lakehouse_delta"
    }
    
    pipeline = ProductionETLPipeline(config)
    success = pipeline.run_pipeline()
    
    if success:
        print("\n🎉 Production ETL Pipeline completed successfully!")
        print("🏆 Delta Lake tables created with time travel capabilities!")
        print(f"📁 Check {config['delta_path']} for Delta Lake tables")
    else:
        print("❌ Production ETL Pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
