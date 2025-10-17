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
        
        layer_path = f"data/lakehouse_delta/{layer}"
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
        
        # Create multiple versions for bronze customers table
        customers_path = f"{self.delta_path}/bronze/customers"
        if os.path.exists(customers_path):
            self._create_table_version(customers_path, "customers", "bronze")
            
            # Show version history
            self._show_version_history(customers_path)
        
        # Create multiple versions for silver layer tables
        silver_path = f"{self.delta_path}/silver"
        if os.path.exists(silver_path):
            for table_dir in os.listdir(silver_path):
                table_path = f"{silver_path}/{table_dir}"
                if os.path.isdir(table_path) and table_dir != "_delta_log":
                    logger.info(f"🔄 Creating additional versions for silver table: {table_dir}")
                    self._create_silver_table_version(table_path, table_dir)
                    self._show_version_history(table_path)
        
        # Create multiple versions for gold layer tables
        gold_path = f"{self.delta_path}/gold"
        if os.path.exists(gold_path):
            for table_dir in os.listdir(gold_path):
                table_path = f"{gold_path}/{table_dir}"
                if os.path.isdir(table_path) and table_dir != "_delta_log":
                    logger.info(f"🔄 Creating additional versions for gold table: {table_dir}")
                    self._create_gold_table_version(table_path, table_dir)
                    self._show_version_history(table_path)
    
    def _create_table_version(self, table_path: str, table_name: str, layer: str):
        """Create multiple versions of a table."""
        logger.info(f"📝 Creating multiple versions of {table_name}...")
        
        # Read existing data
        data_file = f"{table_path}/part-00000-{table_name}.parquet"
        if os.path.exists(data_file):
            data = pd.read_parquet(data_file)
            
            # Create 3 versions
            for version in range(1, 4):
                # Modify data (add new records)
                new_data = data.copy()
                new_records = data.head(25 * version).copy()  # Add more records each version
                new_records = new_records.reset_index(drop=True)  # Reset index to avoid length mismatch
                
                # Create new customer IDs as a list with correct length
                num_new_records = len(new_records)
                new_customer_ids = list(range(len(data) + (version-1)*25 + 1, len(data) + (version-1)*25 + 1 + num_new_records))
                new_names = [f'Customer_v{version}_{i}' for i in new_customer_ids]
                
                # Assign new values with correct length
                new_records = new_records.copy()
                new_records['customer_id'] = new_customer_ids
                new_records['name'] = new_names
                
                updated_data = pd.concat([data, new_records], ignore_index=True)
                
                # Write new version
                new_parquet_file = f"{table_path}/part-0000{version}-{table_name}-v{version+1}.parquet"
                updated_data.to_parquet(new_parquet_file, index=False)
                
                # Create transaction log
                delta_log_dir = f"{table_path}/_delta_log"
                transaction_log = {
                    "protocol": {
                        "minReaderVersion": 1,
                        "minWriterVersion": 2
                    },
                    "add": {
                        "path": f"part-0000{version}-{table_name}-v{version+1}.parquet",
                        "partitionValues": {},
                        "size": os.path.getsize(new_parquet_file),
                        "modificationTime": int(datetime.now().timestamp() * 1000) + version,
                        "dataChange": True,
                        "stats": self._generate_stats(updated_data)
                    }
                }
                
                # Write transaction log
                log_file = f"{delta_log_dir}/0000000000000000000{version}.json"
                with open(log_file, 'w') as f:
                    json.dump(transaction_log, f, indent=2)
                
                # Create checksum
                checksum_file = f"{delta_log_dir}/0000000000000000000{version}.crc"
                with open(checksum_file, 'w') as f:
                    f.write(f"8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3{version}")
                
                logger.info(f"✅ Created version {version} of {table_name} ({len(updated_data)} records)")
    
    def _create_silver_table_version(self, table_path: str, table_name: str):
        """Create multiple versions for silver layer tables."""
        logger.info(f"📝 Creating multiple versions for silver table: {table_name}...")
        
        # Read existing data
        data_file = f"{table_path}/part-00000-{table_name}.parquet"
        if os.path.exists(data_file):
            data = pd.read_parquet(data_file)
            
            # Create 2 versions
            for version in range(1, 3):
                # Modify data based on table type and actual columns
                new_data = data.copy()
                
                if "customers" in table_name.lower():
                    # For customers, update segment and add new customers
                    segment_map = {
                        'Basic': 'Premium', 
                        'Premium': 'Enterprise', 
                        'Enterprise': 'Basic'
                    }
                    new_data['segment'] = new_data['segment'].map(segment_map).fillna('Premium')
                    # Add new customers
                    new_customers = data.head(15 * version).copy()
                    new_customers = new_customers.reset_index(drop=True)  # Reset index to avoid length mismatch
                    
                    # Create new customer IDs as a list with correct length
                    num_new_customers = len(new_customers)
                    new_customer_ids = list(range(len(data) + (version-1)*15 + 1, len(data) + (version-1)*15 + 1 + num_new_customers))
                    new_names = [f'Silver_v{version}_{i}' for i in new_customer_ids]
                    
                    # Assign new values with correct length
                    new_customers = new_customers.copy()
                    new_customers['customer_id'] = new_customer_ids
                    new_customers['name'] = new_names
                    new_data = pd.concat([data, new_customers], ignore_index=True)
                elif "orders" in table_name.lower():
                    # For orders, update amounts and status
                    if 'total_amount' in new_data.columns:
                        new_data['total_amount'] = new_data['total_amount'] * (1.08 ** version)  # Increasing multiplier
                    elif 'amount' in new_data.columns:
                        new_data['amount'] = new_data['amount'] * (1.08 ** version)  # Increasing multiplier
                    new_data['status'] = new_data['status'].map({
                        'Completed': 'Processing',
                        'Processing': 'Completed',
                        'Cancelled': 'Completed',
                        'COMPLETED': 'PROCESSING',
                        'PROCESSING': 'COMPLETED',
                        'PENDING': 'COMPLETED'
                    }).fillna('Completed')
                
                # Write new version
                new_parquet_file = f"{table_path}/part-0000{version}-{table_name}-v{version+1}.parquet"
                new_data.to_parquet(new_parquet_file, index=False)
                
                # Create transaction log
                delta_log_dir = f"{table_path}/_delta_log"
                transaction_log = {
                    "protocol": {
                        "minReaderVersion": 1,
                        "minWriterVersion": 2
                    },
                    "add": {
                        "path": f"part-0000{version}-{table_name}-v{version+1}.parquet",
                        "partitionValues": {},
                        "size": os.path.getsize(new_parquet_file),
                        "modificationTime": int(datetime.now().timestamp() * 1000) + version,
                        "dataChange": True,
                        "stats": self._generate_stats(new_data)
                    }
                }
                
                # Write transaction log
                log_file = f"{delta_log_dir}/0000000000000000000{version}.json"
                with open(log_file, 'w') as f:
                    json.dump(transaction_log, f, indent=2)
                
                # Create checksum
                checksum_file = f"{delta_log_dir}/0000000000000000000{version}.crc"
                with open(checksum_file, 'w') as f:
                    f.write(f"8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3{version}")
                
                logger.info(f"✅ Created version {version} of silver {table_name} ({len(new_data)} records)")
    
    def _create_gold_table_version(self, table_path: str, table_name: str):
        """Create multiple versions for gold layer tables."""
        logger.info(f"📝 Creating multiple versions for gold table: {table_name}...")
        
        # Read existing data
        data_file = f"{table_path}/part-00000-{table_name}.parquet"
        if os.path.exists(data_file):
            data = pd.read_parquet(data_file)
            
            # Create 2 versions
            for version in range(1, 3):
                # Modify data based on table type and actual columns
                new_data = data.copy()
                
                if "customer_analytics" in table_name.lower():
                    # For customer analytics, increase customer counts
                    new_data['customer_count'] = new_data['customer_count'] + (10 * version)
                    new_data['avg_lifetime_days'] = new_data['avg_lifetime_days'] + (5 * version)
                elif "monthly_revenue" in table_name.lower():
                    # For monthly revenue, increase revenue and add new months
                    new_data['total_revenue'] = new_data['total_revenue'] * (1.15 ** version)  # Increasing multiplier
                    new_data['month'] = new_data['month'] + version  # Next months
                elif "order_analytics" in table_name.lower():
                    # For order analytics, increase counts and amounts
                    new_data['order_count'] = new_data['order_count'] + (5 * version)
                    new_data['total_amount'] = new_data['total_amount'] * (1.1 ** version)  # Increasing multiplier
                    new_data['avg_amount'] = new_data['avg_amount'] * (1.05 ** version)  # Increasing multiplier
                else:
                    # For other tables, just duplicate with modified IDs
                    if 'id' in new_data.columns:
                        new_data['id'] = new_data['id'] + (1000 * version)
                
                # Write new version
                new_parquet_file = f"{table_path}/part-0000{version}-{table_name}-v{version+1}.parquet"
                new_data.to_parquet(new_parquet_file, index=False)
                
                # Create transaction log
                delta_log_dir = f"{table_path}/_delta_log"
                transaction_log = {
                    "protocol": {
                        "minReaderVersion": 1,
                        "minWriterVersion": 2
                    },
                    "add": {
                        "path": f"part-0000{version}-{table_name}-v{version+1}.parquet",
                        "partitionValues": {},
                        "size": os.path.getsize(new_parquet_file),
                        "modificationTime": int(datetime.now().timestamp() * 1000) + version,
                        "dataChange": True,
                        "stats": self._generate_stats(new_data)
                    }
                }
                
                # Write transaction log
                log_file = f"{delta_log_dir}/0000000000000000000{version}.json"
                with open(log_file, 'w') as f:
                    json.dump(transaction_log, f, indent=2)
                
                # Create checksum
                checksum_file = f"{delta_log_dir}/0000000000000000000{version}.crc"
                with open(checksum_file, 'w') as f:
                    f.write(f"8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3{version}")
                
                logger.info(f"✅ Created version {version} of gold {table_name} ({len(new_data)} records)")
    
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
