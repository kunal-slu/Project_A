# PySpark Data Engineer Project

A comprehensive data engineering project demonstrating production-ready ETL pipelines for both AWS and Azure cloud platforms.

## 📁 Project Structure

```
pyspark_data_engineer_project/
├── aws/                          # AWS-specific implementations
│   ├── config/                   # AWS configuration files
│   │   ├── config-aws-enterprise-internal.yaml
│   │   ├── config-aws-enterprise-simple.yaml
│   │   ├── config-aws-prod.yaml
│   │   ├── config-aws-real-world.yaml
│   │   └── step-functions-workflow.json
│   ├── scripts/                  # AWS ETL scripts and deployment
│   │   ├── aws_enterprise_internal_etl.py
│   │   ├── aws_enterprise_simple_etl.py
│   │   ├── aws_enterprise_deploy.sh
│   │   ├── aws_real_world_etl.py
│   │   ├── aws_production_etl.py
│   │   ├── glue_etl_job.py
│   │   ├── install-delta.sh
│   │   └── lambda_functions/
│   ├── docs/                     # AWS documentation
│   │   ├── ENTERPRISE_AWS_ETL_GUIDE.md
│   │   ├── AWS_DEPLOYMENT_GUIDE.md
│   │   ├── AWS_REAL_WORLD_DATA_SOURCES.md
│   │   └── ENTERPRISE_ETL_3_SOURCES.md
│   ├── notebooks/                # AWS Databricks notebooks
│   └── requirements-enterprise.txt
├── azure/                        # Azure-specific implementations
│   ├── config/                   # Azure configuration files
│   │   ├── config-azure-dev.yaml
│   │   └── config-azure-prod.yaml
│   ├── scripts/                  # Azure deployment scripts
│   │   └── azure_deploy.sh
│   ├── docs/                     # Azure documentation
│   │   ├── AZURE_DEPLOYMENT_GUIDE.md
│   │   ├── AZURE_QUICK_START.md
│   │   ├── AZURE_COMPLETE_DEPLOYMENT.md
│   │   └── AZURE_UPLOAD_OPTIONS.md
│   └── notebooks/                # Azure Databricks notebooks
│       ├── azure_etl_pipeline.py
│       ├── simple_azure_etl.py
│       └── complete_azure_etl.py
├── src/                          # Core PySpark project code
│   └── pyspark_interview_project/
├── config/                       # Shared configuration files
│   └── contracts/                # Schema contracts for Gold layer
├── scripts/                      # Shared scripts
│   └── run_enterprise_etl.sh     # Main runner script
├── data/                         # Sample data and test files
├── tests/                        # Test files
├── docs/                         # Project documentation
└── README.md                     # This file
```

## 🏗️ Architecture & Data Quality

### Schema Evolution Policy

This project implements a robust schema evolution policy across the data lakehouse layers:

#### **Bronze Layer (Raw Data)**
- **Policy**: Permissive - accepts any schema
- **Purpose**: Raw data ingestion without constraints
- **Validation**: Minimal validation, focuses on data arrival

#### **Silver Layer (Cleaned Data)**
- **Policy**: Fixed schema - fails on breaking changes
- **Purpose**: Data quality and consistency
- **Validation**: 
  - Schema comparison with existing tables
  - Data quality gates (null checks, uniqueness, etc.)
  - Fail-fast on breaking changes

#### **Gold Layer (Business Data)**
- **Policy**: Contract schema - enforces JSON contracts
- **Purpose**: Business-ready data with strict contracts
- **Validation**:
  - JSON schema contracts in `config/contracts/`
  - Required columns, data types, constraints
  - Foreign key relationships
  - Partitioning and indexing rules

#### **Usage Example**
```python
from pyspark_interview_project.schema_validator import validate_schema_evolution

# Bronze layer - always passes
result = validate_schema_evolution(df, "raw_orders", "bronze", spark)

# Silver layer - validates against existing schema
result = validate_schema_evolution(df, "cleaned_orders", "silver", spark)

# Gold layer - validates against JSON contract
result = validate_schema_evolution(df, "fact_orders", "gold", spark)
```

### Data Quality Framework

The project includes comprehensive data quality checks:

- **Null Value Validation**: `require_not_null()`
- **Unique Key Validation**: `require_unique_keys()`
- **Control Totals**: `control_total()`
- **Schema Validation**: `validate_schema()`
- **Custom Business Rules**: Extensible framework

## 🚀 Quick Start

### AWS ETL Pipeline
```bash
# Run the complete AWS enterprise ETL pipeline
./scripts/run_enterprise_etl.sh

# Or deploy AWS infrastructure first
./aws/scripts/aws_enterprise_deploy.sh
```

### Azure ETL Pipeline
```bash
# Deploy Azure infrastructure
./azure/scripts/azure_deploy.sh

# Run Azure ETL pipeline in Databricks
# Upload azure/notebooks/complete_azure_etl.py to Azure Databricks
```

## 📊 Available Implementations

### AWS Implementations

#### 1. **Enterprise ETL Pipeline (1 External + 2 Internal Sources)**
- **Location**: `aws/`
- **Data Sources**: 
  - External: Snowflake Data Warehouse
  - Internal: PostgreSQL Database
  - Internal: Apache Kafka Streaming
- **Features**: Production-ready with monitoring, security, and scalability
- **Run**: `./scripts/run_enterprise_etl.sh`

#### 2. **Simple ETL Pipeline (3 External Sources)**
- **Location**: `aws/`
- **Data Sources**: Snowflake, Salesforce API, Kafka
- **Features**: Simplified enterprise implementation
- **Run**: `python3 aws/scripts/aws_enterprise_simple_etl.py aws/config/config-aws-enterprise-simple.yaml`

#### 3. **Real-World ETL Pipeline**
- **Location**: `aws/`
- **Data Sources**: Multiple external and internal sources
- **Features**: Comprehensive real-world data sources
- **Run**: `python3 aws/scripts/aws_real_world_etl.py aws/config/config-aws-real-world.yaml`

### Azure Implementations

#### 1. **Complete Azure ETL Pipeline**
- **Location**: `azure/`
- **Platform**: Azure Databricks
- **Features**: Full end-to-end ETL pipeline
- **Run**: Upload `azure/notebooks/complete_azure_etl.py` to Databricks

#### 2. **Simple Azure ETL Pipeline**
- **Location**: `azure/`
- **Platform**: Azure Databricks
- **Features**: Minimal implementation for quick start
- **Run**: Upload `azure/notebooks/simple_azure_etl.py` to Databricks

## 🏗️ Architecture

### AWS Architecture
```
External Sources → Internal Sources → ETL Pipeline → Data Lake → Analytics
     Snowflake         PostgreSQL      Spark/EMR      S3         BI Tools
                        Kafka
```

### Azure Architecture
```
Data Sources → ETL Pipeline → Data Lake → Analytics
              Databricks     ADLS Gen2    Power BI
```

## 📚 Documentation

### AWS Documentation
- **Complete Guide**: `aws/docs/ENTERPRISE_AWS_ETL_GUIDE.md`
- **Deployment Guide**: `aws/docs/AWS_DEPLOYMENT_GUIDE.md`
- **Real-World Sources**: `aws/docs/AWS_REAL_WORLD_DATA_SOURCES.md`

### Azure Documentation
- **Deployment Guide**: `azure/docs/AZURE_DEPLOYMENT_GUIDE.md`
- **Quick Start**: `azure/docs/AZURE_QUICK_START.md`
- **Complete Deployment**: `azure/docs/AZURE_COMPLETE_DEPLOYMENT.md`

## 🔧 Prerequisites

### For AWS
- AWS CLI configured
- Python 3.8+
- Required Python packages (see `aws/requirements-enterprise.txt`)

### For Azure
- Azure CLI configured
- Azure Databricks workspace
- Required Python packages (installed in Databricks)

## 🎯 Key Features

### Enterprise-Grade Features
- **Multi-Cloud Support**: AWS and Azure implementations
- **Production Ready**: Monitoring, security, scalability
- **Real-World Data Sources**: External and internal data integration
- **Data Quality**: Automated validation and monitoring
- **Security**: Encryption, IAM, compliance (GDPR, CCPA, SOX)
- **Monitoring**: CloudWatch/Azure Monitor integration
- **Documentation**: Comprehensive guides and examples

### Data Engineering Best Practices
- **Incremental Processing**: Change data capture and watermarks
- **Error Handling**: Comprehensive error handling and retry logic
- **Performance Optimization**: Partitioning, caching, Z-ordering
- **Data Lineage**: Track data flow from source to consumption
- **Testing**: Unit tests and integration tests

## 🚀 Getting Started

1. **Choose your platform**: AWS or Azure
2. **Review documentation**: Check the respective docs folder
3. **Set up credentials**: Configure cloud credentials
4. **Run the pipeline**: Use the provided scripts

## 📞 Support

For questions or issues:
- **AWS**: Check `aws/docs/` for troubleshooting
- **Azure**: Check `azure/docs/` for troubleshooting
- **General**: Review project documentation in `docs/`

---

**This project demonstrates production-ready data engineering implementations for both AWS and Azure cloud platforms, with comprehensive documentation and real-world use cases.** 🚀

# 🚀 **COMPREHENSIVE DATA ENGINEERING PROJECT ENHANCEMENT PLAN**

## 📊 **CURRENT STATE ANALYSIS**

Based on my analysis, this project already has a solid foundation but needs several enhancements to reach the top 10% level. Here's what I found:

### ✅ **STRENGTHS (Already Present)**
- Enterprise-grade architecture with modular design
- Comprehensive AWS and Azure support
- Advanced SCD2 implementation
- Data quality monitoring and validation
- Disaster recovery capabilities
- Streaming and batch processing
- Security and compliance features

### ❌ **CRITICAL GAPS TO FILL**

1. **Real Data Source Integrations** - Currently using mock/sample data
2. **Production Cloud Deployments** - Missing actual cloud infrastructure
3. **Advanced Monitoring & Observability** - Limited production monitoring
4. **Data Lineage & Governance** - Basic implementation
5. **Performance Optimization** - Needs real-world tuning
6. **CI/CD Pipeline** - Missing automated deployment
7. **Cost Optimization** - No cost management features
8. **Advanced Analytics** - Missing ML/MLOps capabilities

---

# 🎯 **TOP 10% ENHANCEMENT ROADMAP**

## **PHASE 1: REAL DATA SOURCE INTEGRATIONS** 🔌

### **1.1 Snowflake Integration**
```python
# Create real Snowflake connector
class SnowflakeConnector:
    def __init__(self, config):
        self.account = config['snowflake']['account']
        self.warehouse = config['snowflake']['warehouse']
        self.database = config['snowflake']['database']
        self.schema = config['snowflake']['schema']
        self.role = config['snowflake']['role']
        
    def extract_data(self, table_name, query=None):
        """Extract data from Snowflake with proper error handling"""
        try:
            conn = snowflake.connector.connect(
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                user=os.getenv('SNOWFLAKE_USERNAME'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                role=self.role
            )
            
            if query:
                df = pd.read_sql(query, conn)
            else:
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                
            return df
        except Exception as e:
            logger.error(f"Snowflake extraction failed: {e}")
            raise
```

### **1.2 Real API Integrations**
```python
# Salesforce API Integration
class SalesforceConnector:
    def __init__(self, config):
        self.base_url = config['salesforce']['base_url']
        self.api_version = config['salesforce']['api_version']
        self.access_token = self._authenticate()
        
    def _authenticate(self):
        """OAuth2 authentication with Salesforce"""
        # Implementation for OAuth2 flow
        pass
        
    def extract_leads(self, last_modified=None):
        """Extract leads with incremental loading"""
        query = "SELECT Id, Name, Email, Company FROM Lead"
        if last_modified:
            query += f" WHERE LastModifiedDate > {last_modified}"
            
        return self._make_api_call(f"/services/data/v{self.api_version}/query/?q={query}")

# Stripe API Integration
class StripeConnector:
    def __init__(self, config):
        self.api_key = os.getenv('STRIPE_SECRET_KEY')
        self.base_url = "https://api.stripe.com/v1"
        
    def extract_charges(self, created_after=None):
        """Extract payment charges with pagination"""
        params = {'limit': 100}
        if created_after:
            params['created'] = {'gte': created_after}
            
        return self._paginated_request('/charges', params)
```

### **1.3 Real-time Streaming Sources**
```python
# Kafka Consumer with Schema Registry
class KafkaStreamingConnector:
    def __init__(self, config):
        self.bootstrap_servers = config['kafka']['bootstrap_servers']
        self.schema_registry_url = config['kafka']['schema_registry_url']
        self.consumer = self._create_consumer()
        
    def _create_consumer(self):
        """Create Kafka consumer with proper configuration"""
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'etl-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_USERNAME'),
            'sasl.password': os.getenv('KAFKA_PASSWORD')
        }
        return KafkaConsumer(**consumer_config)
```

---

## **PHASE 2: PRODUCTION CLOUD DEPLOYMENTS** ☁️

### **2.1 AWS Production Deployment**

#### **Infrastructure as Code (Terraform)**
```hcl
# terraform/aws/main.tf
provider "aws" {
  region = var.aws_region
}

# EMR Cluster
resource "aws_emr_cluster" "etl_cluster" {
  name          = "production-etl-cluster"
  release_label = "emr-6.15.0"
  
  applications = ["Spark", "Hive", "Hadoop", "Delta"]
  
  ec2_attributes {
    subnet_id                         = aws_subnet.private.id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
  }
  
  master_instance_group {
    instance_type = "m5.2xlarge"
    instance_count = 1
  }
  
  core_instance_group {
    instance_type  = "m5.2xlarge"
    instance_count = 3
  }
  
  configurations_json = jsonencode([
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4",
        "spark.driver.memory": "4g"
      }
    }
  ])
  
  service_role = aws_iam_role.emr_service_role.arn
  autoscaling_role = aws_iam_role.emr_autoscaling_role.arn
  
  log_uri = "s3://${aws_s3_bucket.logs.bucket}/emr-logs/"
  
  bootstrap_action {
    path = "s3://${aws_s3_bucket.artifacts.bucket}/bootstrap/install-dependencies.sh"
    name = "Install Dependencies"
  }
}

# S3 Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-lake-${random_id.bucket_suffix.hex}"
  
  versioning {
    enabled = true
  }
  
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
  
  lifecycle_rule {
    id = "data_lifecycle"
    enabled = true
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 2555  # 7 years
    }
  }
}

# Glue Data Catalog
resource "aws_glue_catalog_database" "etl_database" {
  name = "${var.project_name}_data_catalog"
}

resource "aws_glue_crawler" "etl_crawler" {
  database_name = aws_glue_catalog_database.etl_database.name
  name          = "${var.project_name}-crawler"
  role          = aws_iam_role.glue_role.arn
  
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/"
  }
  
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/"
  }
  
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/gold/"
  }
}

# Step Functions for Orchestration
resource "aws_sfn_state_machine" "etl_workflow" {
  name     = "${var.project_name}-etl-workflow"
  role_arn = aws_iam_role.step_functions_role.arn
  
  definition = jsonencode({
    Comment = "ETL Pipeline Workflow"
    StartAt = "ValidateInput"
    States = {
      ValidateInput = {
        Type = "Task"
        Resource = aws_lambda_function.validate_input.arn
        Next = "StartEMRCluster"
      }
      StartEMRCluster = {
        Type = "Task"
        Resource = aws_lambda_function.start_emr.arn
        Next = "WaitForCluster"
      }
      WaitForCluster = {
        Type = "Wait"
        Seconds = 300
        Next = "RunETLJob"
      }
      RunETLJob = {
        Type = "Task"
        Resource = aws_lambda_function.run_etl.arn
        Next = "DataQualityChecks"
      }
      DataQualityChecks = {
        Type = "Task"
        Resource = aws_lambda_function.data_quality.arn
        Next = "TerminateCluster"
      }
      TerminateCluster = {
        Type = "Task"
        Resource = aws_lambda_function.terminate_emr.arn
        End = true
      }
    }
  })
}
```

#### **AWS Lambda Functions**
```python
# aws/lambda_functions/start_emr_cluster.py
import boto3
import json
import os

def lambda_handler(event, context):
    """Start EMR cluster for ETL processing."""
    emr = boto3.client('emr')
    
    cluster_config = {
        'Name': 'production-etl-cluster',
        'ReleaseLabel': 'emr-6.15.0',
        'Applications': [
            {'Name': 'Spark'},
            {'Name': 'Hive'},
            {'Name': 'Hadoop'}
        ],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.2xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.2xlarge',
                    'InstanceCount': 3
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        'Steps': [
            {
                'Name': 'Run ETL Pipeline',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--class', 'com.company.etl.Main',
                        's3://company-artifacts/etl-pipeline.jar',
                        '--config', 's3://company-artifacts/config/production.yaml'
                    ]
                }
            }
        ],
        'ServiceRole': 'EMR_DefaultRole',
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'LogUri': 's3://company-logs/emr-logs/',
        'BootstrapActions': [
            {
                'Name': 'Install Dependencies',
                'ScriptBootstrapAction': {
                    'Path': 's3://company-artifacts/bootstrap/install-dependencies.sh'
                }
            }
        ]
    }
    
    response = emr.create_cluster(**cluster_config)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'clusterId': response['ClusterId'],
            'message': 'EMR cluster creation initiated'
        })
    }
```

### **2.2 Azure Production Deployment**

#### **Azure Resource Manager Template**
```json
{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "projectName": {
      "type": "string",
      "defaultValue": "pyspark-etl"
    },
    "location": {
      "type": "string",
      "defaultValue": "[resourceGroup().location]"
    }
  },
  "variables": {
    "storageAccountName": "[concat(parameters('projectName'), 'storage', uniqueString(resourceGroup().id))]",
    "dataFactoryName": "[concat(parameters('projectName'), '-adf')]",
    "databricksWorkspaceName": "[concat(parameters('projectName'), '-databricks')]",
    "keyVaultName": "[concat(parameters('projectName'), '-kv')]"
  },
  "resources": [
    {
      "type": "Microsoft.Storage/storageAccounts",
      "apiVersion": "2021-09-01",
      "name": "[variables('storageAccountName')]",
      "location": "[parameters('location')]",
      "sku": {
        "name": "Standard_LRS"
      },
      "kind": "StorageV2",
      "properties": {
        "accessTier": "Hot",
        "supportsHttpsTrafficOnly": true,
        "isHnsEnabled": true
      }
    },
    {
      "type": "Microsoft.DataFactory/factories",
      "apiVersion": "2018-06-01",
      "name": "[variables('dataFactoryName')]",
      "location": "[parameters('location')]",
      "identity": {
        "type": "SystemAssigned"
      }
    },
    {
      "type": "Microsoft.Databricks/workspaces",
      "apiVersion": "2018-04-01",
      "name": "[variables('databricksWorkspaceName')]",
      "location": "[parameters('location')]",
      "sku": {
        "name": "premium"
      },
      "properties": {
        "managedResourceGroupId": "[concat(subscription().id, '/resourceGroups/', parameters('projectName'), '-databricks-rg')]"
      }
    },
    {
      "type": "Microsoft.KeyVault/vaults",
      "apiVersion": "2021-10-01",
      "name": "[variables('keyVaultName')]",
      "location": "[parameters('location')]",
      "properties": {
        "tenantId": "[subscription().tenantId]",
        "sku": {
          "family": "A",
          "name": "standard"
        },
        "accessPolicies": [
          {
            "tenantId": "[subscription().tenantId]",
            "objectId": "[reference(variables('dataFactoryName'), '2018-06-01', 'Full').identity.principalId]",
            "permissions": {
              "secrets": ["get", "list"]
            }
          }
        ],
        "enabledForDeployment": false,
        "enabledForDiskEncryption": false,
        "enabledForTemplateDeployment": true
      }
    }
  ]
}
```

---

## **PHASE 3: ADVANCED MONITORING & OBSERVABILITY** 📊

### **3.1 Comprehensive Monitoring Stack**

#### **Prometheus + Grafana Integration**
```python
# monitoring/prometheus_metrics.py
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time

class ETLMetrics:
    def __init__(self):
        # Pipeline metrics
        self.pipeline_runs = Counter('etl_pipeline_runs_total', 'Total pipeline runs', ['status'])
        self.pipeline_duration = Histogram('etl_pipeline_duration_seconds', 'Pipeline duration')
        self.records_processed = Counter('etl_records_processed_total', 'Records processed', ['source', 'stage'])
        
        # Data quality metrics
        self.data_quality_score = Gauge('etl_data_quality_score', 'Data quality score', ['table'])
        self.data_quality_violations = Counter('etl_data_quality_violations_total', 'Data quality violations', ['rule', 'table'])
        
        # Performance metrics
        self.spark_job_duration = Histogram('etl_spark_job_duration_seconds', 'Spark job duration', ['job_name'])
        self.spark_stage_duration = Histogram('etl_spark_stage_duration_seconds', 'Spark stage duration', ['stage_name'])
        self.memory_usage = Gauge('etl_memory_usage_bytes', 'Memory usage', ['component'])
        self.cpu_usage = Gauge('etl_cpu_usage_percent', 'CPU usage', ['component'])
        
        # Business metrics
        self.revenue_processed = Counter('etl_revenue_processed_total', 'Revenue processed', ['currency'])
        self.customers_processed = Counter('etl_customers_processed_total', 'Customers processed')
        self.orders_processed = Counter('etl_orders_processed_total', 'Orders processed')
        
    def record_pipeline_start(self):
        self.pipeline_runs.labels(status='started').inc()
        
    def record_pipeline_success(self, duration):
        self.pipeline_runs.labels(status='success').inc()
        self.pipeline_duration.observe(duration)
        
    def record_pipeline_failure(self, duration):
        self.pipeline_runs.labels(status='failure').inc()
        self.pipeline_duration.observe(duration)
        
    def record_records_processed(self, source, stage, count):
        self.records_processed.labels(source=source, stage=stage).inc(count)
        
    def record_data_quality_score(self, table, score):
        self.data_quality_score.labels(table=table).set(score)
        
    def record_business_metrics(self, revenue, customers, orders):
        self.revenue_processed.labels(currency='USD').inc(revenue)
        self.customers_processed.inc(customers)
        self.orders_processed.inc(orders)
```

#### **Grafana Dashboard Configuration**
```json
{
  "dashboard": {
    "title": "ETL Pipeline Monitoring",
    "panels": [
      {
        "title": "Pipeline Success Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(etl_pipeline_runs_total{status=\"success\"}[5m]) / rate(etl_pipeline_runs_total[5m]) * 100",
            "legendFormat": "Success Rate %"
          }
        ]
      },
      {
        "title": "Pipeline Duration",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(etl_pipeline_duration_seconds_bucket[5m]))",
            "legendFormat": "95th Percentile"
          },
          {
            "expr": "histogram_quantile(0.50, rate(etl_pipeline_duration_seconds_bucket[5m]))",
            "legendFormat": "50th Percentile"
          }
        ]
      },
      {
        "title": "Data Quality Scores",
        "type": "graph",
        "targets": [
          {
            "expr": "etl_data_quality_score",
            "legendFormat": "{{table}}"
          }
        ]
      },
      {
        "title": "Records Processed",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(etl_records_processed_total[5m])",
            "legendFormat": "{{source}} - {{stage}}"
          }
        ]
      }
    ]
  }
}
```

### **3.2 Data Lineage Tracking**

#### **OpenLineage Integration**
```python
# lineage/openlineage_integration.py
from openlineage.client import OpenLineageClient
from openlineage.client.facet import SchemaDatasetFacet, DataSourceDatasetFacet
from openlineage.client.run import Run, Job, Dataset

class DataLineageTracker:
    def __init__(self, config):
        self.client = OpenLineageClient(
            url=config['openlineage']['url'],
            api_key=config['openlineage']['api_key']
        )
        self.namespace = config['openlineage']['namespace']
        
    def start_run(self, job_name, run_id):
        """Start a new lineage run"""
        run = Run(
            runId=run_id,
            facets={}
        )
        
        job = Job(
            namespace=self.namespace,
            name=job_name,
            facets={}
        )
        
        self.client.emit_start(run, job)
        
    def emit_input_dataset(self, run_id, dataset_name, schema, source):
        """Emit input dataset lineage"""
        dataset = Dataset(
            namespace=self.namespace,
            name=dataset_name,
            facets={
                "schema": SchemaDatasetFacet(fields=schema),
                "dataSource": DataSourceDatasetFacet(
                    name=source,
                    uri=f"s3://{source}/{dataset_name}"
                )
            }
        )
        
        self.client.emit_input(run_id, dataset)
        
    def emit_output_dataset(self, run_id, dataset_name, schema, destination):
        """Emit output dataset lineage"""
        dataset = Dataset(
            namespace=self.namespace,
            name=dataset_name,
            facets={
                "schema": SchemaDatasetFacet(fields=schema),
                "dataSource": DataSourceDatasetFacet(
                    name=destination,
                    uri=f"s3://{destination}/{dataset_name}"
                )
            }
        )
        
        self.client.emit_output(run_id, dataset)
        
    def complete_run(self, run_id, job_name, status="COMPLETE"):
        """Complete a lineage run"""
        run = Run(
            runId=run_id,
            facets={}
        )
        
        job = Job(
            namespace=self.namespace,
            name=job_name,
            facets={}
        )
        
        self.client.emit_complete(run, job)
```

---

## **PHASE 4: ADVANCED ANALYTICS & MLOps** 🤖

### **4.1 Machine Learning Pipeline Integration**

#### **MLflow Integration**
```python
# ml/mlflow_integration.py
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

class MLPipeline:
    def __init__(self, config):
        self.mlflow_uri = config['mlflow']['tracking_uri']
        self.experiment_name = config['mlflow']['experiment_name']
        mlflow.set_tracking_uri(self.mlflow_uri)
        mlflow.set_experiment(self.experiment_name)
        
    def train_customer_churn_model(self, spark, training_data_path):
        """Train customer churn prediction model"""
        with mlflow.start_run():
            # Load training data
            df = spark.read.format("delta").load(training_data_path)
            
            # Feature engineering
            feature_cols = ['age', 'income', 'spending_score', 'tenure_months']
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features"
            )
            
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features"
            )
            
            # Model training
            rf = RandomForestClassifier(
                featuresCol="scaled_features",
                labelCol="churn",
                numTrees=100,
                maxDepth=10
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, scaler, rf])
            model = pipeline.fit(df)
            
            # Evaluate model
            predictions = model.transform(df)
            evaluator = BinaryClassificationEvaluator(
                labelCol="churn",
                rawPredictionCol="rawPrediction"
            )
            auc = evaluator.evaluate(predictions)
            
            # Log metrics and model
            mlflow.log_metric("auc", auc)
            mlflow.log_param("num_trees", 100)
            mlflow.log_param("max_depth", 10)
            
            # Log model
            mlflow.spark.log_model(
                model,
                "churn_model",
                registered_model_name="customer_churn_model"
            )
            
            return model, auc
            
    def batch_predict(self, spark, model_path, input_data_path, output_path):
        """Run batch prediction"""
        # Load model
        model = mlflow.spark.load_model(model_path)
        
        # Load data
        df = spark.read.format("delta").load(input_data_path)
        
        # Make predictions
        predictions = model.transform(df)
        
        # Save predictions
        predictions.write.format("delta").mode("overwrite").save(output_path)
        
        return predictions
```

### **4.2 Real-time Feature Store**

#### **Feature Store Implementation**
```python
# features/feature_store.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

class FeatureStore:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.feature_store_path = config['feature_store']['path']
        
    def create_customer_features(self, customer_df, orders_df):
        """Create customer features for ML"""
        # Calculate features
        customer_features = customer_df.join(
            orders_df.groupBy("customer_id").agg(
                count("order_id").alias("total_orders"),
                sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_order_value"),
                max("order_date").alias("last_order_date"),
                min("order_date").alias("first_order_date")
            ),
            "customer_id",
            "left"
        )
        
        # Calculate recency, frequency, monetary (RFM) features
        customer_features = customer_features.withColumn(
            "days_since_last_order",
            datediff(current_date(), col("last_order_date"))
        ).withColumn(
            "customer_tenure_days",
            datediff(col("last_order_date"), col("first_order_date"))
        ).withColumn(
            "order_frequency",
            col("total_orders") / greatest(col("customer_tenure_days"), 1)
        )
        
        # Save to feature store
        customer_features.write.format("delta").mode("overwrite").save(
            f"{self.feature_store_path}/customer_features"
        )
        
        return customer_features
        
    def get_features_for_prediction(self, customer_ids):
        """Get features for specific customers"""
        features_df = self.spark.read.format("delta").load(
            f"{self.feature_store_path}/customer_features"
        )
        
        return features_df.filter(col("customer_id").isin(customer_ids))
```

---

## **PHASE 5: COST OPTIMIZATION & FINANCIAL GOVERNANCE** 💰

### **5.1 Cost Monitoring and Optimization**

#### **AWS Cost Optimization**
```python
# cost_optimization/aws_cost_monitor.py
import boto3
from datetime import datetime, timedelta

class AWSCostMonitor:
    def __init__(self, config):
        self.ce_client = boto3.client('ce')
        self.cloudwatch = boto3.client('cloudwatch')
        
    def get_daily_costs(self, start_date, end_date):
        """Get daily costs for ETL resources"""
        response = self.ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date.strftime('%Y-%m-%d'),
                'End': end_date.strftime('%Y-%m-%d')
            },
            Granularity='DAILY',
            Metrics=['BlendedCost'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'SERVICE'},
                {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}
            ]
        )
        return response
        
    def optimize_s3_storage(self, bucket_name):
        """Optimize S3 storage costs"""
        s3_client = boto3.client('s3')
        
        # Get storage class recommendations
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        for obj in response.get('Contents', []):
            # Move old files to cheaper storage
            if obj['LastModified'] < datetime.now() - timedelta(days=30):
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                    Key=obj['Key'],
                    StorageClass='STANDARD_IA'
                )
                
    def optimize_emr_cluster(self, cluster_id):
        """Optimize EMR cluster configuration"""
        emr_client = boto3.client('emr')
        
        # Get cluster metrics
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        
        # Implement auto-scaling based on workload
        # This would integrate with CloudWatch metrics
        pass
```

#### **Azure Cost Optimization**
```python
# cost_optimization/azure_cost_monitor.py
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import QueryDefinition, QueryTimePeriod

class AzureCostMonitor:
    def __init__(self, config):
        self.cost_client = CostManagementClient(
            config['azure']['credential'],
            config['azure']['subscription_id']
        )
        
    def get_resource_costs(self, resource_group, start_date, end_date):
        """Get costs for specific resource group"""
        query = QueryDefinition(
            type="ActualCost",
            timeframe="Custom",
            time_period=QueryTimePeriod(
                from_property=start_date,
                to=end_date
            ),
            dataset={
                "granularity": "Daily",
                "grouping": [
                    {"type": "Dimension", "name": "ResourceGroupName"},
                    {"type": "Dimension", "name": "ResourceType"}
                ],
                "aggregation": {
                    "totalCost": {"name": "PreTaxCost", "function": "Sum"}
                }
            }
        )
        
        result = self.cost_client.query.usage(
            scope=f"/subscriptions/{config['azure']['subscription_id']}/resourceGroups/{resource_group}",
            parameters=query
        )
        
        return result
```

---

## **PHASE 6: COMPREHENSIVE TESTING & QUALITY ASSURANCE** 🧪

### **6.1 Advanced Testing Framework**

#### **Property-Based Testing**
```python
# tests/property_based_tests.py
from hypothesis import given, strategies as st
import pytest

class PropertyBasedTests:
    @given(st.lists(st.integers(min_value=0, max_value=1000), min_size=1))
    def test_data_quality_rules_always_pass(self, data):
        """Test that data quality rules are always satisfied"""
        # Create DataFrame with test data
        df = spark.createDataFrame([(i,) for i in data], ["value"])
        
        # Apply data quality rules
        dq_result = apply_data_quality_rules(df)
        
        # Assert all rules pass
        assert dq_result.violations.count() == 0
        
    @given(st.lists(st.dictionaries(
        keys=st.text(min_size=1, max_size=10),
        values=st.text(min_size=1, max_size=50)
    ), min_size=1))
    def test_etl_pipeline_idempotency(self, input_data):
        """Test that ETL pipeline is idempotent"""
        # Run pipeline twice with same input
        result1 = run_etl_pipeline(input_data)
        result2 = run_etl_pipeline(input_data)
        
        # Results should be identical
        assert result1.equals(result2)
```

#### **Performance Testing**
```python
# tests/performance_tests.py
import time
import pytest
from memory_profiler import profile

class PerformanceTests:
    @pytest.mark.performance
    def test_pipeline_performance_sla(self):
        """Test that pipeline meets SLA requirements"""
        start_time = time.time()
        
        # Run full pipeline
        result = run_complete_pipeline()
        
        duration = time.time() - start_time
        
        # Assert SLA compliance (e.g., < 30 minutes)
        assert duration < 1800, f"Pipeline took {duration}s, exceeds 30min SLA"
        
    @pytest.mark.performance
    @profile
    def test_memory_usage(self):
        """Test memory usage stays within limits"""
        # Run memory-intensive operations
        result = process_large_dataset()
        
        # Memory usage should be reasonable
        # This would integrate with memory profiling tools
        pass
        
    @pytest.mark.performance
    def test_throughput_requirements(self):
        """Test data processing throughput"""
        # Process known volume of data
        input_size = 1000000  # 1M records
        start_time = time.time()
        
        result = process_data(input_size)
        
        duration = time.time() - start_time
        throughput = input_size / duration
        
        # Assert minimum throughput (e.g., 10K records/second)
        assert throughput > 10000, f"Throughput {throughput} records/sec below requirement"
```

---

## **PHASE 7: COMPLETE DEPLOYMENT SCRIPTS** 🚀

### **7.1 AWS Complete Deployment**

#### **Master Deployment Script**
```bash
#!/bin/bash
# deploy_aws_complete.sh

set -e

# Configuration
PROJECT_NAME="pyspark-etl-enterprise"
AWS_REGION="us-east-1"
ENVIRONMENT="production"

echo " Starting Complete AWS Deployment for $PROJECT_NAME"

# 1. Deploy Infrastructure
echo " Deploying Infrastructure with Terraform..."
cd terraform/aws
terraform init
terraform plan -var="project_name=$PROJECT_NAME" -var="aws_region=$AWS_REGION"
terraform apply -auto-approve

# 2. Deploy Application Code
echo "📦 Deploying Application Code..."
aws s3 cp ../src/ s3://$PROJECT_NAME-artifacts/code/ --recursive
aws s3 cp ../config/ s3://$PROJECT_NAME-artifacts/config/ --recursive

# 3. Deploy Lambda Functions
echo "📦 Deploying Lambda Functions..."
cd ../lambda_functions
for func in */; do
    echo "Deploying $func"
    cd $func
    zip -r function.zip .
    aws lambda create-function \
        --function-name $PROJECT_NAME-${func%/} \
        --runtime python3.9 \
        --role arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/lambda-execution-role \
        --handler lambda_function.lambda_handler \
        --zip-file fileb://function.zip
    cd ..
done

# 4. Deploy Step Functions
echo "📦 Deploying Step Functions..."
aws stepfunctions create-state-machine \
    --name $PROJECT_NAME-workflow \
    --definition file://step_functions_workflow.json \
    --role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/stepfunctions-execution-role

# 5. Deploy Monitoring
echo " Deploying Monitoring Stack..."
cd ../monitoring
helm install prometheus prometheus-community/kube-prometheus-stack
helm install grafana grafana/grafana

# 6. Run Initial Data Pipeline
echo "📦 Running Initial Data Pipeline..."
aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:$AWS_REGION:$(aws sts get-caller-identity --query Account --output text):stateMachine:$PROJECT_NAME-workflow \
    --name "initial-pipeline-run"

echo "✅ AWS Deployment Complete!"
echo " Monitor at: https://console.aws.amazon.com/cloudwatch/"
echo " Grafana at: http://grafana.example.com"
```

### **7.2 Azure Complete Deployment**

#### **Master Deployment Script**
```bash
#!/bin/bash
# deploy_azure_complete.sh

set -e

# Configuration
PROJECT_NAME="pyspark-etl-enterprise"
RESOURCE_GROUP="rg-$PROJECT_NAME"
LOCATION="East US"

echo "🚀 Starting Complete Azure Deployment for $PROJECT_NAME"

# 1. Create Resource Group
echo " Creating Resource Group..."
az group create --name $RESOURCE_GROUP --location "$LOCATION"

# 2. Deploy Infrastructure
echo " Deploying Infrastructure with ARM Template..."
az deployment group create \
    --resource-group $RESOURCE_GROUP \
    --template-file azure/main.json \
    --parameters projectName=$PROJECT_NAME location="$LOCATION"

# 3. Deploy Data Factory Pipelines
echo " Deploying Data Factory Pipelines..."
az datafactory pipeline create \
    --resource-group $RESOURCE_GROUP \
    --factory-name $PROJECT_NAME-adf \
    --name "ETL-Pipeline" \
    --pipeline-file azure/data_factory_pipeline.json

# 4. Deploy Databricks Workspace
echo "📦 Deploying Databricks Workspace..."
az databricks workspace create \
    --resource-group $RESOURCE_GROUP \
    --name $PROJECT_NAME-databricks \
    --location "$LOCATION" \
    --sku premium

# 5. Deploy Monitoring
echo "📦 Deploying Monitoring Stack..."
az monitor log-analytics workspace create \
    --resource-group $RESOURCE_GROUP \
    --workspace-name $PROJECT_NAME-logs

# 6. Deploy Application Insights
az monitor app-insights component create \
    --app $PROJECT_NAME-insights \
    --location "$LOCATION" \
    --resource-group $RESOURCE_GROUP

echo "✅ Azure Deployment Complete!"
echo " Monitor at: https://portal.azure.com"
echo "📈 Databricks at: https://$PROJECT_NAME-databricks.azuredatabricks.net"
```

---

## **PHASE 8: PRODUCTION READINESS CHECKLIST** ✅

### **8.1 Security & Compliance**

#### **Security Hardening**
```python
# security/security_hardener.py
class SecurityHardener:
    def __init__(self, config):
        self.config = config
        
    def enable_encryption_at_rest(self):
        """Enable encryption for all storage"""
        # S3 bucket encryption
        # Azure Storage encryption
        # Database encryption
        pass
        
    def enable_encryption_in_transit(self):
        """Enable TLS/SSL for all communications"""
        # API endpoints
        # Database connections
        # Inter-service communication
        pass
        
    def implement_network_security(self):
        """Implement network security controls"""
        # VPC/Network Security Groups
        # Private endpoints
        # Firewall rules
        pass
        
    def setup_secrets_management(self):
        """Setup proper secrets management"""
        # AWS Secrets Manager
        # Azure Key Vault
        # Environment variable encryption
        pass
```

### **8.2 Disaster Recovery**

#### **DR Implementation**
```python
# disaster_recovery/dr_implementation.py
class DisasterRecoveryManager:
    def __init__(self, config):
        self.config = config
        
    def setup_cross_region_replication(self):
        """Setup cross-region data replication"""
        # S3 cross-region replication
        # Azure geo-redundant storage
        # Database replication
        pass
        
    def implement_backup_strategies(self):
        """Implement comprehensive backup strategies"""
        # Automated backups
        # Point-in-time recovery
        # Backup validation
        pass
        
    def create_failover_procedures(self):
        """Create automated failover procedures"""
        # Health checks
        # Automatic failover
        # Data consistency validation
        pass
```

---

## **🎯 IMPLEMENTATION TIMELINE**

### **Week 1-2: Real Data Source Integration**
- [ ] Implement Snowflake connector
- [ ] Add Salesforce API integration
- [ ] Setup Stripe payment processing
- [ ] Configure Kafka streaming sources

### **Week 3-4: Cloud Infrastructure Deployment**
- [ ] Deploy AWS infrastructure with Terraform
- [ ] Deploy Azure infrastructure with ARM templates
- [ ] Setup CI/CD pipelines
- [ ] Configure monitoring and alerting

### **Week 5-6: Advanced Features**
- [ ] Implement ML pipeline with MLflow
- [ ] Add feature store capabilities
- [ ] Setup data lineage tracking
- [ ] Implement cost optimization

### **Week 7-8: Testing & Production Hardening**
- [ ] Comprehensive testing suite
- [ ] Performance optimization
- [ ] Security hardening
- [ ] Disaster recovery setup

---

## ** FINAL DELIVERABLES**

### **1. Complete Cloud Deployments**
- ✅ AWS production environment
- ✅ Azure production environment
- ✅ Infrastructure as Code
- ✅ Automated deployment pipelines

### **2. Real Data Source Integrations**
- ✅ Snowflake data warehouse
- ✅ Salesforce CRM API
- ✅ Stripe payment processing
- ✅ Kafka streaming sources
- ✅ Additional APIs (Google Analytics, etc.)

### **3. Advanced Monitoring & Observability**
- ✅ Prometheus + Grafana dashboards
- ✅ Data lineage tracking
- ✅ Performance monitoring
- ✅ Cost monitoring and optimization

### **4. Machine Learning & Analytics**
- ✅ MLflow integration
- ✅ Feature store
- ✅ Real-time predictions
- ✅ Advanced analytics capabilities

### **5. Production-Grade Features**
- ✅ Comprehensive testing
- ✅ Security hardening
- ✅ Disaster recovery
- ✅ Cost optimization
- ✅ Documentation and runbooks

---

This comprehensive plan will elevate your project to the top 10% of data engineer projects by adding real-world production features, advanced monitoring, machine learning capabilities, and enterprise-grade security and compliance features. The project will demonstrate not just technical skills but also production readiness, business value, and operational excellence.

Would you like me to start implementing any specific phase or component?