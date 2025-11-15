# Code Hardening & Quality Checklist

Complete guide for hardening Spark, Airflow, Terraform, and all project code.

## 📋 Table of Contents

1. [Static Quality & Style](#static-quality--style)
2. [Runtime / Integration Tests](#runtime--integration-tests)
3. [Data Contracts & DQ](#data-contracts--dq)
4. [Performance / Correctness Guardrails](#performance--correctness-guardrails)
5. [Infra & Deployment Checks](#infra--deployment-checks)

---

## 🔍 A. Static Quality & Style

### Python / PySpark

#### Required Checks

```bash
# 1. Linting with ruff
ruff check src jobs aws/dags
ruff format src jobs aws/dags

# 2. Type checking with mypy
mypy src/pyspark_interview_project

# 3. Import validation
python -m py_compile src/**/*.py jobs/**/*.py
```

#### What to Enforce

**Type Hints:**
- ✅ All public functions in:
  - `src/pyspark_interview_project/utils/*`
  - `src/pyspark_interview_project/transform/*`
  - `src/pyspark_interview_project/dq/*`
  - `src/project_a/jobs/*`

**Code Quality:**
- ✅ No unused imports
- ✅ No dead code
- ✅ No bare `except:` blocks (log + rethrow or handle explicitly)
- ✅ Every job in `jobs/` has a `main()` or `if __name__ == "__main__":` entry
- ✅ Use `logging`, not `print`, in production code

**Example Fixes:**

```python
# ❌ Bad
def process_data(df):
    return df.filter("amount > 0")

# ✅ Good
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspark.sql import DataFrame

def process_data(df: "DataFrame") -> "DataFrame":
    """Filter rows with positive amounts."""
    return df.filter("amount > 0")
```

```python
# ❌ Bad
except:
    pass

# ✅ Good
except Exception as e:
    logger.warning(f"Failed to process: {e}", exc_info=True)
    raise  # or handle appropriately
```

### Airflow DAGs

#### Required Checks

```bash
# 1. Basic import test
python -m compileall aws/dags

# 2. DAG validation test
pytest -q tests/test_dags_import.py
```

#### What to Enforce

- ✅ Each DAG file can be imported without errors
- ✅ DAG IDs are unique and descriptive
- ✅ EMR job operators use correct:
  - `application_id` (from Airflow Variables)
  - `execution_role_arn` (from Airflow Variables)
  - `config/dev.yaml` location (S3 URI)
- ✅ No hardcoded credentials or paths
- ✅ Use Airflow Variables for configuration

**Example Test:**

```python
# tests/test_dags_import.py
import pytest
from airflow.models import DagBag

def test_dag_imports():
    """Test that all DAGs can be imported."""
    dagbag = DagBag(dag_folder="aws/dags", include_examples=False)
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
    
    for dag_id, dag in dagbag.dags.items():
        assert dag.dag_id, f"DAG {dag_id} missing dag_id"
        assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"
```

### Terraform

#### Required Checks

```bash
# 1. Formatting
terraform fmt -recursive aws/terraform

# 2. Validation
cd aws/terraform
terraform init
terraform validate

# 3. Optional: Linting
tflint aws/terraform
```

#### What to Enforce

- ✅ All `.tf` files formatted consistently
- ✅ No `terraform validate` errors
- ✅ Variables have descriptions
- ✅ Resources have tags
- ✅ No hardcoded values (use variables)
- ✅ State file is managed (S3 backend recommended)

---

## 🧪 B. Runtime / Integration Tests

### Spark Integration Tests

#### Required Tests

**Test File:** `tests/integration/test_customer_360_pipeline.py`

**What to Test:**
- ✅ Read real sample data from `data/`
- ✅ Call transform functions
- ✅ Assert:
  - Row counts & duplicates
  - Join keys coverage (customer_id, account_id, etc.)
  - Important metrics (sum(order_amount), number of orders per customer)

**Example Test Structure:**

```python
# tests/integration/test_customer_360_pipeline.py
import pytest
from pyspark.sql import SparkSession
from jobs.transform.bronze_to_silver import bronze_to_silver_complete
from jobs.gold.silver_to_gold import silver_to_gold_complete

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()

def test_bronze_to_silver_joins(spark):
    """Test that bronze→silver joins work correctly."""
    # Load sample data
    customers = spark.read.csv("data/samples/snowflake/snowflake_customers_50000.csv", header=True)
    orders = spark.read.csv("data/samples/snowflake/snowflake_orders_100000.csv", header=True)
    
    # Run transform
    results = bronze_to_silver_complete(spark, config, run_date="2025-01-01")
    
    # Assertions
    assert results['customers'].count() > 0
    assert results['orders'].count() > 0
    
    # Check no nulls in primary keys
    null_customer_ids = results['customers'].filter(col("customer_id").isNull()).count()
    assert null_customer_ids == 0, "Found null customer_id in silver.customers"
    
    # Check join coverage
    joined = results['orders'].join(
        results['customers'],
        on="customer_id",
        how="inner"
    )
    assert joined.count() == results['orders'].count(), "Orders lost in join"

def test_silver_to_gold_aggregations(spark):
    """Test that silver→gold aggregations are correct."""
    # Load silver data
    silver_customers = spark.read.parquet("data/silver/customers/")
    silver_orders = spark.read.parquet("data/silver/orders/")
    
    # Run transform
    results = silver_to_gold_complete(spark, config, run_date="2025-01-01")
    
    # Assertions
    customer_360 = results['customer_360']
    assert customer_360.count() > 0
    
    # Check aggregates match source
    total_orders_gold = customer_360.agg(sum("total_orders")).collect()[0][0]
    total_orders_silver = silver_orders.count()
    assert total_orders_gold == total_orders_silver, "Order count mismatch"
```

### Airflow / EMR "Smoke Test" DAG

**Test File:** `tests/integration/test_emr_smoke.py`

**What to Test:**
- ✅ DAG can be triggered
- ✅ EMR job submission works
- ✅ Job completes successfully
- ✅ Output files exist in S3

**Example Test:**

```python
# tests/integration/test_emr_smoke.py
import pytest
import boto3
from airflow.models import DagBag, DagRun
from airflow.utils.state import State

def test_emr_job_submission():
    """Test that EMR job can be submitted via Airflow."""
    dagbag = DagBag(dag_folder="aws/dags")
    dag = dagbag.get_dag("project_a_daily_pipeline")
    
    # Create a test run
    dagrun = dag.create_dagrun(
        run_id="test_run",
        state=State.RUNNING,
        execution_date=datetime.now()
    )
    
    # Trigger the first task
    task = dag.get_task("extract_fx_json_to_bronze")
    task.run(start_date=dagrun.execution_date, end_date=dagrun.execution_date)
    
    # Verify job was submitted
    emr_client = boto3.client("emr-serverless")
    jobs = emr_client.list_job_runs(applicationId="...")
    assert len(jobs['jobRuns']) > 0
```

---

## 📊 C. Data Contracts & DQ

### Contract Validation

#### Required Tests

**Test File:** `tests/test_contracts_all_sources.py`

**What to Test:**
- ✅ All 5 sources have schema definitions
- ✅ Sample data validates against contracts
- ✅ Bad data is rejected

**Example Test:**

```python
# tests/test_contracts_all_sources.py
import pytest
from pyspark.sql import SparkSession
from pyspark_interview_project.utils.contracts import validate_df_against_contract

SOURCES = [
    ("bronze/crm/accounts", "config/schema_definitions/bronze/crm_accounts.json"),
    ("bronze/snowflake/customers", "config/schema_definitions/bronze/snowflake_customers.json"),
    ("bronze/snowflake/orders", "config/schema_definitions/bronze/snowflake_orders.json"),
    ("bronze/redshift/behavior", "config/schema_definitions/bronze/redshift_behavior.json"),
    ("bronze/fx/rates", "config/schema_definitions/bronze/fx_rates.json"),
    ("bronze/kafka/events", "config/schema_definitions/bronze/kafka_events.json"),
]

@pytest.mark.parametrize("source_path,contract_path", SOURCES)
def test_source_contract(spark, source_path, contract_path):
    """Test that each source validates against its contract."""
    df = spark.read.format("delta").load(f"data/{source_path}")
    errors = validate_df_against_contract(df, contract_path)
    assert len(errors) == 0, f"Contract violations: {errors}"

def test_bad_data_rejected(spark):
    """Test that intentionally bad data is rejected."""
    bad_data = spark.createDataFrame([
        {"customer_id": None, "order_id": "123", "amount": -100}
    ])
    
    errors = validate_df_against_contract(bad_data, "config/schema_definitions/bronze/snowflake_orders.json")
    assert len(errors) > 0, "Bad data should be rejected"
```

### DQ Gate Tests

**Test File:** `tests/test_dq_gates.py`

**What to Test:**
- ✅ DQ gate passes "good" data
- ✅ DQ gate blocks "bad" data
- ✅ Bad data is written to error lane/quarantine
- ✅ Run is marked as failed

**Example Test:**

```python
# tests/test_dq_gates.py
def test_dq_gate_passes_good_data(spark):
    """Test that DQ gate passes valid data."""
    good_data = spark.createDataFrame([
        {"customer_id": "123", "order_id": "456", "amount": 100.0, "order_date": "2025-01-01"}
    ])
    
    result = run_dq_checks(good_data, "silver.orders")
    assert result.status == "PASS"

def test_dq_gate_quarantines_bad_data(spark):
    """Test that DQ gate quarantines invalid data."""
    bad_data = spark.createDataFrame([
        {"customer_id": None, "order_id": "456", "amount": -100.0}
    ])
    
    result = run_dq_checks(bad_data, "silver.orders")
    assert result.status == "FAIL"
    assert result.quarantined_rows.count() > 0
```

---

## ⚡ D. Performance / Correctness Guardrails

### Join Analysis

**Test File:** `tests/test_join_plans.py`

**What to Test:**
- ✅ No cross joins (CartesianProduct)
- ✅ Joins on keyed columns, not entire row
- ✅ Broadcast joins for small tables

**Example Test:**

```python
# tests/test_join_plans.py
def test_no_cross_joins(spark):
    """Test that joins don't produce Cartesian products."""
    customers = spark.read.parquet("data/silver/customers/")
    orders = spark.read.parquet("data/silver/orders/")
    
    joined = customers.join(orders, on="customer_id", how="inner")
    plan = joined.explain(True)
    
    assert "CartesianProduct" not in plan, "Found Cartesian product in join plan"

def test_join_key_coverage(spark):
    """Test that join keys have good coverage."""
    customers = spark.read.parquet("data/silver/customers/")
    orders = spark.read.parquet("data/silver/orders/")
    
    # Check unmatched orders
    unmatched = orders.join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
    unmatched_pct = unmatched.count() / orders.count()
    assert unmatched_pct < 0.01, f"Too many unmatched orders: {unmatched_pct:.2%}"
```

### Aggregation Tests

**Test File:** `tests/test_aggregations.py`

**What to Test:**
- ✅ Grouping keys are correct
- ✅ Floating point tolerance
- ✅ Edge cases (no rows, all nulls, zero amounts)

**Example Test:**

```python
# tests/test_aggregations.py
def test_aggregation_correctness(spark):
    """Test that aggregations produce correct results."""
    orders = spark.read.parquet("data/silver/orders/")
    
    # Manual calculation
    manual_sum = orders.agg(sum("amount")).collect()[0][0]
    
    # Aggregated calculation
    customer_totals = orders.groupBy("customer_id").agg(sum("amount").alias("total"))
    aggregated_sum = customer_totals.agg(sum("total")).collect()[0][0]
    
    assert abs(manual_sum - aggregated_sum) < 0.01, "Aggregation mismatch"

def test_edge_cases(spark):
    """Test edge cases in aggregations."""
    # Empty dataframe
    empty = spark.createDataFrame([], schema="customer_id string, amount double")
    result = empty.groupBy("customer_id").agg(sum("amount"))
    assert result.count() == 0
    
    # All nulls
    nulls = spark.createDataFrame([
        {"customer_id": "123", "amount": None},
        {"customer_id": "123", "amount": None}
    ])
    result = nulls.groupBy("customer_id").agg(sum("amount"))
    assert result.collect()[0][1] is None
```

---

## 🏗️ E. Infra & Deployment Checks

### Terraform Plan Diff Test

**Test File:** `tests/test_terraform_plan.sh`

**What to Test:**
- ✅ Terraform plan is clean (no drift)
- ✅ No unexpected changes

**Example Script:**

```bash
#!/bin/bash
# tests/test_terraform_plan.sh
set -e

cd aws/terraform
terraform init
terraform plan -var-file=env/dev.tfvars -out=tfplan

# Check for unexpected changes
if terraform show tfplan | grep -q "forces replacement"; then
    echo "❌ Terraform plan shows resource replacements"
    exit 1
fi

echo "✅ Terraform plan is clean"
```

### Shell Script Validation

**Required Checks:**

```bash
# Run shellcheck on all shell scripts
find . -name "*.sh" -exec shellcheck {} \;
```

**What to Enforce:**
- ✅ No unsafe bash patterns
- ✅ Proper error handling (`set -e`, `set -u`)
- ✅ Quoted variables

### Makefile Targets

**File:** `Makefile`

**Required Targets:**

```makefile
.PHONY: lint test package deploy-dev

lint:
	ruff check src jobs aws/dags
	ruff format --check src jobs aws/dags
	mypy src/pyspark_interview_project
	terraform fmt -check -recursive aws/terraform
	terraform validate -chdir=aws/terraform

test:
	pytest -q tests/

test-integration:
	pytest -q tests/integration/

test-contracts:
	pytest -q tests/test_contracts_all_sources.py

test-dags:
	pytest -q tests/test_dags_import.py

package:
	python -m build
	@echo "Wheel built: dist/project_a-*.whl"

deploy-dev:
	cd aws/terraform && terraform apply -var-file=env/dev.tfvars

all: lint test package
```

---

## ✅ Complete Checklist

### Static Quality
- [ ] `ruff check` passes
- [ ] `ruff format` applied
- [ ] `mypy` passes (or errors documented)
- [ ] All public functions have type hints
- [ ] No bare `except:` blocks
- [ ] All jobs have `main()` entry point

### Airflow
- [ ] All DAGs import without errors
- [ ] DAG IDs are unique
- [ ] EMR operators use Airflow Variables
- [ ] `test_dags_import.py` passes

### Terraform
- [ ] `terraform fmt` applied
- [ ] `terraform validate` passes
- [ ] All variables have descriptions
- [ ] Resources have tags

### Integration Tests
- [ ] `test_customer_360_pipeline.py` passes
- [ ] `test_emr_smoke.py` passes
- [ ] Row counts validated
- [ ] Join coverage validated

### Contracts & DQ
- [ ] All 5 sources have schema definitions
- [ ] `test_contracts_all_sources.py` passes
- [ ] Bad data is rejected
- [ ] DQ gates work correctly

### Performance
- [ ] No cross joins in plans
- [ ] Join key coverage > 99%
- [ ] Aggregations tested for edge cases

### Infrastructure
- [ ] Terraform plan is clean
- [ ] Shell scripts pass `shellcheck`
- [ ] Makefile has all targets

---

## 🚀 Quick Start

```bash
# Run all checks
make lint
make test
make test-integration
make test-contracts
make test-dags

# Or individually
ruff check src jobs aws/dags
mypy src/pyspark_interview_project
pytest -q tests/
terraform validate -chdir=aws/terraform
```

---

**Status:** Ready for implementation

