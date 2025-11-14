# AWS Project Code Review - Phase 4 Status

## 📋 Executive Summary

**Status**: ⚠️ **Partially On Track** - Infrastructure and code structure are solid, but runtime configuration needs fixes.

**Key Issues Identified**:
1. ✅ **Infrastructure**: Terraform setup is correct (EMR 7.1.0, IAM roles, S3 buckets, Glue DBs)
2. ✅ **Code Structure**: Job scripts properly structured with argparse support
3. ❌ **Delta Lake Configuration**: Missing proper JAR configuration for EMR Serverless
4. ❌ **Event Log Issue**: SparkSession builder tries to create event logs that already exist
5. ⚠️ **Resource Limits**: vCPU quota reached (16 concurrent vCPUs default limit)

---

## 🔍 Detailed Analysis

### 1. Infrastructure (✅ CORRECT)

**File**: `aws/terraform/main.tf`
- ✅ EMR Serverless application configured with `emr-7.1.0` (includes Delta Lake 2.4.0)
- ✅ Auto-start/stop configured correctly
- ✅ IAM roles have proper permissions (S3, Glue, Secrets Manager)
- ✅ S3 buckets configured with versioning (required for Delta Lake)

**File**: `aws/terraform/glue_catalog.tf`
- ✅ Glue databases created: `project-a_bronze_dev`, `project-a_silver_dev`, `project-a_gold_dev`

**File**: `config/dev.yaml`
- ✅ Environment set to `emr`
- ✅ All paths correctly configured
- ✅ EMR application ID and role ARN correct

---

### 2. Job Scripts (✅ MOSTLY CORRECT)

**File**: `jobs/transform/bronze_to_silver.py`
- ✅ Properly uses `argparse` for `--env` and `--config`
- ✅ Config loading from S3 using Spark's `textFile()` (avoids boto3 issues)
- ✅ Sets `environment: emr` in config
- ✅ Uses `build_spark(config)` correctly

**File**: `jobs/gold/star_schema.py`
- ✅ Properly uses `argparse` for `--env` and `--config`
- ✅ Reads from Silver Delta tables
- ✅ Writes to Gold Delta tables

**File**: `jobs/ingest/snowflake_to_bronze.py`
- ✅ Demo mode working correctly
- ✅ Writes to Bronze in Delta format
- ✅ Config loading from S3 working

---

### 3. SparkSession Builder (⚠️ NEEDS FIX)

**File**: `src/pyspark_interview_project/utils/spark_session.py`

**Issues**:
1. **Line 105**: Tries to disable event logging with `spark.eventLog.enabled=false`, but this config is not supported in EMR Serverless job submission
2. **Line 132**: Falls back to mock SparkSession on any exception, which masks real errors
3. **Line 67-83**: Delta Lake configuration logic is correct, but relies on EMR having Delta pre-installed

**Current Behavior**:
- Detects environment as `emr` correctly
- Tries to configure Delta extensions
- Fails when event log directory exists → falls back to mock session

**Required Fix**:
- Remove event log configuration from SparkSession builder (EMR Serverless handles this)
- Don't fall back to mock session in EMR environment - fail fast instead
- Ensure Delta extensions are properly configured

---

### 4. EMR Job Submission Scripts (❌ NEEDS FIX)

**File**: `aws/scripts/submit_silver_job.sh`

**Current Configuration**:
```bash
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**Issue**: 
- EMR 7.1.0 includes Delta Lake 2.4.0, but the JAR might not be on the classpath
- Error: `ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension`

**Solution Options**:
1. **Option A (Recommended)**: Add Delta packages explicitly via `--packages`
   ```bash
   --packages io.delta:delta-core_2.12:2.4.0
   ```

2. **Option B**: Configure Delta in `applicationConfiguration` at EMR app level (not per-job)

3. **Option C**: Use EMR's built-in Delta (should work, but may need classpath fix)

**Recommended Fix**: Use Option A - explicitly include Delta packages in `sparkSubmitParameters`

---

### 5. Resource Configuration (⚠️ NEEDS ATTENTION)

**Current Settings**:
- Executor instances: 2
- Executor cores: 2
- Executor memory: 4g
- Driver cores: 2
- Driver memory: 4g

**Total vCPU per job**: 2 (driver) + 2×2 (executors) = 6 vCPUs

**Issue**: 
- Default AWS account limit: 16 concurrent vCPUs
- Multiple jobs running simultaneously can hit this limit
- Error: `ServiceQuotaExceededException: Account vCPU limit reached`

**Solutions**:
1. **Immediate**: Reduce resources further (1 executor, 1 core each)
2. **Short-term**: Request quota increase via AWS Service Quotas console
3. **Long-term**: Implement job queuing or resource pooling

---

### 6. Event Log Directory Issue (❌ NEEDS FIX)

**Error**: `IOException: Target log directory already exists (file:/var/log/spark/apps/eventlog_v2_...)`

**Root Cause**: 
- EMR Serverless creates event log directories automatically
- SparkSession builder tries to create the same directory
- Conflict occurs

**Fix**:
1. Remove `spark.eventLog.enabled` from SparkSession builder (line 105)
2. EMR Serverless handles event logging automatically
3. Don't configure event logs in job submission scripts

---

## 🔧 Required Fixes

### Fix 1: Update `spark_session.py`
```python
# Remove event log configuration for EMR
# EMR Serverless handles this automatically
if spark_env in {"emr", "prod", "aws"}:
    # Don't try to configure event logs - EMR handles it
    pass  # Remove line 105
```

### Fix 2: Update `submit_silver_job.sh`
```bash
# Add Delta packages explicitly
--packages io.delta:delta-core_2.12:2.4.0 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

### Fix 3: Reduce Resource Requirements
```bash
# Reduce to 1 executor to avoid vCPU quota issues
--conf spark.executor.instances=1 \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=2g \
--conf spark.driver.cores=1 \
--conf spark.driver.memory=2g
```

### Fix 4: Update `submit_bronze_job.sh` (same fixes)

---

## ✅ What's Working

1. **Infrastructure**: All Terraform resources correctly configured
2. **S3 Uploads**: Jobs, configs, and wheels uploaded correctly
3. **Config Loading**: S3 config loading working (using Spark textFile)
4. **Job Structure**: All jobs have proper argparse and config handling
5. **Bronze Ingestion**: Successfully writing data to Bronze layer
6. **Demo Mode**: Working correctly for testing without live connections

---

## 🎯 Next Steps

1. **Immediate** (Fix runtime errors):
   - [ ] Fix Delta Lake JAR configuration in submit scripts
   - [ ] Remove event log configuration from SparkSession builder
   - [ ] Reduce resource requirements to avoid vCPU quota

2. **Short-term** (Improve reliability):
   - [ ] Request AWS vCPU quota increase
   - [ ] Add retry logic for quota errors
   - [ ] Implement job queuing for resource management

3. **Long-term** (Optimize):
   - [ ] Add monitoring and alerting
   - [ ] Implement data quality gates
   - [ ] Add automated testing for EMR jobs

---

## 📊 Code Quality Assessment

| Component | Status | Notes |
|-----------|--------|-------|
| Terraform Infrastructure | ✅ Excellent | Well-structured, follows best practices |
| Job Scripts | ✅ Good | Proper error handling, argparse support |
| SparkSession Builder | ⚠️ Needs Fix | Event log issue, mock fallback too aggressive |
| EMR Submission Scripts | ⚠️ Needs Fix | Missing Delta packages, resource config |
| Config Management | ✅ Good | S3 loading working correctly |
| Error Handling | ⚠️ Needs Improvement | Mock fallback masks real errors |

---

## 🚀 Conclusion

**Overall Assessment**: The project is **on the right track** with solid infrastructure and code structure. The main issues are runtime configuration problems that can be fixed quickly:

1. Add Delta packages to job submission
2. Remove event log configuration
3. Reduce resource requirements

Once these fixes are applied, Phase 4 (Silver/Gold transformations) should work correctly.

**Estimated Time to Fix**: 1-2 hours

**Risk Level**: Low - All issues are configuration-related, not architectural

