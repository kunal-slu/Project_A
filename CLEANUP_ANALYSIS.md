# File Cleanup Analysis & Recommendations

## 🔍 Analysis Results

### 1. Duplicate Data Files Found
**Issue**: Same data files exist in both root and subdirectories
- `aws/data_fixed/hubspot_contacts_25000.csv` (9.6MB) vs `aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv` (5.2MB)
- `aws/data_fixed/hubspot_deals_30000.csv` (12.2MB) vs `aws/data_fixed/01_hubspot_crm/hubspot_deals_30000.csv` (3.4MB)

**Recommendation**: Keep root directory files (enhanced versions) and delete subdirectory duplicates

### 2. Temporary Files Found
**Files to Delete**:
- `__pycache__/` directories (multiple locations)
- `*.pyc` files
- `.DS_Store` files (multiple locations)
- `.pytest_cache/` directory

### 3. Duplicate Documentation Files
**Similar Files Found**:
- `README.md` (main)
- `aws/README_AWS.md` (AWS-specific)
- `DATA_IMPROVEMENTS_SUMMARY.md`
- `DATA_SOURCES_ANALYSIS.md`
- `ERROR_FIXES_SUMMARY.md`
- `ETL_RUNNER_SUMMARY.md`
- `FOLDER_ORGANIZATION_SUMMARY.md`
- `PRODUCTION_READINESS_SUMMARY.md`

**Recommendation**: Merge similar documentation files

### 4. Multiple Pipeline Files
**Similar Files Found**:
- `run_complete_etl.py`
- `run_simple_etl.py`
- `run_step_by_step_etl.py`
- `simple_pipeline.py`
- `delta_pipeline.py`
- `src/pyspark_interview_project/pipeline.py` (main)
- `src/pyspark_interview_project/pipeline_core.py`

**Recommendation**: Keep main pipeline, archive or merge others

## 🧹 Cleanup Recommendations

### Phase 1: Safe Deletions (Immediate)
1. **Delete temporary files**:
   - All `__pycache__/` directories
   - All `*.pyc` files
   - All `.DS_Store` files
   - `.pytest_cache/` directory

2. **Delete duplicate data files**:
   - Remove subdirectory duplicates (keep root enhanced versions)
   - Keep only the enhanced versions in root directory

### Phase 2: Documentation Consolidation
1. **Merge documentation files**:
   - Combine `DATA_IMPROVEMENTS_SUMMARY.md` and `DATA_SOURCES_ANALYSIS.md` into `DATA_ANALYSIS.md`
   - Merge `ERROR_FIXES_SUMMARY.md` into main `README.md`
   - Consolidate AWS documentation

### Phase 3: Pipeline Consolidation
1. **Keep main pipeline**: `src/pyspark_interview_project/pipeline.py`
2. **Archive old pipelines**: Move to `archive/` directory
3. **Merge similar functionality**: Combine related pipeline files

## 📊 Space Savings Estimate

### Files to Delete
- **Temporary files**: ~50MB
- **Duplicate data files**: ~15MB
- **Cache files**: ~10MB
- **Total estimated savings**: ~75MB

### Files to Merge
- **Documentation**: 7 files → 3 files
- **Pipeline files**: 7 files → 3 files
- **Total files to consolidate**: 14 files → 6 files

## ⚠️ Safety Considerations

### Before Deletion
1. **Backup**: Create backup of all files
2. **Verify**: Check file contents before deletion
3. **Test**: Ensure no dependencies on files to be deleted

### Merge Considerations
1. **Content overlap**: Check for duplicate content
2. **Dependencies**: Ensure no broken imports
3. **Functionality**: Preserve all unique functionality

## 🎯 Implementation Plan

### Step 1: Create Archive Directory
```bash
mkdir -p archive/{documentation,pipelines,data}
```

### Step 2: Delete Temporary Files
```bash
find . -name "__pycache__" -type d -exec rm -rf {} +
find . -name "*.pyc" -delete
find . -name ".DS_Store" -delete
rm -rf .pytest_cache
```

### Step 3: Remove Duplicate Data Files
```bash
rm -rf aws/data_fixed/01_hubspot_crm/
rm -rf aws/data_fixed/02_snowflake_warehouse/
rm -rf aws/data_fixed/03_redshift_analytics/
rm -rf aws/data_fixed/04_stream_data/
rm -rf aws/data_fixed/05_fx_rates/
```

### Step 4: Consolidate Documentation
- Merge analysis files into comprehensive documentation
- Update main README with consolidated information

### Step 5: Consolidate Pipeline Files
- Keep main pipeline in src/
- Archive old pipeline files
- Update imports and references

## ✅ Expected Results

After cleanup:
- **Cleaner project structure**
- **Reduced file count by ~30%**
- **Easier maintenance**
- **Better organization**
- **Faster build times**
- **Reduced confusion**

## 🚀 Next Steps

1. **Review recommendations**
2. **Approve cleanup plan**
3. **Execute cleanup in phases**
4. **Verify functionality**
5. **Update documentation**
