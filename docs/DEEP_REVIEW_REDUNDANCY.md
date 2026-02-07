# Project_A – Deep Review: Redundancy, Merge & Delete

This document summarizes redundancy found across the codebase and recommended consolidations. **Canonical entry point for the ETL pipeline is `jobs/run_pipeline.py`** (uses `project_a.core.config.ProjectConfig` and repo-root `jobs/transform/`, `jobs/ingest/`).

---

## 1. Config loading (redundancy)

| Location | Functions | Used by |
|----------|-----------|--------|
| `project_a.utils.config` | `load_config`, `load_config_resolved` | Many jobs, utils, DQ, local scripts |
| `project_a.config_loader` | `ConfigLoader`, `load_config_resolved` | DQ, legacy ingestion_pipeline, lineage (fixed to use utils.config) |
| `project_a.pyspark_interview_project.utils.config_loader` | `load_config_resolved`, `load_config_from_s3` | aws/jobs, tools/validate_*.py, local/scripts/quick_check.py |
| `project_a.legacy.config_loader` | `load_config_resolved` | Legacy only |
| `project_a.pyspark_interview_project.utils.config` | `load_conf` | Legacy jobs, aws/jobs (ingest) |

**Recommendation**

- **Canonical:** `project_a.utils.config` – single place for `load_config` and `load_config_resolved`. Already supports S3 and local; `"local"` env → `local/config/local.yaml` was added.
- **Keep:** `project_a.pyspark_interview_project.utils.config_loader` for callers that rely on it; consider making it a thin re-export of `project_a.utils.config` plus optional Spark-based S3 fallback.
- **Legacy:** `project_a.config_loader` has Databricks-specific validation (unity_catalog, azure_security). Keep for Databricks; other code should use `project_a.utils.config`.
- **Delete when cleaning legacy:** `project_a.legacy.config_loader` (only used inside legacy).

---

## 2. Legacy code (safe to archive / delete after validation)

- **`src/project_a/legacy/`** – No imports from outside legacy (`grep` found no `from project_a.legacy` in non-legacy code). Used only by legacy scripts and tests. Contains duplicate concepts:
  - `legacy/lineage_tracker.py` vs `project_a/lineage/tracking.py`
  - `legacy/delta_utils.py` vs `project_a/delta_utils.py`
  - `legacy/config_loader.py` vs `project_a/config_loader.py` / `utils/config.py`
  - `legacy/pipeline/pipeline/run_pipeline.py` – duplicate entry point
  - Legacy jobs (snowflake_to_bronze, fx_to_bronze, etc.) duplicated by `jobs/ingest/` and `jobs/transform/`
- **`jobs_legacy/`** – Old transform scripts; only reference `project_a.pyspark_interview_project` and `project_a.config_loader`. Not used by `jobs/run_pipeline.py`.

**Recommendation**

- Add a short **DEPRECATED** notice in `src/project_a/legacy/README.md` (see below).
- Plan to archive or remove `legacy/` and `jobs_legacy/` after confirming no external/CI usage.

---

## 3. Lineage (two implementations)

| Location | Purpose |
|----------|--------|
| `project_a.lineage.tracking` | `LineageTracker`, `LineageEvent`, file-based storage, used by pipeline |
| `project_a.legacy.lineage_tracker` | Different API (`start_event`/`complete_event`), in-memory + export |

**Recommendation**

- Use only **`project_a.lineage.tracking`** for new code and pipeline.
- **Lineage/tracking.py** was updated to use `project_a.utils.config.load_config_resolved` instead of `config_loader`.
- Treat **`project_a.legacy.lineage_tracker`** as deprecated with legacy.

---

## 4. Delta utilities (two modules)

| Location | Contents | Used by |
|----------|----------|--------|
| `project_a.delta_utils` | `spark_session`, `write_staging`, `merge_publish` | `project_a.jobs.contacts_silver` |
| `project_a.legacy.delta_utils` | `read_delta`, `write_delta`, `DeltaUtils` class | `legacy/ingestion_pipeline`, `legacy/enterprise_data_platform` |

**Recommendation**

- Keep **`project_a.delta_utils`** as the main API.
- Do not merge legacy’s `DeltaUtils` into core unless you need those features; treat as legacy and deprecate with the rest of `legacy/`.

---

## 5. Pipeline entry points (two runners)

| Entry point | Uses |
|-------------|------|
| **`jobs/run_pipeline.py`** | `project_a.core.ProjectConfig`, `jobs/transform/*`, `jobs/ingest/*`. **This is the one that works.** |
| **`project_a.pipeline.run_pipeline`** | `project_a.jobs` (bronze_to_silver, silver_to_gold, etc.). **`project_a.jobs` only defines 4 modules** (fx_json_to_bronze, publish_gold_to_snowflake, contacts_silver, __init__); the rest (bronze_to_silver, silver_to_gold, snowflake_to_bronze, …) are not under `src/project_a/jobs/`, so this entry point will fail on import. |

**Recommendation**

- **Use and document** `jobs/run_pipeline.py` as the single supported CLI entry point.
- Fix or remove **`project_a.pipeline.run_pipeline`**: either (a) make it delegate to `jobs.run_pipeline.main()`, or (b) have `project_a.jobs` re-export from the repo-root `jobs/` (e.g. by adding `jobs` to `sys.path` or packaging root `jobs`), or (c) delete it and point all docs/scripts to `jobs/run_pipeline.py`.

---

## 6. Publish jobs (Snowflake – two scripts)

| File | Role |
|------|------|
| **`jobs/publish/gold_to_snowflake.py`** | Full implementation (Secrets Manager, `build_spark`, detailed options). |
| **`jobs/publish/publish_gold_to_snowflake.py`** | Simpler (load_config, get_spark, read/write). |

**Recommendation**

- Prefer **`gold_to_snowflake.py`** as the implementation.
- Make **`publish_gold_to_snowflake.py`** a thin wrapper that parses args and calls `gold_to_snowflake.main()` so one code path and one place to maintain.

---

## 7. Ingest jobs (overlap)

- **`jobs/ingest/ingest_snowflake_to_s3.py`** and **`ingest_crm_to_s3.py`** – used by `local/tests/local/run_etl_end_to_end.py` as optional ingest steps.
- **`jobs/ingest/snowflake_to_bronze.py`** and **`crm_to_bronze.py`** – used by `jobs/run_pipeline.py` (BaseJob-based).

**Recommendation**

- Keep both for now: different interfaces (function vs class) and different callers. Optionally later refactor ETL test to call the same job entry points as `jobs/run_pipeline.py` to reduce duplication.

---

## 8. Duplicate / redundant scripts

- **`scripts/show_pipeline_results.py`** – Keep; writes results to file for viewing without terminal.
- **`test_pipeline.py`** (root) – Runs full pipeline with `config/dev.yaml` (S3 paths); fails locally without S3. Consider renaming to `run_full_pipeline_dev.py` and documenting, or gate behind env check.
- **`local/scripts/run_local_etl_safe.py`** vs **`local/scripts/run_local_etl.sh`** – Both run Bronze→Silver→Gold; safe script adds checks. Prefer the safe script; document one “run local pipeline” command.

---

## 9. Applied changes (this review)

1. **`project_a.utils.config`** – Added `"local"` → `local/config/local.yaml` in `load_config_resolved` env candidates.
2. **`project_a.lineage.tracking`** – `get_lineage_tracker()` now uses `project_a.utils.config.load_config_resolved(None, "local")` instead of `config_loader`.
3. **`src/project_a/legacy/README.md`** – Added (see below) to mark legacy as deprecated.

---

## 10. Suggested next steps (optional)

1. **Config:** Implement re-export in `project_a.pyspark_interview_project.utils.config_loader` from `project_a.utils.config` and keep only Spark S3 fallback there if needed.
2. **Pipeline entry:** Fix or remove `project_a.pipeline.run_pipeline` and standardize on `jobs/run_pipeline.py`.
3. **Publish:** Merge logic into `gold_to_snowflake.py` and make `publish_gold_to_snowflake.py` a thin wrapper.
4. **Legacy:** After confirming no external use, move `src/project_a/legacy/` and `jobs_legacy/` to an `archive/` or remove them.
5. **Tests:** Run test suite after any further merges; fix tests that still depend on legacy or duplicate config/lineage.
