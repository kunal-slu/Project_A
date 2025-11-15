# Cursor AI Task Prompts

Ready-to-paste prompts for Cursor AI to act as your code reviewer and fixer.

## 🧩 Project-Level Setup

**First, add this to Cursor's Workspace Settings / Project Instructions:**

See `.cursorrules` file in the repo root.

---

## 🛠️ Task Prompt 1 — Static & Test Baseline

**Paste this into Cursor chat:**

```
Task 1 – Baseline quality & tests

Scan the repo and:

1. Run `ruff check src jobs aws/dags` and summarize the main issues.
2. Run `mypy src/pyspark_interview_project` and list the top type problems.
3. Run `pytest -q` and show failing tests (if any).

Based on the results:

- Fix the most important issues first: imports, obvious bugs, missing type hints in core utilities.
- Update code and tests so that `pytest -q` passes.

Show me:
- The final code for each file you modified.
- The exact commands you ran and their final outputs.
```

---

## 🛠️ Task Prompt 2 — Join & Transformation Tests

**Paste this into Cursor chat:**

```
Task 2 – Validate joins & transformations

I want strong coverage for the main joins and transforms:
- customers ↔ orders ↔ products
- CRM accounts/contacts/opportunities
- Redshift behavior → customer enrichment

Please:

1. Find the main transform functions for:
   - building silver tables from Bronze
   - building gold tables (customer_360, product performance, etc.)

2. Add integration tests under `tests/integration/` that:
   - load small sample data from `data/` (customers, orders, products, crm, behavior, fx, kafka)
   - run the transform functions directly (without EMR)
   - assert:
     - no nulls in primary keys in Silver/Gold where they shouldn't exist
     - row counts make sense vs. source
     - joins actually match keys (e.g., no large share of unmatched keys)

3. Run `pytest -q tests/integration` and fix issues until it passes.

Show me the new tests and any changes you made to the transform code.
```

---

## 🛠️ Task Prompt 3 — DQ & Contracts

**Paste this into Cursor chat:**

```
Task 3 – Data contracts + DQ

I want contract + DQ coverage for all 5 sources:
- Snowflake: customers, orders, products
- Redshift: customer_behavior
- CRM: accounts, contacts, opportunities
- FX: fx_rates
- Kafka: events JSON

Please:

1. Ensure we have schema definitions under `config/schema_definitions/bronze/` for each of these.

2. Implement or refine a generic contract validator (probably already in `utils/contracts.py`) to:
   - check column presence, types, nullability, and simple ranges

3. Add a test `tests/test_contracts_all_sources.py` that:
   - loads a small sample for each source from `data/`
   - validates it against its contract

4. Add at least one test that feeds intentionally bad data and expects validation to fail.

5. Run `pytest -q tests/test_contracts_all_sources.py` and fix until green.
```

---

## 🛠️ Task Prompt 4 — Airflow & EMR Orchestration Sanity

**Paste this into Cursor chat:**

```
Task 4 – Airflow DAG & EMR job sanity

I want to be sure Airflow DAGs and EMR job wiring are correct.

Please:

1. Inspect `aws/dags/` and:
   - Make sure each DAG file can be imported without errors
   - DAG IDs are unique and descriptive
   - EMR job operators use the right application_id, execution_role_arn, and config/dev.yaml location (use placeholders or Airflow Variables if needed)

2. Add a simple test `tests/test_dags_import.py` that:
   - imports each DAG
   - asserts that dag.dag_id is set and there is at least one task

3. Show example airflow variables we should set to run this in MWAA or local Airflow.

4. If needed, refactor the DAGs slightly to reduce duplication and make EMR job submission consistent.

Show me the test file and any DAG changes you made.
```

---

## 🛠️ Task Prompt 5 — Polish & Documentation

**Paste this into Cursor chat:**

```
Task 5 – Final polish & documentation

Please:

1. Add or improve docstrings on the most important functions:
   - Spark session factory
   - IO utilities (read/write from S3, Snowflake, Redshift, Kafka)
   - Bronze→Silver and Silver→Gold transform entrypoints
   - DQ gate logic

2. Update or create a `docs/VERIFICATION_COMPLETE.md` that:
   - lists all tests we run (pytest, integration, contracts, DAG import)
   - documents how to run a full end-to-end test (Bronze→Silver→Gold on dev)
   - mentions how we validate joins and DQ

3. Ensure Makefile (or add one) has helpful targets:
   - `make lint` → ruff + mypy
   - `make test` → pytest
   - `make package` → build wheel
   - `make deploy-dev` → terraform apply

Show me the final VERIFICATION_COMPLETE.md and updated Makefile.
```

---

## 🚀 Quick Usage

1. **Add `.cursorrules` to your repo** (already done)
2. **Open Cursor AI chat**
3. **Paste Task Prompt 1** and let Cursor fix issues
4. **Review changes**, then move to Task Prompt 2
5. **Repeat for all 5 tasks**

---

## 📝 Expected Outcomes

After running all 5 tasks, you should have:

- ✅ Clean, typed, tested PySpark code
- ✅ Integration tests for all major transforms
- ✅ Contract validation for all sources
- ✅ Validated Airflow DAGs
- ✅ Complete documentation and Makefile

---

**Status:** Ready to use

