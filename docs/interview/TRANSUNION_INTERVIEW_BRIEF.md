# TransUnion Interview Brief — Project_A (Canonical Story)

Use this doc as the **single source of truth** for interviews. It keeps the stack consistent and avoids tool confusion.

## 1. One‑Sentence Pitch

“Project_A is a Spark + Iceberg lakehouse pipeline that ingests multi‑source data into Bronze, standardizes it in Silver with contracts and DQ gates, and builds Gold analytics models with reconciliation tests and lineage — designed for regulated, privacy‑sensitive environments.”

## 2. Canonical Stack (Say This)

- **Compute**: Spark (PySpark)
- **Storage**: **Iceberg** for Silver + Gold (ACID, time travel, schema evolution)
- **Orchestration**: Airflow (Docker locally)
- **Governance**: Data contracts + DQ gates + Gold Truth Tests
- **dbt**: Optional (SQL governance/tests if enabled)

**Do not lead with** Delta/EMR/Step Functions unless asked.

## 3. TransUnion‑Style Domain Framing

This project uses **synthetic data**, but the flow maps cleanly to credit‑bureau style workloads:

- **CRM** → client onboarding / account metadata  
- **Snowflake (orders/customers/products)** → core bureau‑style entities (accounts, consumers, products)  
- **Redshift behavior** → usage analytics / engagement signals  
- **Kafka events** → near‑real‑time events (inquiries / application activity)  
- **FX rates** → multi‑currency normalization (global portfolio)

You can say: *“The data is synthetic and privacy‑safe, but the pipeline mirrors regulated credit data workflows.”*

## 4. Governance + Compliance (Signal Maturity)

- **PII safety**: synthetic IDs only; no real PII stored
- **Access control**: storage paths and configs are environment‑scoped
- **Auditability**: run audits + lineage + DQ results persisted
- **Fail‑fast**: contracts block null PKs, duplicates, and RI breaks

## 5. What To Emphasize

- **Bronze** is immutable raw truth
- **Silver** enforces schema/contracts and dedupes + CDC handling
- **Gold** builds business‑ready facts/dims with reconciliation tests
- **Iceberg** is the primary table format (ACID + time travel)

## 6. If Asked About Delta / EMR

Short answer:

“Delta and EMR are optional/legacy paths. The canonical build is Spark + Iceberg with Airflow orchestration; the same jobs can run locally or on a managed Spark runtime.”

## 7. 30‑Second Interview Summary

“We ingest multi‑source data into Bronze, standardize it in Silver with schema contracts and CDC handling, and produce Gold dims/facts with reconciliation checks. Iceberg gives ACID and time travel, Airflow orchestrates the jobs, and DQ gates ensure we fail fast on bad data. The entire pipeline is synthetic but modeled after regulated credit‑data workflows.”
