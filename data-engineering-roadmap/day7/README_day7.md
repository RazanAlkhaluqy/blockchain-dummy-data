# Day 7 — Data Governance, Quality & Capstone
### Lineage · Great Expectations · GDPR · Monitoring · End-to-end Platform

---

## What this builds

The complete governance layer on top of the 6-day blockchain data platform:
- Data lineage graph: 13 nodes, 12 edges, 5 layers (raw → staging → intermediate → mart → serving)
- Impact analysis: what breaks downstream when a model changes
- Great Expectations-style quality suite: 10 checks on capstone data
- 90-day quality score per model with flaky model detection
- GDPR compliance: pseudonymisation, right to erasure, k-anonymity, data minimisation
- Pipeline monitoring: SLA breach detection, cost tracking, health dashboard
- Full capstone ETL: 20,000 events through complete pipeline end-to-end
- 8 SQL governance and analytics queries
- Full 7-day portfolio summary

---

## Files

```
day7/
├── data_quality_checks.csv     # 2,880 DQ check records (90 days × 8 models)
├── lineage_metadata.json       # 13-node lineage graph (JSON)
├── pipeline_metrics.csv        # 500 pipeline run records with SLA + cost
├── capstone_events.csv         # 20,000 final blockchain events
├── day7_capstone.py            # Main script: lineage + DQ + GDPR + capstone
├── day7_queries.sql            # 8 final SQL queries
└── README_day7.md              # This file
```

---

## Setup

```bash
docker start blockchain-db
pip install pandas psycopg2-binary sqlalchemy
```

---

## Run

```bash
cd day7
python3 day7_capstone.py
psql -U postgres -h localhost -d blockchain_db -f day7_queries.sql
```

---

## What each task covers

### Task 1 — Data Lineage
Parse a lineage graph and answer two questions every data engineer needs:
- **Impact analysis**: if `stg_blockchain_events` changes, what breaks downstream?
- **Root cause**: what are all the sources feeding `mart_wallet_leaderboard`?

### Task 2 — Data Quality Framework
10-check quality suite on the capstone dataset:
`not_null`, `unique`, `accepted_values`, `min_value`, `regex`, `freshness`
Plus 90-day model health scoring to detect flaky models.

### Task 3 — Privacy & GDPR
Four techniques applied to wallet data:
- **Pseudonymisation**: `wallet_address → USR_<hash>` (one-way, irreversible)
- **Right to erasure**: delete all records for a given wallet token
- **Data minimisation**: analytics export with PII columns removed
- **K-anonymity**: verify group sizes ≥ 5 to prevent re-identification

### Task 4 — Pipeline Monitoring
SLA breach detection, cost tracking, and health dashboard across 5 DAGs.
Identify which pipelines are flaky, which are expensive, and which miss SLAs.

### Task 5 — Capstone ETL
Full extract → transform → quality check → pseudonymise → mart build → load.
All 7 days of work converge here into one end-to-end run.

### Task 6 — Portfolio Summary
Complete summary of all 7 days: tools, datasets, skills, and blockchain-specific
capabilities ready to apply to your startup's infrastructure.

---

## Key concepts

### Data lineage
Tracks where data comes from and where it goes.
Answers: "our mart numbers changed — which upstream model caused it?"
Tools: OpenLineage, Marquez, dbt docs, Apache Atlas

### Great Expectations
Open-source Python library for data quality checks.
Each "expectation" is a test: `expect_column_not_null("tx_hash")`.
Runs automatically in Airflow after each pipeline load.
Generates HTML data quality reports.

### GDPR for blockchain data
Wallet addresses are pseudonymous by nature but become PII when linked to a real person.
- Store the wallet → pseudonymised token mapping in an encrypted lookup table
- All warehouse tables only contain tokens, never raw addresses
- Right to erasure: delete the mapping → all other tables become permanently anonymised

### K-anonymity
A dataset satisfies k-anonymity if every combination of quasi-identifiers
appears in at least k rows. Prevents singling out individuals from aggregate data.
Minimum k=5 is the standard threshold.

---

## Final commit — complete roadmap

```bash
cd ~/data-engineering-roadmap
git add day7/
git commit -m "day7: governance, data quality, GDPR, capstone — 7-day roadmap complete"
git push
```

Your portfolio: https://github.com/YOUR_USERNAME/data-engineering-roadmap

---

*7-Day Data Engineering Roadmap — Blockchain Infrastructure Track — Complete*
