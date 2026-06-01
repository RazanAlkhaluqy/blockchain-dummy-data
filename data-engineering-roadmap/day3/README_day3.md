# Day 3 — ETL Pipelines & Apache Airflow
### Extract · Transform · Load · dbt concepts · Idempotency · Data Quality

---

## What this project builds

A complete ETL pipeline for a blockchain startup:
- Extract from 3 sources: JSON events, CSV price feed, CSV app logs
- Transform: filter, deduplicate, quality-check, enrich with prices, add derived columns
- Load: idempotent PostgreSQL staging tables + analytics marts
- Airflow DAG: a real schedulable pipeline with retry logic, branching, XCom, and alerts
- 8 SQL validation and analytics queries
- 35+ automated exam checks across 4 sections

---

## Files

```
day3/
├── raw_blockchain_events.json    # 5,000 raw on-chain events
├── raw_api_prices.csv            # 3,650 daily token prices (10 tokens × 365 days)
├── raw_app_logs.csv              # 2,000 application log entries
├── pipeline_run_log.csv          # 200 simulated DAG run history records
├── day3_pipeline.py              # Main ETL script: extract → transform → load + exams
├── blockchain_etl_dag.py         # Real Apache Airflow DAG (ready to deploy)
├── day3_queries.sql              # 8 SQL queries for validation + analytics
└── README_day3.md                # This file
```

---

## Setup

```bash
docker start blockchain-db
pip install pandas psycopg2-binary sqlalchemy
```

---

## Run the ETL pipeline

```bash
cd day3
python3 day3_pipeline.py
```

**4 tasks, 35+ exam checks:**

| Task | What runs | Exams |
|------|-----------|-------|
| Extract | Load 3 sources into pandas DataFrames | 9 |
| Transform | Filter, dedup, quality check, enrich, derive columns | 10 |
| Load | Idempotent PostgreSQL load, indexes, re-run test | 6 |
| Airflow sim | DAG task execution order printed, concepts validated | 5 |

---

## Run the SQL queries

```bash
psql -U postgres -h localhost -d blockchain_db -f day3_queries.sql
```

| Query | What it does |
|-------|-------------|
| Q1 | Data quality report on staged table |
| Q2 | Volume by chain + token post-ETL |
| Q3 | Hourly transaction pattern with ASCII bar chart |
| Q4 | Monthly completeness check |
| Q5 | High-value transaction alert candidates |
| Q6 | Token price mart validation |
| Q7 | Pipeline health — success rate by DAG |
| Q8 | dbt-style mart with window functions |

---

## Deploy the real Airflow DAG (optional today, required Day 6)

```bash
pip install apache-airflow
airflow db init
airflow users create --username admin --password admin \
    --firstname A --lastname B --role Admin --email a@b.com

# copy DAG to Airflow's dags folder
cp blockchain_etl_dag.py ~/airflow/dags/

# start Airflow in two terminals
airflow webserver --port 8080
airflow scheduler
```

Open http://localhost:8080 → login → find `blockchain_etl_pipeline` → toggle ON → Trigger DAG.

---

## Key concepts

### ETL vs ELT
- **ETL**: transform before loading (today's approach — clean in Python, load clean data)
- **ELT**: load raw first, transform inside the warehouse (BigQuery/dbt approach — Day 6)
- Blockchain data uses ETL because raw on-chain data is messy and large

### Idempotency
Running the pipeline twice must produce the same result. Achieved by:
`if_exists="replace"` on `to_sql()` — drops and recreates the table each run.
In production: use `TRUNCATE + INSERT` or `MERGE/UPSERT` with tx_hash as the unique key.

### Data quality checks
Four checks applied before loading:
1. `amount_usd > 0` — no zero or negative values
2. `status == 'confirmed'` — only finalized on-chain data
3. `tx_hash` not null and not duplicate — no reprocessed transactions
4. Known token and chain values — no corrupt/unknown source data

### Airflow concepts
- **DAG** (Directed Acyclic Graph): the pipeline definition — tasks + their order
- **Operator**: a task type — `PythonOperator`, `BranchPythonOperator`, `EmptyOperator`
- **XCom**: key-value store for passing data between tasks (e.g. row count)
- **TriggerRule**: when should a task run — `ALL_SUCCESS`, `ONE_SUCCESS`, `ALL_DONE`
- **Schedule**: cron expression — `0 2 * * *` means every day at 2am
- **Retries**: if a task fails, try again N times with a delay

### dbt concepts (applied manually today)
- **Staging model**: one-to-one with source, just cleans and renames — `stg_blockchain_events`
- **Mart model**: aggregated, business-logic layer — `mart_token_prices`, `mart_pipeline_runs`
- **Incremental model**: only process new rows since last run (Day 6 with dbt installed)

---

## Commit

```bash
git add .
git commit -m "day3: ETL pipeline, data quality, idempotent load, Airflow DAG"
git push
```

---

*Part of the 7-Day Data Engineer Roadmap — Blockchain Infrastructure Track*
