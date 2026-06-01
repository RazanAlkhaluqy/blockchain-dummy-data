# Day 6 — Cloud, Data Warehousing & Production Pipeline
### BigQuery · Snowflake · Terraform · dbt · Airflow on Cloud · Cost optimisation

---

## What this builds

A complete production cloud data platform for a blockchain startup:
- BigQuery warehouse with partitioning (by day) + clustering (by chain + token)
- dbt model layers: staging → intermediate → 4 mart tables
- Terraform config: S3 Data Lake + BigQuery + IAM roles (real deployable)
- dbt schema tests: not_null, unique, accepted_values, positive_values
- Cost estimation: partitioned vs unpartitioned queries
- 50,000 warehouse events + 5,000 wallet profiles loaded
- 60-day dbt run history monitoring

---

## Files

```
day6/
├── warehouse_events.csv        # 50,000 cleaned events (warehouse-ready)
├── wallet_profiles.csv         # 5,000 wallet dimension records
├── dbt_model_results.csv       # 480 dbt run history records (60 days × 8 models)
├── terraform_state.json        # Simulated Terraform state (4 cloud resources)
├── day6_cloud.py               # Main script: BigQuery sim, dbt, Terraform, exams
├── main.tf                     # Real Terraform config (deploy to AWS + GCP)
├── dbt_models.sql              # All dbt layers as SQL views + tables
└── README_day6.md              # This file
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
cd day6
python3 day6_cloud.py
psql -U postgres -h localhost -d blockchain_db -f dbt_models.sql
```

---

## Deploy to real cloud (optional — needs accounts)

### Terraform (AWS + GCP)
```bash
brew install terraform
terraform init
terraform plan    # preview — no changes made
terraform apply   # creates real S3, BigQuery, IAM
terraform destroy # tear down when done (avoid costs)
```

### Real BigQuery (Google Cloud free tier = 10GB/month free)
```bash
# Install Google Cloud CLI
brew install --cask google-cloud-sdk
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Upload warehouse_events.csv to BigQuery
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  blockchain_warehouse.fact_transactions \
  warehouse_events.csv

# Query it
bq query --use_legacy_sql=false \
  "SELECT chain, SUM(amount_usd) FROM blockchain_warehouse.fact_transactions
   WHERE DATE(timestamp) = '2024-01-01'
   GROUP BY chain"
```

### Real dbt
```bash
pip install dbt-bigquery    # or dbt-postgres for local
dbt init blockchain_project
# copy model SQL into models/ folder
dbt run --models stg+ mart+
dbt test
dbt docs generate && dbt docs serve   # interactive lineage UI
```

---

## Key concepts

### BigQuery partitioning + clustering
```sql
CREATE TABLE fact_transactions
PARTITION BY DATE(timestamp)          -- splits table into daily folders
CLUSTER BY chain, token_symbol        -- sorts rows within each partition
OPTIONS (require_partition_filter = TRUE);  -- forces WHERE date clause

-- Fast query (scans 1 day only):
SELECT * FROM fact_transactions
WHERE DATE(timestamp) = '2024-01-15'  -- uses partition
  AND chain = 'ethereum';             -- uses clustering

-- Slow query (scans everything — AVOID):
SELECT * FROM fact_transactions
WHERE token_symbol = 'ETH';          -- no partition filter!
```

### dbt model layers
| Layer | Purpose | Example |
|-------|---------|---------|
| Staging (stg_) | Clean + rename raw source | stg_blockchain_events |
| Intermediate (int_) | Business logic, joins, aggregations | int_daily_volume |
| Mart (mart_) | Final business-facing tables | mart_chain_analytics |

### Terraform commands
```bash
terraform init     # download AWS + GCP providers
terraform plan     # dry run — shows what will change
terraform apply    # create/update resources
terraform destroy  # delete everything
```

### Cost optimisation rules
1. Always `PARTITION BY DATE(timestamp)` on time-series tables
2. `CLUSTER BY` the columns you GROUP BY most often
3. `SELECT` only columns you need (BigQuery charges by bytes scanned)
4. Use `WHERE DATE(timestamp) =` not `WHERE timestamp =` (forces partition)
5. Cache query results in BigQuery (free for 24h)

---

## Commit

```bash
git add .
git commit -m "day6: cloud warehouse, dbt staging→mart, Terraform IaC"
git push
```

---

*Part of the 7-Day Data Engineer Roadmap — Blockchain Infrastructure Track*
