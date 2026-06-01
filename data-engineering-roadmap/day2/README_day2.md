# Day 2 — Databases & Data Modelling
### Star Schema · SCD Type 2 · Indexing · OLAP vs OLTP · Blockchain Infrastructure

---

## What this project builds

A production-grade star schema for a blockchain analytics platform:
- 4 dimension tables (date, wallet, token, product)
- 1 central fact table (10,000 on-chain transactions)
- SCD Type 2 wallet history (tracks risk level + KYC changes over time)
- 8 indexes for query performance
- 8 SQL queries covering OLAP joins, risk exposure, window functions, and point-in-time lookups
- 40+ automated exam checks across 4 sections

---

## Star Schema Design

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  date_id PK │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────▼───────────┐    ┌─────────────┐
│  dim_wallet  │    │ fact_transactions │    │  dim_token  │
│  wallet_id PK│◄───│  tx_id PK        │───►│  token_id PK│
└──────────────┘    │  date_id  FK     │    └─────────────┘
                    │  from_wallet FK  │
┌──────────────┐    │  to_wallet FK    │
│  dim_product │◄───│  token_id FK     │
│  product_id  │    │  product_id FK   │
└──────────────┘    │  amount_usd      │
                    │  gas_fee_usd     │
┌──────────────┐    │  chain           │
│ dim_wallet   │    │  status          │
│ _scd (Type2) │    └──────────────────┘
│ valid_from   │
│ valid_to     │
│ is_current   │
└──────────────┘
```

---

## Files

```
day2-blockchain/
├── dim_date.csv              # 366 rows — 2024 date dimension
├── dim_wallet.csv            # 300 wallets with risk levels
├── dim_token.csv             # 10 tokens (ETH, BTC, USDC, SOL...)
├── dim_product.csv           # 10 products
├── dim_wallet_scd.csv        # 160 rows — SCD Type 2 wallet history
├── fact_transactions.csv     # 10,000 on-chain transactions
├── day2_starter.py           # Python: load + OLAP analytics + 40 exams
├── day2_practice.sql         # SQL: CREATE schema + 8 queries
└── README_day2.md            # This file
```

---

## Setup

### Start PostgreSQL (if not already running)
```bash
docker start blockchain-db
# or start fresh:
docker run --name blockchain-db \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=blockchain_db \
  -p 5432:5432 -d postgres:15
```

### Install dependencies
```bash
pip install pandas psycopg2-binary sqlalchemy
```

---

## Run the Python script

```bash
cd day2-blockchain
python3 day2_starter.py
```

**4 sections, 40+ exam checks:**

| Section | What runs | Exams |
|---------|-----------|-------|
| 1 | Load all 6 CSVs, verify foreign keys, check grain | 10 |
| 2 | OLAP joins in pandas: token volume, risk exposure, weekend analysis | 8 |
| 3 | SCD Type 2: validate point-in-time records, valid_from/to logic | 7 |
| 4 | Load to PostgreSQL, create indexes, run 4 SQL queries, cross-validate | 7 |

---

## Run the SQL file

```bash
# From the folder containing all CSV files:
psql -U postgres -h localhost -d blockchain_db -f day2_practice.sql
```

### Inside DBeaver:
1. Connect to `localhost:5432` / `blockchain_db` / `postgres` / `password`
2. Open `day2_practice.sql`
3. **Ctrl+Shift+Enter** — runs the full file
4. After loading, use the **ER Diagram** view:
   Right-click `blockchain_db` → Tools → ER Diagram → see your star schema visually

---

## The 8 SQL queries explained

| # | Query | Concepts |
|---|-------|----------|
| Q1 | Monthly volume by token | 3-table star join, `GROUP BY` multiple dims |
| Q2 | Wallet risk exposure | `JOIN` + `OVER()` for percentage of total |
| Q3 | Top 20 wallets by volume | 4-table join, aggregation across all dims |
| Q4 | SCD current state | `WHERE is_current = 1` pattern |
| Q5 | SCD audit — who changed | Self-join on SCD table to compare versions |
| Q6 | EXPLAIN ANALYZE | Read query plans, confirm index usage |
| Q7 | Point-in-time query | `BETWEEN valid_from AND valid_to` pattern |
| Q8 | Quarterly OLAP | `RANK() OVER`, `SUM() OVER PARTITION BY` |

---

## Key concepts learned today

### OLTP vs OLAP
- **OLTP** (Online Transaction Processing): normalized tables (3NF), optimized for INSERT/UPDATE speed, used by your app backend — e.g. recording a new transaction
- **OLAP** (Online Analytical Processing): denormalized star schema, optimized for GROUP BY queries across millions of rows, used by your analytics layer — e.g. monthly revenue by chain

### Star Schema
- **Fact table**: one row per event (transaction), contains measures (amounts) and foreign keys to dimensions
- **Dimension tables**: descriptive context (who, what, when, where) — queried via JOIN
- **Grain**: the fact table grain here is one row per on-chain transaction

### SCD Type 2
Tracks historical changes in dimension attributes by keeping multiple rows per entity with `valid_from`, `valid_to`, and `is_current` columns. Used here to track wallet risk level changes over time. Point-in-time query: `WHERE '2024-06-01' BETWEEN valid_from AND valid_to`

### Indexing
- B-tree indexes on `date_id`, `from_wallet_id`, `token_id`, `status`, `chain`
- Run `EXPLAIN ANALYZE` to verify the query planner uses them (look for `Index Scan` instead of `Seq Scan`)

### CAP Theorem applied to blockchain
- **Consistency**: all nodes see the same data (important for balance queries)
- **Availability**: system responds even during node failures
- **Partition tolerance**: system works despite network splits
- Blockchain chooses **C + P** (Ethereum) — you may see stale reads during forks, which is why you always verify `status = 'confirmed'` and check block finality

---

## Commit and push

```bash
git add .
git commit -m "day2: star schema, SCD Type 2, indexing — all exams passed"
git push
```

---

## What comes next

**Day 3 — ETL & Apache Airflow**: automate today's manual `\copy` load into a scheduled DAG that runs every night, adds data quality checks, and sends alerts on failure.

---

*Part of the 7-Day Data Engineer Roadmap — Blockchain Infrastructure Track*
