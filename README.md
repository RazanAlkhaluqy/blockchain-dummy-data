# Day 1 — Python + SQL Foundations
### Blockchain Startup Analytics · Data Engineering Roadmap

---

## What this project does

A complete data pipeline for a blockchain SaaS startup:
load 10,000 orders + 200 users from CSV into PostgreSQL,
run 6 analytical SQL queries (monthly revenue, top customers by wallet,
chain breakdown, product funnel, rolling averages, KYC gap analysis),
and validate every result with 30+ senior-level automated exam checks.

---

## Folder structure

```
day1-blockchain/
├── orders.csv              # 10,000 orders — wallets, chains, tx hashes, gas fees
├── users.csv               # 200 users — plan tiers, KYC status, wallet addresses
├── day1_starter.py         # Main script: pandas + PostgreSQL + all exam checks
├── day1_practice.sql       # Standalone SQL: CREATE, COPY, 6 queries + BONUS
└── README.md               # This file
```

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.11+ | [python.org](https://python.org) |
| Docker | any | [docker.com](https://docker.com) |
| Git | any | [git-scm.com](https://git-scm.com) |
| DBeaver (optional) | any | [dbeaver.io](https://dbeaver.io) |

---

## Setup — run once

### 1. Clone / create project folder

```bash
mkdir day1-blockchain && cd day1-blockchain
# copy all 4 files here
```

### 2. Install Python libraries

```bash
pip install pandas psycopg2-binary sqlalchemy
```

### 3. Start PostgreSQL with Docker

```bash
docker run --name blockchain-db \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=blockchain_db \
  -p 5432:5432 \
  -d postgres:15
```

Verify it is running:

```bash
docker ps
# Should show: blockchain-db   Up X seconds
```

### 4. Initialise Git

```bash
git init
echo "__pycache__/\n*.pyc\n.env" > .gitignore
git add .
git commit -m "day1: initial project setup"
```

---

## Run the Python script

```bash
python day1_starter.py
```

What runs in order:

1. **Section 1** — loads CSVs with pandas, prints shape + dtypes + describe
2. **Exam 1** — 12 schema checks (row counts, dtypes, no duplicates, wallet format)
3. **Section 2** — data quality audit (nulls, negatives, referential integrity)
4. **Exam 2** — 10 data quality assertions
5. **Section 3** — pandas analytics (groupby, merge, window functions)
6. **Exam 3** — 10 pandas correctness checks
7. **Section 4** — loads into PostgreSQL, runs all 6 SQL queries
8. **Exam 4** — 12 SQL correctness checks (counts, sort order, math cross-check)

Expected output when all checks pass:

```
✔ PASS  orders has 10,000 rows
✔ PASS  created_at parsed as datetime
✔ PASS  No duplicate order_ids
...
✔ PASS  Q1 total revenue matches pandas total (within $1)
```

---

## Run the SQL file

### Option A — psql command line (recommended)

```bash
# Make sure your CSV files are in the current directory
psql -U postgres -h localhost -d blockchain_db -f day1_practice.sql
```

Password when prompted: `password`

You will see each query's output printed with section headers like:

```
------------------------------------------------------------
 Q1: Revenue by month
------------------------------------------------------------
  month   | total_orders | completed_orders | revenue_usd | avg_order_usd
----------+--------------+------------------+-------------+--------------
 2024-01  |          834 |              593 |    58420.15 |         98.52
 ...
```

### Option B — psql interactive shell

```bash
psql -U postgres -h localhost -d blockchain_db
```

Then inside psql:

```sql
\i day1_practice.sql
```

### Option C — DBeaver (GUI)

1. New Connection → PostgreSQL → host `localhost`, port `5432`, db `blockchain_db`, user `postgres`, password `password`
2. Open `day1_practice.sql` in the SQL editor
3. Press **Ctrl+Enter** to run each query block, or **Ctrl+Shift+Enter** for the full file

### Option D — run a single query via Python

```python
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("postgresql://postgres:password@localhost:5432/blockchain_db")
with engine.connect() as conn:
    df = pd.read_sql(text("SELECT * FROM orders LIMIT 10"), conn)
    print(df)
```

---

## The 6 SQL queries explained

| # | Query | SQL concepts practiced |
|---|-------|----------------------|
| Q1 | Revenue by month | `DATE_TRUNC`, `FILTER`, `GROUP BY` |
| Q2 | Top 10 customers by LTV | `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT` |
| Q3 | Revenue by chain + payment method | `COALESCE`, multi-column `GROUP BY` |
| Q4 | Product conversion funnel | Conditional aggregation with `FILTER` |
| Q5 | 7-day rolling revenue | `CTE`, window function `ROWS BETWEEN` |
| Q6 | KYC compliance gap | `LEFT JOIN`, `OVER()` window for percentages |
| BONUS | Revenue by plan tier | `SUM(...) OVER()` for share calculation |

---

## Exam summary

| Section | Checks | What is tested |
|---------|--------|---------------|
| Exam 1 | 12 | Schema: row counts, dtypes, duplicates, wallet format, tx hash format |
| Exam 2 | 10 | Quality: nulls, negatives, referential integrity, business rules |
| Exam 3 | 10 | pandas: groupby correctness, dtypes, join cardinality, revenue thresholds |
| Exam 4 | 12 | SQL: DB row counts, query shape, sort order, cross-validation vs pandas |
| **Total** | **44** | |

---

## Commit your work

```bash
git add .
git commit -m "day1: blockchain orders pipeline — 44 exams passed"
```

Push to GitHub (first time):

```bash
gh repo create day1-blockchain --public --source=. --push
# or manually:
git remote add origin https://github.com/YOUR_USERNAME/day1-blockchain.git
git push -u origin main
```

---

## What comes next

| Day | Topic | Builds on today by... |
|-----|-------|----------------------|
| Day 2 | Databases & Modelling | designing proper star schema for these same tables |
| Day 3 | ETL & Airflow | automating today's manual load into a scheduled DAG |
| Day 4 | Spark | processing 1M row version of orders.csv in parallel |
| Day 5 | Kafka | streaming new orders in real-time instead of batch |

---

## Troubleshooting

**`psycopg2.OperationalError: could not connect`**
→ Check Docker is running: `docker ps`
→ Restart container: `docker start blockchain-db`

**`\copy: No such file`**
→ Run psql from the same folder as the CSV files, or use full path:
`\copy orders FROM '/full/path/to/orders.csv' CSV HEADER`

**`relation "orders" does not exist`**
→ The `\copy` needs the tables created first.
The SQL file handles this — run the full file, not individual queries.

**pandas `SettingWithCopyWarning`**
→ Already suppressed with `warnings.filterwarnings("ignore")`. In production code use `.copy()` explicitly after filtering.

---

*Part of the 7-Day Data Engineer Roadmap — Blockchain Infrastructure Track*
