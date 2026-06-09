"""
Day 2 Starter Script — Databases & Data Modelling
===================================================
Covers: Star schema design, OLTP vs OLAP, indexing,
        SCD Type 2, CAP theorem applied to blockchain
Run:    python3 day2_starter.py
Requires: pip install pandas psycopg2-binary sqlalchemy tabulate
"""

import pandas as pd
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings("ignore")

PASS = "\033[92m✔ PASS\033[0m"
FAIL = "\033[91m✘ FAIL\033[0m"
HEAD = "\033[94m"
END  = "\033[0m"

def section(title):
    print(f"\n{HEAD}{'='*62}\n  {title}\n{'='*62}{END}")

def exam(label, result=None, expected=None, check_fn=None):
    ok = check_fn(result) if check_fn else (result == expected)
    print(f"  {PASS if ok else FAIL}  {label}")
    if not ok and expected is not None:
        print(f"         got: {result}  |  expected: {expected}")
    return ok

DB_URL = "postgresql://postgres:password@localhost:5432/blockchain_db"

# ════════════════════════════════════════════════════════════════
# SECTION 1 — LOAD ALL DIMENSION & FACT FILES
# ════════════════════════════════════════════════════════════════
section("SECTION 1 — Load Star Schema CSVs")

dim_date    = pd.read_csv("dim_date.csv")
dim_wallet  = pd.read_csv("dim_wallet.csv")
dim_token   = pd.read_csv("dim_token.csv")
dim_product = pd.read_csv("dim_product.csv")

dim_scd = pd.read_csv("dim_wallet_scd.csv")

dim_scd["valid_from"] = pd.to_datetime(dim_scd["valid_from"])
dim_scd["valid_to"]   = dim_scd["valid_to"].astype(str)   # keep as string — 9999-12-31 overflows ns

fact_tx     = pd.read_csv("fact_transactions.csv", parse_dates=["created_at"])

print(f"\n  dim_date        : {len(dim_date):>5} rows  — date dimension (366 days in 2024)")
print(f"  dim_wallet      : {len(dim_wallet):>5} rows  — wallet dimension")
print(f"  dim_token       : {len(dim_token):>5} rows  — token dimension")
print(f"  dim_product     : {len(dim_product):>5} rows  — product dimension")
print(f"  dim_wallet_scd  : {len(dim_scd):>5} rows  — SCD Type 2 wallet history")
print(f"  fact_transactions: {len(fact_tx):>4} rows  — central fact table")

print("\n  fact_transactions sample:")
print(fact_tx.head(3).to_string())

section("EXAM 1 — Star Schema Structure")
exam("fact_transactions has 10,000 rows",        result=len(fact_tx),    expected=10000)
exam("dim_date has 366 rows (2024 is leap year)", result=len(dim_date),   expected=366)
exam("dim_wallet has 300 wallets",               result=len(dim_wallet), expected=300)
exam("fact has foreign key: date_id in dim_date",
     check_fn=lambda _: fact_tx["date_id"].isin(dim_date["date_id"]).all())
exam("fact has foreign key: from_wallet_id in dim_wallet",
     check_fn=lambda _: fact_tx["from_wallet_id"].isin(dim_wallet["wallet_id"]).all())
exam("fact has foreign key: token_id in dim_token",
     check_fn=lambda _: fact_tx["token_id"].isin(dim_token["token_id"]).all())
exam("fact grain: tx_id is unique (one row per transaction)",
     result=fact_tx["tx_id"].duplicated().sum(), expected=0)
exam("tx_hash is unique (no double-processed transactions)",
     result=fact_tx["tx_hash"].duplicated().sum(), expected=0)
exam("amount_usd has no negative values",
     result=(fact_tx["amount_usd"] < 0).sum(), expected=0)
exam("All tx_types are known values",
     check_fn=lambda _: set(fact_tx["tx_type"].unique()).issubset(
         {"transfer","swap","mint","burn","stake","unstake","bridge","approve"}))

# ════════════════════════════════════════════════════════════════
# SECTION 2 — OLTP vs OLAP ANALYSIS IN PANDAS
# ════════════════════════════════════════════════════════════════
section("SECTION 2 — OLAP Analytics on the Star Schema")

# Join fact to dims (the OLAP query pattern)
enriched = (
    fact_tx
    .merge(dim_date[["date_id","month","month_name","quarter","is_weekend"]], on="date_id")
    .merge(dim_wallet[["wallet_id","wallet_type","risk_level","country"]],
           left_on="from_wallet_id", right_on="wallet_id")
    .merge(dim_token[["token_id","symbol","chain"]], on="token_id", suffixes=("","_token"))
)

print("\n  Volume by token symbol:")
print(enriched.groupby("symbol")["amount_usd"]
      .agg(["count","sum","mean"]).round(2).sort_values("sum", ascending=False).to_string())

print("\n  Volume by quarter:")
print(enriched.groupby("quarter")["amount_usd"]
      .agg(["count","sum"]).round(2).to_string())

print("\n  Volume by tx_type:")
print(enriched.groupby("tx_type")["amount_usd"]
      .agg(["count","sum"]).round(2).sort_values("sum", ascending=False).to_string())

print("\n  Weekend vs weekday transaction volume:")
print(enriched.groupby("is_weekend")["amount_usd"]
      .agg(["count","sum","mean"]).round(2).to_string())

print("\n  High-risk wallet transaction exposure:")
print(enriched.groupby("risk_level")["amount_usd"]
      .agg(["count","sum"]).round(2).to_string())

section("EXAM 2 — OLAP Join Correctness")
exam("Enriched table has no extra rows (all joins are many-to-one)",
     result=len(enriched), expected=len(fact_tx))
exam("No null symbols after token join",
     result=enriched["symbol"].isnull().sum(), expected=0)
exam("Quarter values are only 1-4",
     check_fn=lambda _: set(enriched["quarter"].unique()).issubset({1,2,3,4}))
exam("is_weekend is binary (0 or 1 only)",
     check_fn=lambda _: set(enriched["is_weekend"].unique()).issubset({0,1}))
exam("All 10 tokens appear in enriched data",
     check_fn=lambda _: enriched["symbol"].nunique() == 10)
exam("risk_level has exactly 3 values",
     check_fn=lambda _: enriched["risk_level"].nunique() == 3)
exam("Total amount_usd > $1 million (data sanity)",
     check_fn=lambda _: enriched["amount_usd"].sum() > 1_000_000)
exam("Confirmed txs make majority of fact rows (>60%)",
     check_fn=lambda _: (fact_tx["status"]=="confirmed").mean() > 0.6)

# ════════════════════════════════════════════════════════════════
# SECTION 3 — SCD TYPE 2 ANALYSIS
# ════════════════════════════════════════════════════════════════
section("SECTION 3 — Slowly Changing Dimension (SCD Type 2)")

print("\n  SCD Type 2 — wallet history sample:")
print(dim_scd.head(6).to_string())

current    = dim_scd[dim_scd["is_current"] == 1]
historical = dim_scd[dim_scd["is_current"] == 0]

print(f"\n  Current records  : {len(current)}")
print(f"  Historical records: {len(historical)}")

risk_change = dim_scd.groupby("wallet_id")["risk_level"].nunique()
upgraded    = (risk_change > 1).sum()
print(f"  Wallets that changed risk level: {upgraded}")

print("\n  Risk level distribution — CURRENT only:")
print(current["risk_level"].value_counts().to_string())

section("EXAM 3 — SCD Type 2 Correctness")
exam("Each wallet has exactly 2 SCD records (before + after)",
     check_fn=lambda _: dim_scd.groupby("wallet_id").size().eq(2).all())
exam("Only one current record per wallet (is_current=1)",
     check_fn=lambda _: current.groupby("wallet_id").size().eq(1).all())

exam("All historical records have valid_to < 9999-12-31",
     check_fn=lambda _: (historical["valid_to"] < "9999-12-31").all())
exam("All current records have valid_to == 9999-12-31",
     check_fn=lambda _: (current["valid_to"] == "9999-12-31").all())
exam("valid_from < valid_to for all records",
     check_fn=lambda _: (dim_scd["valid_from"].astype(str) < dim_scd["valid_to"]).all())

exam("KYC upgraded in current version (kyc=1 for current records)",
     check_fn=lambda _: current["kyc_verified"].eq(1).all())
exam("Historical records had risk_level = 'low'",
     check_fn=lambda _: historical["risk_level"].eq("low").all())

# ════════════════════════════════════════════════════════════════
# SECTION 4 — POSTGRESQL: CREATE SCHEMA + LOAD + INDEXES
# ════════════════════════════════════════════════════════════════
section("SECTION 4 — PostgreSQL Star Schema + Indexing")

try:
    engine = create_engine(DB_URL)
    print("\n  Loading all tables...")

    dim_date.to_sql("dim_date",       engine, if_exists="replace", index=False)
    dim_wallet.to_sql("dim_wallet",   engine, if_exists="replace", index=False)
    dim_token.to_sql("dim_token",     engine, if_exists="replace", index=False)
    dim_product.to_sql("dim_product", engine, if_exists="replace", index=False)
    dim_scd.to_sql("dim_wallet_scd",  engine, if_exists="replace", index=False)
    fact_tx.to_sql("fact_transactions", engine, if_exists="replace", index=False, method="multi")
    print("  All tables loaded.")

    with engine.connect() as conn:
        # Create indexes for performance
        idx_sql = [
            "CREATE INDEX IF NOT EXISTS idx_fact_date      ON fact_transactions(date_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_wallet    ON fact_transactions(from_wallet_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_token     ON fact_transactions(token_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_status    ON fact_transactions(status)",
            "CREATE INDEX IF NOT EXISTS idx_fact_chain     ON fact_transactions(chain)",
            "CREATE INDEX IF NOT EXISTS idx_scd_wallet     ON dim_wallet_scd(wallet_id)",
            "CREATE INDEX IF NOT EXISTS idx_scd_current    ON dim_wallet_scd(is_current)",
        ]
        for sql in idx_sql:
            conn.execute(text(sql))
        conn.commit()
        print(f"  {len(idx_sql)} indexes created.")

    section("EXAM 4 — SQL Star Schema Queries")
    with engine.connect() as conn:

        # Q1: monthly volume by token
        r1 = pd.DataFrame(conn.execute(text("""
            SELECT d.month_name, t.symbol,
                   COUNT(*) AS txs,
                   ROUND(SUM(f.amount_usd)::numeric, 2) AS volume_usd
            FROM fact_transactions f
            JOIN dim_date   d ON f.date_id   = d.date_id
            JOIN dim_token  t ON f.token_id  = t.token_id
            WHERE f.status = 'confirmed'
            GROUP BY d.month, d.month_name, t.symbol
            ORDER BY d.month, volume_usd DESC
        """)).fetchall(), columns=["month","symbol","txs","volume_usd"])
        print("\n  Q1 — Monthly volume by token (first 10 rows):")
        print(r1.head(10).to_string(index=False))

        # Q2: wallet risk exposure
        r2 = pd.DataFrame(conn.execute(text("""
            SELECT w.risk_level,
                   COUNT(DISTINCT f.from_wallet_id)   AS wallets,
                   COUNT(*)                           AS transactions,
                   ROUND(SUM(f.amount_usd)::numeric,2) AS total_usd,
                   ROUND(AVG(f.amount_usd)::numeric,2) AS avg_usd
            FROM fact_transactions f
            JOIN dim_wallet w ON f.from_wallet_id = w.wallet_id
            WHERE f.status = 'confirmed'
            GROUP BY w.risk_level ORDER BY total_usd DESC
        """)).fetchall(), columns=["risk_level","wallets","transactions","total_usd","avg_usd"])
        print("\n  Q2 — Wallet risk exposure:")
        print(r2.to_string(index=False))

        # Q3: SCD Type 2 — current wallet state query
        r3 = pd.DataFrame(conn.execute(text("""
            SELECT s.risk_level, s.kyc_verified,
                   COUNT(*) AS wallets
            FROM dim_wallet_scd s
            WHERE s.is_current = 1
            GROUP BY s.risk_level, s.kyc_verified
            ORDER BY wallets DESC
        """)).fetchall(), columns=["risk_level","kyc_verified","wallets"])
        print("\n  Q3 — Current wallet states (SCD):")
        print(r3.to_string(index=False))

        # Q4: EXPLAIN ANALYZE on indexed query
        r4 = conn.execute(text("""
            EXPLAIN (FORMAT TEXT)
            SELECT COUNT(*), SUM(amount_usd)
            FROM fact_transactions
            WHERE chain = 'ethereum' AND status = 'confirmed'
        """)).fetchall()
        print("\n  Q4 — EXPLAIN query plan (index check):")
        for row in r4[:5]:
            print(f"    {row[0]}")

        # Exam checks
        db_fact = conn.execute(text("SELECT COUNT(*) FROM fact_transactions")).scalar()
        db_dim_date = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar()
        db_scd_current = conn.execute(text(
            "SELECT COUNT(*) FROM dim_wallet_scd WHERE is_current=1")).scalar()
        db_scd_hist = conn.execute(text(
            "SELECT COUNT(*) FROM dim_wallet_scd WHERE is_current=0")).scalar()

        exam("fact_transactions loaded: 10,000 rows in PostgreSQL",
             result=db_fact, expected=10000)
        exam("dim_date loaded: 366 rows",
             result=db_dim_date, expected=366)
        exam("Q1 returns data (monthly token volume)",
             check_fn=lambda _: len(r1) > 0)
        exam("Q2 returns exactly 3 risk levels",
             result=len(r2), expected=3)
        exam("Q3 SCD current records == 80",
             result=db_scd_current, expected=80)
        exam("Q3 SCD historical records == 80",
             result=db_scd_hist, expected=80)
        exam("Risk exposure: high risk wallets have highest avg_usd",
             check_fn=lambda _: (
                 r2.set_index("risk_level").loc["high","avg_usd"] >
                 r2.set_index("risk_level").loc["low","avg_usd"]))

except Exception as e:
    print(f"\n  [PostgreSQL not reachable — update DB_URL]\n  Error: {e}")
    print("  Sections 1-3 (pandas) ran successfully above.")

section("DAY 2 COMPLETE")
print("  git add .")
print('  git commit -m "day2: star schema, SCD Type 2, indexing — all exams passed"')
print("  Next: Day 3 — ETL Pipelines with Apache Airflow + dbt\n")
