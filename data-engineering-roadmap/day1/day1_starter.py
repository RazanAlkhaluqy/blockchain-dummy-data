"""
Day 1 Starter Script — Blockchain Startup Analytics
======================================================
Covers: pandas basics, PostgreSQL loading, 5 SQL queries via Python
Run: python day1_starter.py
Requires: pip install pandas psycopg2-binary sqlalchemy
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ── 1. LOAD CSV WITH PANDAS ───────────────────────────────────
orders = pd.read_csv("orders.csv", parse_dates=["created_at"])
users  = pd.read_csv("users.csv",  parse_dates=["registered_at"])

print("=== Dataset Overview ===")
print(f"Orders : {len(orders):,} rows | {orders.columns.tolist()}")
print(f"Users  : {len(users):,} rows  | {users.columns.tolist()}")
print()

# ── 2. QUICK PANDAS EXPLORATION ───────────────────────────────
print("=== Revenue by status ===")
print(orders.groupby("status")["final_amount_usd"].agg(["count","sum","mean"]).round(2))
print()

print("=== Top 5 products by revenue (completed orders) ===")
completed = orders[orders["status"] == "completed"]
top_products = (
    completed.groupby("product_name")["final_amount_usd"]
    .sum().sort_values(ascending=False).head(5)
)
print(top_products.round(2))
print()

print("=== Payment method breakdown ===")
print(completed["payment_method"].value_counts())
print()

print("=== Monthly revenue trend ===")
completed_copy = completed.copy()
completed_copy["month"] = completed_copy["created_at"].dt.to_period("M")
monthly = completed_copy.groupby("month")["final_amount_usd"].sum().round(2)
print(monthly)
print()

# ── 3. LOAD INTO POSTGRESQL ───────────────────────────────────
# Update the connection string with your credentials:
DB_URL = "postgresql://postgres:password@localhost:5432/blockchain_db"

try:
    engine = create_engine(DB_URL)

    print("=== Loading data into PostgreSQL ===")
    users.to_sql("users",  engine, if_exists="replace", index=False, method="multi")
    orders.to_sql("orders", engine, if_exists="replace", index=False, method="multi")
    print("Tables loaded: users, orders")
    print()

    # ── 4. RUN SQL QUERIES VIA PYTHON ─────────────────────────
    with engine.connect() as conn:

        print("=== Q1: Revenue by month ===")
        result = conn.execute(text("""
            SELECT
                DATE_TRUNC('month', created_at) AS month,
                COUNT(*) FILTER (WHERE status='completed') AS orders,
                ROUND(SUM(final_amount_usd) FILTER (WHERE status='completed')::numeric, 2) AS revenue_usd
            FROM orders
            GROUP BY 1 ORDER BY 1
        """))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        print()

        print("=== Q2: Top 10 customers by lifetime value ===")
        result = conn.execute(text("""
            SELECT u.user_id, u.wallet_address, u.plan_tier,
                   COUNT(o.order_id) AS orders,
                   ROUND(SUM(o.final_amount_usd)::numeric, 2) AS ltv_usd
            FROM users u
            JOIN orders o ON u.user_id = o.user_id
            WHERE o.status = 'completed'
            GROUP BY u.user_id, u.wallet_address, u.plan_tier
            ORDER BY ltv_usd DESC LIMIT 10
        """))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        print()

        print("=== Q3: Chain revenue breakdown ===")
        result = conn.execute(text("""
            SELECT COALESCE(chain,'fiat') AS chain,
                   COUNT(*) AS orders,
                   ROUND(SUM(final_amount_usd)::numeric, 2) AS revenue_usd,
                   ROUND(AVG(gas_fee_usd)::numeric, 4) AS avg_gas_usd
            FROM orders WHERE status = 'completed'
            GROUP BY 1 ORDER BY 2 DESC
        """))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        print()

        print("=== Q4: KYC gap — revenue at risk ===")
        result = conn.execute(text("""
            SELECT u.kyc_verified,
                   COUNT(DISTINCT u.user_id) AS users,
                   ROUND(SUM(o.final_amount_usd)::numeric, 2) AS revenue_usd
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id AND o.status = 'completed'
            GROUP BY u.kyc_verified
        """))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))

except Exception as e:
    print(f"[PostgreSQL skipped — update DB_URL first]\nError: {e}")
    print("The pandas section above ran fine without a DB connection.")

print("\n=== Day 1 complete! Commit your work: git add . && git commit -m 'day1: load and query blockchain orders' ===")
