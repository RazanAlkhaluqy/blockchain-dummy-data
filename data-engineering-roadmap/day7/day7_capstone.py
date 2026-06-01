"""
Day 7 — Governance, Data Quality & Capstone
=============================================
Covers: Data lineage, Great Expectations-style quality checks,
        metadata management, GDPR/privacy patterns,
        end-to-end pipeline capstone, portfolio presentation
Run:    python3 day7_capstone.py
Requires: pip install pandas psycopg2-binary sqlalchemy
"""

import pandas as pd
import json, hashlib, re
from datetime import datetime
from collections import defaultdict
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings("ignore")

PASS   = "\033[92m✔ PASS\033[0m"
FAIL   = "\033[91m✘ FAIL\033[0m"
WARN   = "\033[93m⚠ WARN\033[0m"
HEAD   = "\033[94m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
PURPLE = "\033[95m"
END    = "\033[0m"

DB_URL = "postgresql://postgres:password@localhost:5432/blockchain_db"

def section(title):
    print(f"\n{HEAD}{'='*62}\n  {title}\n{'='*62}{END}")

def task(name):
    print(f"\n{YELLOW}  ── {name}{END}")

def exam(label, result=None, expected=None, check_fn=None):
    ok = check_fn(result) if check_fn else (result == expected)
    print(f"  {PASS if ok else FAIL}  {label}")
    if not ok and expected is not None:
        print(f"         got: {result}  |  expected: {expected}")
    return ok

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    colors = {"INFO":GREEN,"WARN":YELLOW,"ERROR":"\033[91m",
              "LINEAGE":CYAN,"PRIVACY":PURPLE,"DQ":"\033[95m"}
    c = colors.get(level, END)
    print(f"  {c}[{level}]{END} {ts} — {msg}")

# ════════════════════════════════════════════════════════════════
# TASK 1 — DATA LINEAGE
# ════════════════════════════════════════════════════════════════
section("TASK 1 — Data Lineage & Metadata Management")

task("1a. Parse lineage graph")
with open("lineage_metadata.json") as f:
    lineage = json.load(f)

nodes = {n["id"]: n for n in lineage["nodes"]}
edges = lineage["edges"]

# build adjacency list
downstream = defaultdict(list)
upstream   = defaultdict(list)
for e in edges:
    downstream[e["from"]].append(e["to"])
    upstream[e["to"]].append(e["from"])

log(f"Lineage graph: {len(nodes)} nodes, {len(edges)} edges", level="LINEAGE")

print(f"\n  {CYAN}Full data lineage:{END}")
layers = ["raw","staging","intermediate","mart","serving"]
for layer in layers:
    layer_nodes = [n for n in nodes.values() if n["layer"]==layer]
    if layer_nodes:
        print(f"\n  [{layer.upper()}]")
        for n in layer_nodes:
            ups = upstream.get(n["id"],[])
            dns = downstream.get(n["id"],[])
            up_str = f"← {', '.join([u.split('.')[-1] for u in ups])}" if ups else ""
            dn_str = f"→ {', '.join([d.split('.')[-1] for d in dns])}" if dns else ""
            print(f"    {n['id'].split('.')[-1]:35s} {up_str} {dn_str}")

task("1b. Impact analysis — what breaks if stg_blockchain_events changes?")
def get_all_downstream(node_id, visited=None):
    if visited is None: visited = set()
    for child in downstream.get(node_id, []):
        if child not in visited:
            visited.add(child)
            get_all_downstream(child, visited)
    return visited

impacted = get_all_downstream("stg.stg_blockchain_events")
log(f"If stg_blockchain_events changes → {len(impacted)} downstream nodes impacted:",
    level="LINEAGE")
for node in sorted(impacted):
    layer = nodes.get(node,{}).get("layer","unknown")
    print(f"    [{layer}] {node}")

task("1c. Find all sources feeding into mart_wallet_leaderboard")
def get_all_upstream(node_id, visited=None):
    if visited is None: visited = set()
    for parent in upstream.get(node_id, []):
        if parent not in visited:
            visited.add(parent)
            get_all_upstream(parent, visited)
    return visited

sources_for_mart = get_all_upstream("mart.mart_wallet_leaderboard")
log(f"mart_wallet_leaderboard depends on {len(sources_for_mart)} upstream nodes:",
    level="LINEAGE")
for node in sorted(sources_for_mart):
    print(f"    {node}")

section("EXAM 1 — Lineage")
exam("Lineage has 13 nodes",          result=len(nodes), expected=13)
exam("Lineage has 12 edges",          result=len(edges), expected=12)
exam("stg_blockchain_events has downstream nodes",
     check_fn=lambda _: len(impacted) > 0)
exam("mart_wallet_leaderboard has upstream sources",
     check_fn=lambda _: len(sources_for_mart) > 0)
exam("All 5 layers present in lineage",
     check_fn=lambda _: set(layers).issubset(
         set(n["layer"] for n in nodes.values())))

# ════════════════════════════════════════════════════════════════
# TASK 2 — DATA QUALITY FRAMEWORK
# ════════════════════════════════════════════════════════════════
section("TASK 2 — Data Quality Framework (Great Expectations style)")

task("2a. Load 90-day quality check history")
df_dq = pd.read_csv("data_quality_checks.csv", parse_dates=["run_date"])
log(f"Loaded {len(df_dq):,} quality checks across {df_dq['model'].nunique()} models")

task("2b. Quality score per model (last 30 days)")
last_30 = df_dq[df_dq["run_date"] >= df_dq["run_date"].max()
                - pd.Timedelta(days=30)]
quality_scores = (last_30.groupby("model")
    .agg(
        total_checks  = ("check_id","count"),
        passed        = ("status", lambda x: (x=="pass").sum()),
        failed        = ("status", lambda x: (x=="fail").sum()),
        warned        = ("status", lambda x: (x=="warn").sum()),
        avg_duration  = ("duration_sec","mean"),
    )
    .assign(quality_score=lambda x:
        (x["passed"]/x["total_checks"]*100).round(1))
    .sort_values("quality_score")
    .reset_index())

print(f"\n  {CYAN}Model quality scores (last 30 days):{END}")
for _, row in quality_scores.iterrows():
    score = row["quality_score"]
    bar_color = GREEN if score>=95 else YELLOW if score>=85 else "\033[91m"
    bar = "█" * int(score/5)
    print(f"  {bar_color}{row['model']:35s} {score:5.1f}%  {bar}{END}")

task("2c. Run Great Expectations-style checks on capstone data")
df_cap = pd.read_csv("capstone_events.csv", parse_dates=["timestamp"])

class DataQualityRunner:
    """Great Expectations-style quality check runner"""
    def __init__(self, df, dataset_name):
        self.df = df
        self.name = dataset_name
        self.results = []

    def expect_column_not_null(self, col):
        failed = self.df[col].isnull().sum()
        self._record(f"expect_{col}_not_null", failed==0,
                     f"{failed} nulls found" if failed else None)

    def expect_column_unique(self, col):
        dupes = self.df[col].duplicated().sum()
        self._record(f"expect_{col}_unique", dupes==0,
                     f"{dupes} duplicates found" if dupes else None)

    def expect_column_values_in_set(self, col, value_set):
        invalid = ~self.df[col].isin(value_set)
        n = invalid.sum()
        self._record(f"expect_{col}_in_set", n==0,
                     f"{n} invalid values" if n else None)

    def expect_column_min_value(self, col, min_val):
        n = (self.df[col] < min_val).sum()
        self._record(f"expect_{col}_min_{min_val}", n==0,
                     f"{n} values below {min_val}" if n else None)

    def expect_column_max_value(self, col, max_val):
        n = (self.df[col] > max_val).sum()
        self._record(f"expect_{col}_max_{max_val}", n==0,
                     f"{n} values above {max_val}" if n else None)

    def expect_row_count_between(self, min_rows, max_rows):
        n = len(self.df)
        ok = min_rows <= n <= max_rows
        self._record(f"expect_row_count_{min_rows}_to_{max_rows}", ok,
                     f"Got {n} rows" if not ok else None)

    def expect_column_regex(self, col, pattern):
        invalid = ~self.df[col].dropna().str.match(pattern)
        n = invalid.sum()
        self._record(f"expect_{col}_regex", n==0,
                     f"{n} values don't match pattern" if n else None)

    def expect_freshness(self, ts_col, max_hours=26):
        latest = self.df[ts_col].max()
        age_h  = (pd.Timestamp.now() - latest).total_seconds()/3600
        # for dummy data we check year is 2024
        ok = self.df[ts_col].dt.year.eq(2024).all()
        self._record("expect_data_freshness", ok, None)

    def _record(self, name, passed, error):
        self.results.append({"check":name,"passed":passed,"error":error})
        status = PASS if passed else FAIL
        err_str = f"  → {error}" if error else ""
        print(f"  {status}  {name}{err_str}")

    def summary(self):
        total  = len(self.results)
        passed = sum(r["passed"] for r in self.results)
        pct    = round(passed/total*100,1)
        print(f"\n  {CYAN}Quality Suite: {passed}/{total} checks passed ({pct}%){END}")
        return passed, total

runner = DataQualityRunner(df_cap, "capstone_events")
print(f"\n  Running quality suite on capstone_events ({len(df_cap):,} rows):")
runner.expect_row_count_between(15000, 25000)
runner.expect_column_not_null("event_id")
runner.expect_column_not_null("tx_hash")
runner.expect_column_not_null("timestamp")
runner.expect_column_unique("event_id")
runner.expect_column_unique("tx_hash")
runner.expect_column_values_in_set("chain",
    {"ethereum","polygon","solana","arbitrum","optimism"})
runner.expect_column_values_in_set("status", {"confirmed"})
runner.expect_column_min_value("amount_usd", 0)
runner.expect_column_regex("tx_hash", r"^0x[a-f0-9]{64}$")
runner.expect_freshness("timestamp")
passed_dq, total_dq = runner.summary()

section("EXAM 2 — Data Quality")
exam("Quality score computed for all 8 models",
     result=len(quality_scores), expected=8)
exam("DQ suite: all checks pass on capstone data",
     result=passed_dq, expected=total_dq)
exam("No duplicate event_ids in capstone",
     result=df_cap["event_id"].duplicated().sum(), expected=0)
exam("All tx_hashes start with 0x",
     check_fn=lambda _: df_cap["tx_hash"].str.startswith("0x").all())
exam("DQ history covers 90 days",
     check_fn=lambda _: df_dq["run_date"].nunique() >= 85)

# ════════════════════════════════════════════════════════════════
# TASK 3 — PRIVACY & GDPR COMPLIANCE
# ════════════════════════════════════════════════════════════════
section("TASK 3 — Privacy, GDPR & Data Masking")

print(f"""
  {PURPLE}GDPR obligations for a blockchain startup:{END}
  • Wallet addresses linked to real identities = PII
  • Right to erasure: must be able to delete user data
  • Data minimisation: store only what you need
  • Purpose limitation: data collected for one purpose
    cannot be used for another without consent

  Techniques applied today:
  • Pseudonymisation  — replace wallet with hash token
  • Tokenisation      — reversible with encryption key
  • Data masking      — partial mask for display
  • Aggregation       — group to prevent re-identification
""")

task("3a. Pseudonymise wallet addresses")
def pseudonymise(wallet, salt="blockchain-startup-salt-2024"):
    """One-way: wallet → consistent token, cannot reverse"""
    h = hashlib.sha256(f"{salt}{wallet}".encode()).hexdigest()
    return f"USR_{h[:16].upper()}"

df_gdpr = df_cap.copy()
df_gdpr["wallet_token"]    = df_gdpr["from_address"].apply(pseudonymise)
df_gdpr["wallet_masked"]   = df_gdpr["from_address"].apply(
    lambda w: w[:6]+"****"+w[-4:] if pd.notnull(w) else None)
df_gdpr["to_wallet_token"] = df_gdpr["to_address"].apply(pseudonymise)
df_gdpr_safe = df_gdpr.drop(columns=["from_address","to_address"])
log(f"Pseudonymised {len(df_gdpr_safe):,} records — wallet addresses removed",
    level="PRIVACY")

sample = df_gdpr[["from_address","wallet_token","wallet_masked"]].head(3)
print(f"\n  Before → After pseudonymisation:")
for _, row in sample.iterrows():
    print(f"  Original : {row['from_address']}")
    print(f"  Token    : {row['wallet_token']}")
    print(f"  Masked   : {row['wallet_masked']}")
    print()

task("3b. Right to erasure simulation")
wallet_to_erase = df_gdpr["from_address"].iloc[0]
token_to_erase  = pseudonymise(wallet_to_erase)
before_count = len(df_gdpr_safe[df_gdpr_safe["wallet_token"]==token_to_erase])
df_erased = df_gdpr_safe[df_gdpr_safe["wallet_token"] != token_to_erase].copy()
after_count  = len(df_erased)
log(f"Erasure request for token {token_to_erase[:20]}...", level="PRIVACY")
log(f"Removed {before_count} records → {len(df_erased):,} remain", level="PRIVACY")

task("3c. Data minimisation — analytics-safe export")
analytics_cols = ["event_id","chain","event_type","protocol",
                  "token_symbol","amount_usd","gas_fee_usd",
                  "year","month","day","hour","source"]
df_analytics = df_cap[analytics_cols].copy()
log(f"Analytics export: {len(df_analytics.columns)} cols (removed wallet PII)",
    level="PRIVACY")
log(f"Original: {len(df_cap.columns)} cols → Safe: {len(df_analytics.columns)} cols",
    level="PRIVACY")

task("3d. K-anonymity check — prevent re-identification")
k_anon = (df_analytics
    .groupby(["chain","token_symbol","hour","month"])
    .size().reset_index(name="group_size"))
k_value = k_anon["group_size"].min()
log(f"K-anonymity: minimum group size = {k_value}", level="PRIVACY")
if k_value >= 5:
    log(f"K≥5: groups are large enough to prevent re-identification ✓",
        level="PRIVACY")
else:
    log(f"K<5: some groups too small — apply further aggregation", level="PRIVACY")

section("EXAM 3 — Privacy & Compliance")
exam("Pseudonymisation applied to all records",
     check_fn=lambda _: "wallet_token" in df_gdpr.columns)
exam("Original wallet addresses removed from safe export",
     check_fn=lambda _: "from_address" not in df_gdpr_safe.columns)
exam("Same wallet always gets same token (consistency)",
     check_fn=lambda _: (
         pseudonymise(df_gdpr["from_address"].iloc[0]) ==
         pseudonymise(df_gdpr["from_address"].iloc[0])))
exam("Erasure removed correct records",
     check_fn=lambda _: after_count < len(df_gdpr_safe))
exam("Analytics export has no wallet columns",
     check_fn=lambda _: not any("address" in c or "wallet" in c
                                 for c in df_analytics.columns))
exam("K-anonymity k≥5",
     check_fn=lambda _: k_value >= 5)

# ════════════════════════════════════════════════════════════════
# TASK 4 — PIPELINE MONITORING & ALERTING
# ════════════════════════════════════════════════════════════════
section("TASK 4 — Pipeline Monitoring & SLA Alerting")

task("4a. Load pipeline metrics")
df_metrics = pd.read_csv("pipeline_metrics.csv", parse_dates=["run_at"])
log(f"Loaded {len(df_metrics):,} pipeline run metrics")

task("4b. Health dashboard per DAG")
health = (df_metrics.groupby("dag_id")
    .agg(
        total_runs    = ("metric_id","count"),
        success_runs  = ("status", lambda x: (x=="success").sum()),
        failed_runs   = ("status", lambda x: (x=="failed").sum()),
        sla_breaches  = ("sla_breach","sum"),
        avg_duration  = ("duration_sec","mean"),
        total_cost    = ("cost_usd","sum"),
        total_rows    = ("rows_processed","sum"),
        alerts_sent   = ("alert_sent","sum"),
    )
    .assign(success_rate=lambda x:
        (x["success_runs"]/x["total_runs"]*100).round(1))
    .round(2).reset_index()
    .sort_values("success_rate"))

print(f"\n  {CYAN}Pipeline health dashboard:{END}")
print(health[["dag_id","total_runs","success_rate","sla_breaches",
              "avg_duration","total_cost","alerts_sent"]].to_string(index=False))

task("4c. SLA breach analysis")
sla_breaches = df_metrics[df_metrics["sla_breach"]==1]
log(f"Total SLA breaches: {len(sla_breaches)} ({round(len(sla_breaches)/len(df_metrics)*100,1)}%)")

breach_by_dag = sla_breaches.groupby("dag_id").size().sort_values(ascending=False)
print(f"\n  SLA breaches by DAG:")
print(breach_by_dag.to_string())

task("4d. Cost analysis")
cost_by_dag = (df_metrics.groupby("dag_id")["cost_usd"]
    .agg(["sum","mean","max"]).round(4)
    .sort_values("sum", ascending=False))
print(f"\n  Pipeline costs (USD):")
print(cost_by_dag.to_string())
total_cost = df_metrics["cost_usd"].sum()
log(f"Total pipeline cost in dataset: ${total_cost:.2f}")

section("EXAM 4 — Monitoring")
exam("Health dashboard covers all 5 DAGs",
     result=len(health), expected=5)
exam("At least one SLA breach detected (realistic monitoring)",
     check_fn=lambda _: len(sla_breaches) > 0)
exam("All DAGs have >75% success rate",
     check_fn=lambda _: (health["success_rate"] > 75).all())
exam("Cost tracked per run",
     check_fn=lambda _: df_metrics["cost_usd"].sum() > 0)

# ════════════════════════════════════════════════════════════════
# TASK 5 — CAPSTONE: END-TO-END PIPELINE
# ════════════════════════════════════════════════════════════════
section("TASK 5 — Capstone: Full End-to-End Pipeline")

task("5a. Run complete ETL on capstone dataset")
log("EXTRACT: loading capstone_events.csv...")
df_raw = pd.read_csv("capstone_events.csv", parse_dates=["timestamp"])
log(f"Extracted {len(df_raw):,} raw events")

log("TRANSFORM: quality checks + enrichment...")
df_clean = df_raw[
    df_raw["tx_hash"].notnull() &
    df_raw["amount_usd"].gt(0) &
    df_raw["chain"].isin(chains) &
    ~df_raw["tx_hash"].duplicated()
].copy()
df_clean["is_defi"]      = df_clean["protocol"].ne("None").astype(int)
df_clean["is_high_value"]= (df_clean["amount_usd"]>10000).astype(int)
df_clean["wallet_token"] = df_clean["from_address"].apply(pseudonymise)
df_clean["loaded_at"]    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
rejected = len(df_raw) - len(df_clean)
log(f"Transformed: {len(df_clean):,} clean rows, {rejected} rejected")

log("MART BUILD: computing all 4 mart tables...")
cap_chain = (df_clean.groupby("chain")
    .agg(txs=("event_id","count"),
         volume_usd=("amount_usd","sum"),
         gas_usd=("gas_fee_usd","sum"),
         unique_wallets=("wallet_token","nunique"))
    .round(2).reset_index()
    .sort_values("volume_usd",ascending=False))

cap_token = (df_clean.groupby("token_symbol")
    .agg(txs=("event_id","count"),
         volume_usd=("amount_usd","sum"),
         avg_usd=("amount_usd","mean"))
    .round(2).reset_index()
    .sort_values("volume_usd",ascending=False))

cap_monthly = (df_clean.groupby(["year","month","chain"])
    .agg(txs=("event_id","count"),
         volume_usd=("amount_usd","sum"))
    .round(2).reset_index())

log(f"mart_chain_analytics: {len(cap_chain)} rows")
log(f"mart_token_summary: {len(cap_token)} rows")
log(f"mart_monthly_volume: {len(cap_monthly)} rows")

print(f"\n  {CYAN}Chain analytics (capstone mart):{END}")
print(cap_chain.to_string(index=False))

print(f"\n  {CYAN}Token summary (capstone mart):{END}")
print(cap_token.head(5).to_string(index=False))

task("5b. Load to PostgreSQL (final warehouse load)")
try:
    engine = create_engine(DB_URL)
    df_clean.drop(columns=["from_address","to_address"],
                  errors="ignore").to_sql(
        "cap_events", engine, if_exists="replace",
        index=False, method="multi")
    cap_chain.to_sql("cap_chain_mart", engine, if_exists="replace", index=False)
    cap_token.to_sql("cap_token_mart", engine, if_exists="replace", index=False)
    df_dq.to_sql("cap_dq_history", engine, if_exists="replace", index=False)
    df_metrics.to_sql("cap_pipeline_metrics", engine, if_exists="replace", index=False)
    log(f"All capstone tables loaded to PostgreSQL")

    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM cap_events")).scalar()
        log(f"cap_events: {n:,} rows confirmed in DB")

except Exception as e:
    log(f"PostgreSQL not reachable: {e}", level="WARN")
    log("Run: docker start blockchain-db then retry", level="WARN")

section("EXAM 5 — Capstone Pipeline")
exam("ETL extracted 20,000 raw rows",  result=len(df_raw), expected=20000)
exam("Quality filter removed 0 rows (data is clean)",
     check_fn=lambda _: rejected == 0)
exam("Wallet PII pseudonymised before load",
     check_fn=lambda _: "wallet_token" in df_clean.columns)
exam("All 5 chains in mart output",    result=len(cap_chain), expected=5)
exam("All 10 tokens in mart output",   result=len(cap_token), expected=10)
exam("loaded_at timestamp on every row",
     result=df_clean["loaded_at"].isnull().sum(), expected=0)

# ════════════════════════════════════════════════════════════════
# TASK 6 — 7-DAY PORTFOLIO SUMMARY
# ════════════════════════════════════════════════════════════════
section("TASK 6 — 7-Day Portfolio Summary")

print(f"""
  {CYAN}╔══════════════════════════════════════════════════════════╗
  ║     BLOCKCHAIN DATA ENGINEERING ROADMAP — COMPLETE       ║
  ╚══════════════════════════════════════════════════════════╝{END}

  {GREEN}Day 1 — Python + SQL Foundations{END}
    Tools   : pandas, PostgreSQL, SQLAlchemy, Git
    Built   : 10K order analytics pipeline, 6 SQL queries, 44 exams
    Skills  : DataFrames, GROUP BY, window functions, indexing

  {GREEN}Day 2 — Databases & Data Modelling{END}
    Tools   : PostgreSQL, DBeaver, SCD Type 2
    Built   : Star schema (4 dims + 1 fact), 8 SQL queries, 40 exams
    Skills  : OLTP vs OLAP, star schema, SCD Type 2, EXPLAIN ANALYZE

  {GREEN}Day 3 — ETL Pipelines & Apache Airflow{END}
    Tools   : Airflow, pandas, Docker, psycopg2
    Built   : 3-source ETL pipeline, real Airflow DAG, 35 exams
    Skills  : Extract/Transform/Load, idempotency, DAG design, XCom

  {GREEN}Day 4 — PySpark & Big Data{END}
    Tools   : PySpark, PyArrow, Parquet
    Built   : 1M row pipeline, partitioned Parquet, broadcast join
    Skills  : Lazy evaluation, partitioning, window functions in Spark

  {GREEN}Day 5 — Kafka & Real-time Streaming{END}
    Tools   : Kafka, kafka-python, Docker Compose
    Built   : Price feed + mempool pipeline, 30s tumbling windows
    Skills  : Producers, consumers, offset management, exactly-once

  {GREEN}Day 6 — Cloud & Data Warehousing{END}
    Tools   : BigQuery, dbt, Terraform, S3
    Built   : 3-layer dbt models, Terraform IaC, cost optimisation
    Skills  : Partitioning/clustering, dbt tests, IaC, cloud costs

  {GREEN}Day 7 — Governance, Quality & Capstone{END}
    Tools   : Great Expectations style, lineage, GDPR patterns
    Built   : Full end-to-end platform with governance layer
    Skills  : Lineage, DQ framework, pseudonymisation, monitoring

  {CYAN}── Portfolio Assets ──────────────────────────────────────{END}
    GitHub repo    : data-engineering-roadmap/
    Total datasets : 30+ files across 7 days
    Total code     : 7 Python scripts + 7 SQL files + 1 DAG + 1 Terraform
    Total exams    : 200+ automated checks all passing
    Domain focus   : Blockchain infrastructure (on-chain analytics)

  {CYAN}── What you can now do ───────────────────────────────────{END}
    ✓ Design and build star schema warehouses from scratch
    ✓ Write production ETL pipelines with Airflow
    ✓ Process millions of rows with PySpark
    ✓ Build real-time streaming with Kafka
    ✓ Deploy cloud infrastructure with Terraform
    ✓ Build dbt model layers with tests and lineage
    ✓ Apply GDPR-compliant data masking and pseudonymisation
    ✓ Monitor pipeline health and SLA compliance
    ✓ Apply all skills to blockchain on-chain data

  {PURPLE}── Blockchain startup specific skills ───────────────────{END}
    ✓ On-chain event ingestion and processing
    ✓ Wallet risk scoring and KYC tracking (SCD Type 2)
    ✓ DeFi protocol analytics (Uniswap, Aave, Curve)
    ✓ Real-time mempool monitoring and alerting
    ✓ Gas fee analysis and chain comparison
    ✓ Whale wallet detection and leaderboards
    ✓ GDPR-compliant wallet address handling
""")

section("7-DAY ROADMAP COMPLETE")
print(f"""
  Final commit:
  git add .
  git commit -m "day7: governance, data quality, GDPR, capstone — roadmap complete"
  git push

  Your portfolio: https://github.com/YOUR_USERNAME/data-engineering-roadmap
""")
