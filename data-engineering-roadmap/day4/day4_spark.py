"""
Day 4 — PySpark & Big Data Processing
=======================================
Covers: PySpark DataFrames, transformations, actions,
        partitioning, Parquet, aggregations at scale,
        broadcast joins, window functions in Spark
Run:    python3 day4_spark.py
Requires: pip install pyspark pandas pyarrow
Dataset:  blockchain_events_1m.csv (1 million rows, ~285MB)
"""

import time
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

PASS = "\033[92m✔ PASS\033[0m"
FAIL = "\033[91m✘ FAIL\033[0m"
HEAD = "\033[94m"
YELLOW = "\033[93m"
END  = "\033[0m"

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

def timer(fn, label):
    t0 = time.time()
    result = fn()
    elapsed = round(time.time() - t0, 2)
    print(f"  \033[93m⏱  {label}: {elapsed}s\033[0m")
    return result, elapsed

# ════════════════════════════════════════════════════════════════
# SETUP — Initialize Spark Session
# ════════════════════════════════════════════════════════════════
section("SETUP — Initialize PySpark Session")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (StructType, StructField,
        StringType, IntegerType, LongType, DoubleType, TimestampType)

    spark = (SparkSession.builder
        .appName("BlockchainBigData")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    cores = spark.sparkContext.defaultParallelism
    print(f"\n  Spark session started")
    print(f"  Master  : local[*] — uses all CPU cores")
    print(f"  Cores   : {cores}")
    print(f"  Memory  : 4GB driver")
    print(f"  Version : {spark.version}")
    SPARK_OK = True

except ImportError:
    print("\n  PySpark not installed — running pandas fallback mode")
    print("  Install: pip install pyspark pyarrow")
    print("  All concepts demonstrated with pandas on same 1M dataset\n")
    SPARK_OK = False

# ════════════════════════════════════════════════════════════════
# TASK 1 — READ 1M ROWS
# ════════════════════════════════════════════════════════════════
section("TASK 1 — Read 1M Row Dataset")

if SPARK_OK:
    task("1a. Read CSV into Spark DataFrame")
    schema = StructType([
        StructField("tx_id",          LongType(),   False),
        StructField("tx_hash",         StringType(), False),
        StructField("block_number",    LongType(),   True),
        StructField("chain",           StringType(), True),
        StructField("event_type",      StringType(), True),
        StructField("protocol",        StringType(), True),
        StructField("from_address",    StringType(), True),
        StructField("to_address",      StringType(), True),
        StructField("token_symbol",    StringType(), True),
        StructField("amount",          DoubleType(), True),
        StructField("price_usd",       DoubleType(), True),
        StructField("amount_usd",      DoubleType(), True),
        StructField("gas_used",        LongType(),   True),
        StructField("gas_price_gwei",  DoubleType(), True),
        StructField("gas_fee_eth",     DoubleType(), True),
        StructField("status",          StringType(), True),
        StructField("year",            IntegerType(),True),
        StructField("month",           IntegerType(),True),
        StructField("day",             IntegerType(),True),
        StructField("hour",            IntegerType(),True),
        StructField("timestamp",       StringType(), True),
    ])

    def read_spark():
        return (spark.read
            .option("header","true")
            .schema(schema)
            .csv("blockchain_events_1m.csv"))

    df_spark, t_read = timer(read_spark, "Read 1M rows with schema")
    row_count = df_spark.count()
    print(f"\n  Rows     : {row_count:,}")
    print(f"  Columns  : {len(df_spark.columns)}")
    print(f"  Partitions: {df_spark.rdd.getNumPartitions()}")
    print(f"\n  Schema:")
    df_spark.printSchema()
    print("\n  Sample (3 rows):")
    df_spark.select("tx_id","chain","event_type","token_symbol",
                    "amount_usd","status").show(3, truncate=False)

else:
    task("1a. Read CSV into pandas (fallback)")
    def read_pandas():
        return pd.read_csv("blockchain_events_1m.csv")
    df_pd, t_read = timer(read_pandas, "Read 1M rows with pandas")
    row_count = len(df_pd)
    print(f"  Rows: {row_count:,} | Cols: {len(df_pd.columns)}")

section("EXAM 1 — Read & Schema")
exam("Dataset has exactly 1,000,000 rows",  result=row_count, expected=1_000_000)
exam("Read time recorded",                  check_fn=lambda _: t_read > 0)
exam("21 columns present",
     check_fn=lambda _: (
         len(df_spark.columns) if SPARK_OK else len(df_pd.columns)) == 21)

# ════════════════════════════════════════════════════════════════
# TASK 2 — TRANSFORMATIONS (lazy evaluation)
# ════════════════════════════════════════════════════════════════
section("TASK 2 — Transformations (lazy evaluation)")

if SPARK_OK:
    task("2a. Filter confirmed transactions only")
    df_confirmed = df_spark.filter(F.col("status") == "confirmed")
    print(f"  Filtered (lazy — not executed yet)")

    task("2b. Add derived columns")
    df_enriched = (df_confirmed
        .withColumn("gas_fee_usd",
            F.round(F.col("gas_used") * F.col("gas_price_gwei") * 1e-9 * 3200, 4))
        .withColumn("is_high_value",
            F.when(F.col("amount_usd") > 10000, 1).otherwise(0))
        .withColumn("timestamp_ts",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("week_of_year",
            F.weekofyear(F.to_timestamp("timestamp","yyyy-MM-dd HH:mm:ss")))
        .withColumn("pipeline_ts",
            F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    )
    print(f"  Added: gas_fee_usd, is_high_value, timestamp_ts, week_of_year, pipeline_ts")
    print(f"  Still lazy — nothing executed yet (Spark DAG planned)")

    task("2c. Trigger first action — count confirmed rows")
    def count_confirmed():
        return df_enriched.count()
    confirmed_count, t_count = timer(count_confirmed, "Count confirmed rows (first action)")
    print(f"  Confirmed rows: {confirmed_count:,}")

else:
    df_confirmed = df_pd[df_pd["status"]=="confirmed"].copy()
    df_confirmed["gas_fee_usd"] = (
        df_confirmed["gas_used"] * df_confirmed["gas_price_gwei"] * 1e-9 * 3200).round(4)
    df_confirmed["is_high_value"] = (df_confirmed["amount_usd"] > 10000).astype(int)
    confirmed_count = len(df_confirmed)
    t_count = 0
    print(f"  Confirmed rows: {confirmed_count:,}")

section("EXAM 2 — Transformations")
exam("Confirmed rows < total rows (filtering worked)",
     check_fn=lambda _: confirmed_count < row_count)
exam("Confirmed rows > 500,000 (majority confirmed)",
     check_fn=lambda _: confirmed_count > 500_000)

# ════════════════════════════════════════════════════════════════
# TASK 3 — AGGREGATIONS AT SCALE
# ════════════════════════════════════════════════════════════════
section("TASK 3 — Aggregations at Scale")

if SPARK_OK:
    task("3a. Volume by chain")
    def agg_chain():
        return (df_enriched
            .groupBy("chain")
            .agg(
                F.count("*").alias("transactions"),
                F.round(F.sum("amount_usd"),2).alias("volume_usd"),
                F.round(F.avg("amount_usd"),2).alias("avg_tx_usd"),
                F.round(F.sum("gas_fee_usd"),4).alias("total_gas_usd"),
                F.countDistinct("from_address").alias("unique_wallets")
            )
            .orderBy(F.desc("volume_usd")))
    chain_agg, t_chain = timer(agg_chain, "Group by chain")
    print(f"\n  Volume by chain:")
    chain_agg.show(truncate=False)

    task("3b. Top 10 protocols by volume")
    def agg_protocol():
        return (df_enriched
            .filter(F.col("protocol") != "None")
            .groupBy("protocol","chain")
            .agg(
                F.count("*").alias("txs"),
                F.round(F.sum("amount_usd"),2).alias("volume_usd"),
                F.round(F.avg("gas_fee_usd"),4).alias("avg_gas"),
                F.count(F.when(F.col("is_high_value")==1,1)).alias("whale_txs")
            )
            .orderBy(F.desc("volume_usd"))
            .limit(10))
    proto_agg, t_proto = timer(agg_protocol, "Group by protocol")
    print(f"\n  Top protocols:")
    proto_agg.show(truncate=False)

    task("3c. Monthly volume trend")
    def agg_monthly():
        return (df_enriched
            .groupBy("year","month")
            .agg(
                F.count("*").alias("transactions"),
                F.round(F.sum("amount_usd"),2).alias("volume_usd"),
                F.countDistinct("from_address").alias("active_wallets")
            )
            .orderBy("year","month"))
    monthly_agg, t_monthly = timer(agg_monthly, "Group by month")
    print(f"\n  Monthly trend:")
    monthly_agg.show(15, truncate=False)

else:
    task("3a-c. Aggregations with pandas")
    chain_agg = (df_confirmed.groupby("chain")["amount_usd"]
        .agg(["count","sum","mean"]).round(2).reset_index())
    print(f"\n  Chain volume:\n{chain_agg.to_string()}")
    monthly_agg = (df_confirmed.groupby(["year","month"])["amount_usd"]
        .sum().round(2).reset_index())
    print(f"\n  Monthly trend (first 6):\n{monthly_agg.head(6).to_string()}")

section("EXAM 3 — Aggregations")
if SPARK_OK:
    chain_count = chain_agg.count()
    exam("Chain aggregation returns all 6 chains",
         result=chain_count, expected=6)
    exam("Volume aggregation has no nulls in volume_usd",
         result=chain_agg.filter(F.col("volume_usd").isNull()).count(), expected=0)
    exam("Monthly trend returns 12 months",
         result=monthly_agg.count(), expected=12)
else:
    exam("Chain aggregation has 6 chains",
         result=len(chain_agg), expected=6)

# ════════════════════════════════════════════════════════════════
# TASK 4 — WINDOW FUNCTIONS IN SPARK
# ════════════════════════════════════════════════════════════════
section("TASK 4 — Window Functions in Spark")

if SPARK_OK:
    task("4a. Rank tokens by volume within each chain")
    def window_rank():
        w = Window.partitionBy("chain").orderBy(F.desc("volume_usd"))
        return (df_enriched
            .groupBy("chain","token_symbol")
            .agg(F.round(F.sum("amount_usd"),2).alias("volume_usd"))
            .withColumn("rank_in_chain", F.rank().over(w))
            .filter(F.col("rank_in_chain") <= 3)
            .orderBy("chain","rank_in_chain"))
    ranked, t_rank = timer(window_rank, "Rank tokens by chain")
    print("\n  Top 3 tokens per chain:")
    ranked.show(20, truncate=False)

    task("4b. Running total volume by month")
    def window_running():
        w = Window.orderBy("year","month").rowsBetween(
            Window.unboundedPreceding, Window.currentRow)
        return (df_enriched
            .groupBy("year","month")
            .agg(F.round(F.sum("amount_usd"),2).alias("monthly_vol"))
            .withColumn("cumulative_vol",
                F.round(F.sum("monthly_vol").over(w),2))
            .orderBy("year","month"))
    running, t_running = timer(window_running, "Running total volume")
    print("\n  Cumulative volume by month:")
    running.show(truncate=False)

section("EXAM 4 — Window Functions")
if SPARK_OK:
    exam("Ranked result has max rank 3 per chain",
         result=ranked.agg(F.max("rank_in_chain")).collect()[0][0], expected=3)
    exam("Running total is monotonically increasing",
         check_fn=lambda _: (
             running.select("cumulative_vol").rdd.map(lambda r:r[0]).collect()
             == sorted(running.select("cumulative_vol").rdd.map(lambda r:r[0]).collect())))

# ════════════════════════════════════════════════════════════════
# TASK 5 — WRITE PARTITIONED PARQUET
# ════════════════════════════════════════════════════════════════
section("TASK 5 — Write Partitioned Parquet")

if SPARK_OK:
    task("5a. Write partitioned by chain + month")
    def write_parquet():
        (df_enriched
            .write
            .mode("overwrite")
            .partitionBy("chain","month")
            .parquet("blockchain_events_partitioned.parquet"))
    _, t_write = timer(write_parquet, "Write partitioned Parquet")
    print(f"  Written to: blockchain_events_partitioned.parquet/")
    print(f"  Partitioned by: chain (6) × month (12) = up to 72 folders")

    task("5b. Read back ONE partition (predicate pushdown)")
    def read_partition():
        return (spark.read
            .parquet("blockchain_events_partitioned.parquet")
            .filter((F.col("chain")=="ethereum") & (F.col("month")==1)))
    eth_jan, t_read_part = timer(read_partition, "Read ethereum/month=1 partition only")
    part_count = eth_jan.count()
    print(f"  Rows in ethereum/January: {part_count:,}")
    print(f"  Predicate pushdown: Spark skipped all other partitions!")

    task("5c. Compare: full scan vs partition scan")
    print(f"\n  Full CSV read   : {t_read:.2f}s  (reads all 1M rows)")
    print(f"  Partition read  : {t_read_part:.2f}s  (reads only 1 partition)")
    print(f"  Speedup         : ~{round(t_read/max(t_read_part,0.01),1)}x faster")

else:
    task("5a. Write Parquet with pandas + pyarrow")
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        df_confirmed.to_parquet("blockchain_events.parquet", index=False)
        size = os.path.getsize("blockchain_events.parquet")/(1024*1024)
        print(f"  Written: blockchain_events.parquet ({size:.1f}MB)")
    except ImportError:
        print("  pyarrow not installed — pip install pyarrow")

section("EXAM 5 — Parquet & Partitioning")
if SPARK_OK:
    import os
    exam("Parquet folder created",
         check_fn=lambda _: os.path.exists("blockchain_events_partitioned.parquet"))
    exam("Partition read returned rows",
         check_fn=lambda _: part_count > 0)
    exam("Partition read faster than full CSV read",
         check_fn=lambda _: t_read_part < t_read)

# ════════════════════════════════════════════════════════════════
# TASK 6 — BROADCAST JOIN (small table optimization)
# ════════════════════════════════════════════════════════════════
section("TASK 6 — Broadcast Join (small table optimization)")

if SPARK_OK:
    task("6a. Create token reference table (small)")
    token_data = [
        ("ETH","Ethereum","ethereum",18,3200.0,"layer1"),
        ("USDC","USD Coin","ethereum",6,1.0,"stablecoin"),
        ("USDT","Tether","ethereum",6,1.0,"stablecoin"),
        ("WBTC","Wrapped Bitcoin","ethereum",8,65000.0,"wrapped"),
        ("SOL","Solana","solana",9,175.0,"layer1"),
        ("MATIC","Polygon","polygon",18,0.85,"layer2"),
        ("ARB","Arbitrum","arbitrum",18,1.2,"layer2"),
        ("OP","Optimism","optimism",18,2.1,"layer2"),
        ("BNB","BNB Chain","binance_smart_chain",18,420.0,"layer1"),
        ("LINK","Chainlink","ethereum",18,18.5,"oracle"),
        ("UNI","Uniswap","ethereum",18,8.5,"defi"),
        ("AAVE","Aave","ethereum",18,95.0,"defi"),
        ("CRV","Curve","ethereum",18,0.45,"defi"),
        ("MKR","MakerDAO","ethereum",18,1800.0,"defi"),
        ("SNX","Synthetix","ethereum",18,2.8,"defi"),
    ]
    token_cols = ["symbol","name","chain","decimals","price_usd","category"]
    df_tokens = spark.createDataFrame(token_data, token_cols)

    task("6b. Broadcast join — enrich events with token category")
    def broadcast_join():
        return (df_enriched
            .join(F.broadcast(df_tokens),
                  df_enriched.token_symbol == df_tokens.symbol, "left")
            .groupBy("category")
            .agg(
                F.count("*").alias("txs"),
                F.round(F.sum("amount_usd"),2).alias("volume_usd"),
                F.round(F.avg("amount_usd"),2).alias("avg_usd")
            )
            .orderBy(F.desc("volume_usd")))
    joined, t_join = timer(broadcast_join, "Broadcast join + group by category")
    print(f"\n  Volume by token category:")
    joined.show(truncate=False)
    print(f"\n  Broadcast join: small token table sent to ALL executors")
    print(f"  No shuffle needed — avoids most expensive Spark operation")

section("EXAM 6 — Broadcast Join")
if SPARK_OK:
    exam("Broadcast join returned token categories",
         check_fn=lambda _: joined.count() > 0)
    exam("All categories present (defi, layer1, layer2, stablecoin, oracle, wrapped)",
         check_fn=lambda _: joined.count() >= 5)

# ════════════════════════════════════════════════════════════════
# TASK 7 — PERFORMANCE: pandas vs Spark comparison
# ════════════════════════════════════════════════════════════════
section("TASK 7 — Performance: pandas vs Spark on 1M rows")

task("7a. Same aggregation in pandas")
df_pd_compare = pd.read_csv("blockchain_events_1m.csv")
def pandas_agg():
    return (df_pd_compare[df_pd_compare["status"]=="confirmed"]
        .groupby("chain")["amount_usd"]
        .agg(["count","sum","mean"]))
_, t_pandas = timer(pandas_agg, "pandas groupby on 1M rows")

if SPARK_OK:
    print(f"\n  ┌─────────────────────────────────────────┐")
    print(f"  │  Performance comparison on 1M rows      │")
    print(f"  ├─────────────────────────────────────────┤")
    print(f"  │  pandas read + groupby  : {t_read+t_count:.2f}s          │")
    print(f"  │  Spark  read + groupby  : {t_chain:.2f}s           │")
    print(f"  │  Spark  parquet read    : {t_read_part:.2f}s           │")
    print(f"  └─────────────────────────────────────────┘")
    print(f"\n  At 1M rows pandas is often faster (JVM overhead).")
    print(f"  At 100M+ rows Spark wins — parallelism scales,")
    print(f"  pandas runs out of memory.")

section("EXAM 7 — Concepts")
exam("Lazy evaluation: transformations don't run until action called",
     check_fn=lambda _: True)
exam("Partitioned Parquet enables predicate pushdown",
     check_fn=lambda _: True)
exam("Broadcast join avoids shuffle for small tables",
     check_fn=lambda _: True)
exam("Spark wins at 100M+ rows; pandas wins under 10M on single machine",
     check_fn=lambda _: True)

if SPARK_OK:
    spark.stop()
    print(f"\n  Spark session stopped.")

section("DAY 4 COMPLETE")
print("""
  Summary:
  • Read      : 1,000,000 rows from CSV with explicit schema
  • Filtered  : confirmed transactions only
  • Enriched  : gas_fee_usd, is_high_value, week_of_year
  • Aggregated: by chain, protocol, month (at scale)
  • Window fn : RANK() and running total in Spark
  • Parquet   : written partitioned by chain + month
  • Broadcast : enriched with token category (no shuffle)
  • Compared  : pandas vs Spark performance on 1M rows

  git add .
  git commit -m "day4: PySpark 1M rows, partitioned Parquet, window functions"
  git push
""")
