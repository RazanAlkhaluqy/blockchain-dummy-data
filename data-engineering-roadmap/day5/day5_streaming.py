"""
Day 5 — Kafka & Real-time Streaming
======================================
Covers: Kafka concepts, producers, consumers, streaming windows,
        exactly-once semantics, offset management, real-time
        analytics on DeFi price feed + mempool events
Run:    python3 day5_streaming.py
Requires: pip install pandas kafka-python psycopg2-binary sqlalchemy
Kafka:    docker-compose up -d  (see README_day5.md)
"""

import pandas as pd
import json, time, threading, queue
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings("ignore")

PASS   = "\033[92m✔ PASS\033[0m"
FAIL   = "\033[91m✘ FAIL\033[0m"
HEAD   = "\033[94m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
RED    = "\033[91m"
END    = "\033[0m"

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
    colors = {"INFO":GREEN,"WARN":YELLOW,"ERROR":RED,"KAFKA":"\033[96m"}
    c = colors.get(level, END)
    print(f"  {c}[{level}]{END} {ts} — {msg}")

# ════════════════════════════════════════════════════════════════
# SECTION 0 — Kafka concept explainer (always runs)
# ════════════════════════════════════════════════════════════════
section("KAFKA CONCEPTS — Before we write any code")

print("""
  Kafka is a distributed message streaming platform.
  Think of it as a very fast, persistent, ordered log file
  that multiple producers write to and multiple consumers read from.

  Core components:
  ┌─────────────────────────────────────────────────────────────┐
  │                                                             │
  │  PRODUCER          KAFKA BROKER          CONSUMER          │
  │  (your ETL)  ───►  Topic: defi-prices ──► (your app)      │
  │  (Ethereum   ───►  Topic: mempool     ──► (analytics)     │
  │   node)      ───►  Topic: alerts      ──► (alerting)      │
  │                                                             │
  │  Each topic is split into PARTITIONS (for parallelism)     │
  │  Each message has an OFFSET (its position in the log)      │
  │  Consumers track their offset — can replay from any point  │
  │                                                             │
  └─────────────────────────────────────────────────────────────┘

  Key guarantees:
  • At-least-once : message delivered ≥1 times (default)
  • Exactly-once  : message delivered exactly once (transactions)
  • Messages kept : configurable retention (default 7 days)
  • Order         : guaranteed within a partition, not across

  Why blockchain needs Kafka:
  • Ethereum produces ~1 block every 12 seconds = ~200 txs/block
  • Peak DeFi: 100,000+ events per minute
  • You need to process these in real time without losing any
  • Kafka buffers the stream so your consumer can fall behind
    and catch up — it won't miss messages
""")

# ════════════════════════════════════════════════════════════════
# TASK 1 — LOAD & UNDERSTAND STREAMING DATA
# ════════════════════════════════════════════════════════════════
section("TASK 1 — Load & Understand Streaming Data Sources")

task("1a. Load DeFi price feed (simulates Kafka topic: defi-prices)")
with open("defi_price_feed.json") as f:
    price_msgs = json.load(f)
df_prices = pd.DataFrame(price_msgs)
df_prices["timestamp"] = pd.to_datetime(df_prices["timestamp"])
log(f"Loaded {len(df_prices):,} price messages")
log(f"Time range: {df_prices['timestamp'].min()} → {df_prices['timestamp'].max()}")
log(f"Tokens: {sorted(df_prices['token'].unique().tolist())}")
log(f"Exchanges: {df_prices['exchange'].unique().tolist()}")
print(f"\n  Sample messages:")
print(df_prices[["msg_id","token","exchange","mid_price","timestamp"]].head(5).to_string())

task("1b. Load mempool events (simulates Kafka topic: mempool-events)")
with open("mempool_stream.json") as f:
    mempool_msgs = json.load(f)
df_mempool = pd.DataFrame(mempool_msgs)
df_mempool["timestamp"] = pd.to_datetime(df_mempool["timestamp"])
log(f"Loaded {len(df_mempool):,} mempool messages")
log(f"Status breakdown:")
print(f"\n{df_mempool['status'].value_counts().to_string()}")

task("1c. Load Kafka consumer offsets (monitoring)")
df_offsets = pd.read_csv("kafka_consumer_offsets.csv")
log(f"Loaded {len(df_offsets):,} offset records across {df_offsets['topic'].nunique()} topics")
print(f"\n  Consumer lag by topic:")
print(df_offsets.groupby("topic")["lag"].agg(["mean","max"]).round(1).to_string())

section("EXAM 1 — Data Loading")
exam("Price feed has 50,000 messages",
     result=len(df_prices), expected=50000)
exam("Mempool has 30,000 messages",
     result=len(df_mempool), expected=30000)
exam("All 10 tokens present in price feed",
     result=df_prices["token"].nunique(), expected=10)
exam("Timestamps are ordered (messages arrive in order)",
     check_fn=lambda _: df_prices["timestamp"].is_monotonic_increasing)
exam("msg_id is unique (no duplicate messages — exactly-once check)",
     result=df_prices["msg_id"].duplicated().sum(), expected=0)
exam("Mempool has pending, confirmed, failed statuses",
     check_fn=lambda _: {"pending","confirmed","failed"}.issubset(
         df_mempool["status"].unique()))
exam("No null timestamps in price feed",
     result=df_prices["timestamp"].isnull().sum(), expected=0)

# ════════════════════════════════════════════════════════════════
# TASK 2 — SIMULATE KAFKA PRODUCER
# ════════════════════════════════════════════════════════════════
section("TASK 2 — Kafka Producer Simulation")

task("2a. Simulate message production at 1000 msg/sec")

class KafkaProducerSim:
    """Simulates a Kafka producer publishing to a topic"""
    def __init__(self, topic, bootstrap_servers="localhost:9092"):
        self.topic = topic
        self.servers = bootstrap_servers
        self.sent = 0
        self.errors = 0
        self._buffer = queue.Queue(maxsize=10000)

    def produce(self, key, value, partition=None):
        """Publish a message — non-blocking"""
        msg = {
            "topic":    self.topic,
            "key":      key,
            "value":    value,
            "offset":   self.sent,
            "partition": partition or (self.sent % 8),
            "timestamp": datetime.now().isoformat()
        }
        try:
            self._buffer.put_nowait(msg)
            self.sent += 1
        except queue.Full:
            self.errors += 1

    def flush(self):
        """Wait for all messages to be acknowledged"""
        return self.sent

producer = KafkaProducerSim(topic="defi-prices")

log("Starting producer — publishing DeFi prices...")
t0 = time.time()
for i, msg in enumerate(price_msgs[:10000]):
    producer.produce(
        key=msg["token"],
        value=json.dumps(msg),
        partition=hash(msg["token"]) % 8
    )
elapsed = time.time() - t0
rate = round(10000 / elapsed)
log(f"Published 10,000 messages in {elapsed:.2f}s ({rate:,} msg/sec)")
log(f"Producer stats: sent={producer.sent:,}, errors={producer.errors}")
log(f"Messages distributed across 8 partitions (key=token_symbol)")

print(f"""
  Producer key concept — partitioning by token:
  • ETH messages   → partition 0  (all ETH prices together)
  • BTC messages   → partition 1  (all BTC prices together)
  • SOL messages   → partition 2  ...
  → Same token always goes to same partition = ORDER PRESERVED
  → Different tokens processed in PARALLEL across partitions
""")

section("EXAM 2 — Producer")
exam("Producer sent 10,000 messages",
     result=producer.sent, expected=10000)
exam("Producer had zero errors",
     result=producer.errors, expected=0)
exam("Throughput > 5,000 msg/sec",
     check_fn=lambda _: rate > 5000)

# ════════════════════════════════════════════════════════════════
# TASK 3 — SIMULATE KAFKA CONSUMER + OFFSET MANAGEMENT
# ════════════════════════════════════════════════════════════════
section("TASK 3 — Kafka Consumer + Offset Management")

task("3a. Consumer with manual offset commit")

class KafkaConsumerSim:
    """Simulates a Kafka consumer with offset tracking"""
    def __init__(self, topic, group_id, auto_commit=False):
        self.topic    = topic
        self.group_id = group_id
        self.auto_commit = auto_commit
        self.offsets  = defaultdict(int)   # partition → last offset
        self.consumed = 0
        self.processed = []

    def poll(self, messages, batch_size=100):
        """Poll for next batch of messages"""
        start = sum(self.offsets.values())
        batch = messages[start:start+batch_size]
        return batch

    def commit(self, partition, offset):
        """Manually commit offset after successful processing"""
        self.offsets[partition] = offset
        self.consumed += 1

    def lag(self, total_messages):
        """How far behind is this consumer?"""
        committed = sum(self.offsets.values())
        return total_messages - committed

consumer = KafkaConsumerSim(
    topic="defi-prices",
    group_id="blockchain-analytics-cg",
    auto_commit=False  # manual commit = exactly-once guarantee
)

log("Consumer starting — manual offset commit mode")
log("(auto_commit=False gives us exactly-once processing)")

processed_batches = 0
total_processed   = 0
all_messages_copy = price_msgs[:5000]

while total_processed < len(all_messages_copy):
    batch = consumer.poll(all_messages_copy, batch_size=500)
    if not batch: break

    # process batch
    for msg in batch:
        consumer.processed.append(msg)
        total_processed += 1

    # commit only AFTER successful processing
    last_msg = batch[-1]
    partition = last_msg["msg_id"] % 8
    consumer.commit(partition=partition, offset=last_msg["msg_id"])
    processed_batches += 1

lag = consumer.lag(len(all_messages_copy))
log(f"Consumed {total_processed:,} messages in {processed_batches} batches")
log(f"Consumer lag: {lag} messages (0 = fully caught up)")
log(f"Offsets committed per partition: {dict(list(consumer.offsets.items())[:4])}...")

print(f"""
  Offset management — why it matters:
  • auto_commit=True  → fast but may LOSE messages on crash
    (committed before processing finished)
  • auto_commit=False → commit only after processing = SAFE
    (if crash happens, re-read from last committed offset)

  Exactly-once pattern (Day 6 production):
  1. Poll messages
  2. Process + write to DB in a TRANSACTION
  3. Commit Kafka offset inside same transaction
  → Either both succeed or both rollback — no duplicates
""")

section("EXAM 3 — Consumer & Offsets")
exam("Consumer processed all 5,000 messages",
     result=total_processed, expected=5000)
exam("Consumer lag is 0 (fully caught up)",
     result=lag, expected=0)
exam("Offset committed for each partition",
     check_fn=lambda _: len(consumer.offsets) > 0)
exam("Processed messages match input (no loss)",
     result=len(consumer.processed), expected=5000)

# ════════════════════════════════════════════════════════════════
# TASK 4 — STREAM PROCESSING: TUMBLING WINDOWS
# ════════════════════════════════════════════════════════════════
section("TASK 4 — Stream Processing: Tumbling Windows")

task("4a. 30-second tumbling window — avg price per token")
print("""
  Window types:
  ┌──────────────────────────────────────────────────────────┐
  │ TUMBLING  [0-30s][30-60s][60-90s]  non-overlapping      │
  │ SLIDING   [0-30s]                                        │
  │             [15-45s]               overlapping           │
  │                [30-60s]                                  │
  │ SESSION   [─────────][──────]      gap-based             │
  └──────────────────────────────────────────────────────────┘
  We use TUMBLING windows — standard for price aggregation
""")

df_p = df_prices.copy()
df_p["window"] = df_p["timestamp"].dt.floor("30s")

tumbling = (df_p.groupby(["window","token","exchange"])
    .agg(
        msg_count     = ("msg_id","count"),
        avg_price     = ("mid_price","mean"),
        min_price     = ("mid_price","min"),
        max_price     = ("mid_price","max"),
        total_volume  = ("volume_24h","mean"),
        avg_latency   = ("latency_ms","mean"),
    )
    .round(4)
    .reset_index()
    .sort_values(["window","token"]))

log(f"30s windows computed: {len(tumbling):,} window-token-exchange combinations")
print(f"\n  Sample windows (ETH only):")
eth_windows = tumbling[tumbling["token"]=="ETH"].head(8)
print(eth_windows[["window","exchange","avg_price","min_price","max_price","msg_count"]].to_string())

task("4b. Detect price anomalies — spike > 2% in one window")
token_avg = df_p.groupby("token")["mid_price"].mean()
df_p["token_avg"] = df_p["token"].map(token_avg)
df_p["pct_from_avg"] = ((df_p["mid_price"] - df_p["token_avg"])
                        / df_p["token_avg"] * 100).round(4)
anomalies = df_p[df_p["pct_from_avg"].abs() > 2].copy()
log(f"Anomalies detected: {len(anomalies):,} price spikes > 2% from average")
print(f"\n  Top anomalies:")
print(anomalies[["token","exchange","mid_price","pct_from_avg","timestamp"]]
      .sort_values("pct_from_avg", ascending=False)
      .head(5).to_string())

task("4c. 5-minute sliding window — rolling avg (watermarking concept)")
df_p_sorted = df_p.sort_values("timestamp").set_index("timestamp")
sliding = (df_p_sorted.groupby("token")["mid_price"]
    .rolling("5min", min_periods=1)
    .mean()
    .round(6)
    .reset_index())
sliding.columns = ["token","timestamp","rolling_5m_avg"]
log(f"5-min rolling average computed for all tokens")
print(f"\n  Rolling 5m avg sample (ETH):")
print(sliding[sliding["token"]=="ETH"].head(5).to_string())

section("EXAM 4 — Windowing")
exam("30s windows computed with correct columns",
     check_fn=lambda _: {"window","token","avg_price","msg_count"}.issubset(tumbling.columns))
exam("Anomaly detection found price spikes",
     check_fn=lambda _: len(anomalies) > 0)
exam("Rolling avg has one row per price message",
     result=len(sliding), expected=len(df_p))
exam("All 10 tokens in tumbling windows",
     result=tumbling["token"].nunique(), expected=10)
exam("No future timestamps (watermark concept: late data rejected)",
     check_fn=lambda _: (df_p["timestamp"] <= pd.Timestamp("2024-12-31 23:59:59", tz="UTC")).all())

# ════════════════════════════════════════════════════════════════
# TASK 5 — MEMPOOL STREAM PROCESSING
# ════════════════════════════════════════════════════════════════
section("TASK 5 — Mempool Event Processing")

task("5a. Real-time pending → confirmed transition tracking")
df_m = df_mempool.copy()

pending   = df_m[df_m["status"]=="pending"]
confirmed = df_m[df_m["status"]=="confirmed"]
failed    = df_m[df_m["status"]=="failed"]

log(f"Pending   : {len(pending):,} transactions")
log(f"Confirmed : {len(confirmed):,} transactions")
log(f"Failed    : {len(failed):,} transactions")

confirm_rate = round(len(confirmed)/len(df_m)*100, 1)
log(f"Confirmation rate: {confirm_rate}%")

task("5b. High-value transaction alerting (real-time pattern)")
HIGH_VALUE_THRESHOLD = 50000
high_value = confirmed[confirmed["amount_usd"] > HIGH_VALUE_THRESHOLD]
log(f"HIGH VALUE ALERTS: {len(high_value)} transactions > ${HIGH_VALUE_THRESHOLD:,}")
print(f"\n  Top high-value confirmed transactions:")
print(high_value[["tx_hash","token","amount_usd","protocol","timestamp"]]
      .sort_values("amount_usd", ascending=False)
      .head(5).to_string())

task("5c. Gas price spike detection (MEV bot indicator)")
gas_mean = df_m["gas_price_gwei"].mean()
gas_std  = df_m["gas_price_gwei"].std()
gas_spike_threshold = gas_mean + 2 * gas_std
gas_spikes = df_m[df_m["gas_price_gwei"] > gas_spike_threshold]
log(f"Gas mean: {gas_mean:.1f} gwei | Std: {gas_std:.1f} | Spike threshold: {gas_spike_threshold:.1f}")
log(f"Gas spikes detected: {len(gas_spikes):,} (potential MEV bot activity)")

task("5d. Per-protocol streaming volume (1-minute buckets)")
df_m["minute"] = df_m["timestamp"].dt.floor("1min")


confirmed = df_m[df_m["status"]=="confirmed"]

protocol_stream = (confirmed[confirmed["protocol"]!="None"]
    .groupby(["minute","protocol"])
    .agg(txs=("tx_hash","count"),
         volume_usd=("amount_usd","sum"))
    .round(2).reset_index()
    .sort_values(["minute","volume_usd"], ascending=[True,False]))
log(f"Protocol stream: {len(protocol_stream)} minute-protocol buckets")
print(f"\n  Protocol volume (first 10 buckets):")
print(protocol_stream.head(10).to_string())

section("EXAM 5 — Mempool Processing")
exam("Confirmation rate is between 20-50%",
     check_fn=lambda _: 20 < confirm_rate < 50)
exam("High-value alerts detected",
     check_fn=lambda _: len(high_value) > 0)
exam("Gas spike detection found outliers",
     check_fn=lambda _: len(gas_spikes) > 0)
exam("Protocol stream has minute-level granularity",
     check_fn=lambda _: "minute" in protocol_stream.columns)
exam("No duplicate tx_hashes in confirmed (exactly-once)",
     result=confirmed["tx_hash"].duplicated().sum(), expected=0)

# ════════════════════════════════════════════════════════════════
# TASK 6 — STREAM RESULTS ANALYSIS
# ════════════════════════════════════════════════════════════════
section("TASK 6 — Analyze Pre-computed Stream Window Results")

task("6a. Load and analyze stream_window_results.csv")
df_windows = pd.read_csv("stream_window_results.csv",
                          parse_dates=["window_start","window_end"])
log(f"Loaded {len(df_windows):,} 30-second windows")

print(f"\n  Alerts triggered by token:")
alert_summary = (df_windows[df_windows["alert_triggered"]==1]
    .groupby("token")
    .agg(alerts=("alert_triggered","sum"),
         avg_price_change=("price_change_pct","mean"))
    .round(4).sort_values("alerts",ascending=False))
print(alert_summary.to_string())

task("6b. Consumer lag analysis from offsets")
df_off = pd.read_csv("kafka_consumer_offsets.csv")
print(f"\n  Consumer lag by topic:")
lag_summary = (df_off.groupby("topic")["lag"]
    .agg(["mean","max","min"])
    .round(1))
print(lag_summary.to_string())

high_lag = df_off[df_off["lag"] > 100]
log(f"High lag events (>100): {len(high_lag)} — consumer falling behind!")

section("EXAM 6 — Stream Results")
exam("Window results have 2,000 rows",
     result=len(df_windows), expected=2000)
exam("Alert rate < 30% (not too many false positives)",
     check_fn=lambda _: df_windows["alert_triggered"].mean() < 0.3)
exam("All tokens appear in window results",
     result=df_windows["token"].nunique(), expected=10)
exam("Price change pct has both positive and negative values",
     check_fn=lambda _: (
         df_windows["price_change_pct"].min() < 0 and
         df_windows["price_change_pct"].max() > 0))

# ════════════════════════════════════════════════════════════════
# TASK 7 — POSTGRESQL: PERSIST STREAM RESULTS
# ════════════════════════════════════════════════════════════════
section("TASK 7 — Persist Stream Results to PostgreSQL")

from sqlalchemy import create_engine, text
DB_URL = "postgresql://postgres:password@localhost:5432/blockchain_db"

try:
    engine = create_engine(DB_URL)

    log("Loading stream results to PostgreSQL...")
    tumbling_load = tumbling.copy()
    tumbling_load["window"] = tumbling_load["window"].astype(str)
    tumbling_load.to_sql("stream_price_windows", engine,
                          if_exists="replace", index=False)
    log(f"stream_price_windows: {len(tumbling_load):,} rows loaded")

    df_windows.to_sql("stream_alert_windows", engine,
                       if_exists="replace", index=False)
    log(f"stream_alert_windows: {len(df_windows):,} rows loaded")

    confirmed.to_sql("stream_confirmed_txs", engine,
                     if_exists="replace", index=False, method="multi")
    log(f"stream_confirmed_txs: {len(confirmed):,} rows loaded")

    df_off.to_sql("stream_kafka_offsets", engine,
                  if_exists="replace", index=False)
    log(f"stream_kafka_offsets: {len(df_off):,} rows loaded")

    with engine.connect() as conn:
        for tbl in ["stream_price_windows","stream_alert_windows",
                    "stream_confirmed_txs","stream_kafka_offsets"]:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            log(f"  {tbl}: {n:,} rows in DB")

    section("EXAM 7 — Persistence")
    with engine.connect() as conn:
        n_windows = conn.execute(
            text("SELECT COUNT(*) FROM stream_price_windows")).scalar()
        n_alerts  = conn.execute(
            text("SELECT COUNT(*) FROM stream_alert_windows")).scalar()
        n_txs     = conn.execute(
            text("SELECT COUNT(*) FROM stream_confirmed_txs")).scalar()
    exam("stream_price_windows loaded correctly",
         check_fn=lambda _: n_windows > 0)
    exam("stream_alert_windows has 2,000 rows",
         result=n_alerts, expected=2000)
    exam("Confirmed transactions persisted",
         check_fn=lambda _: n_txs > 0)

except Exception as e:
    print(f"\n  [PostgreSQL not reachable — update DB_URL]\n  Error: {e}")
    print("  Tasks 1-6 ran fully without DB.")

section("DAY 5 COMPLETE")
print(f"""
  Summary:
  • Loaded    : {len(df_prices):,} price msgs + {len(df_mempool):,} mempool msgs
  • Produced  : 10,000 messages at {rate:,} msg/sec (simulated Kafka)
  • Consumed  : 5,000 messages with manual offset commit
  • Windows   : 30s tumbling + 5min sliding on price feed
  • Anomalies : {len(anomalies):,} price spikes detected
  • Alerts    : {len(high_value)} high-value txs + {len(gas_spikes):,} gas spikes
  • Persisted : 4 stream result tables to PostgreSQL

  Next: Day 6 — run this on REAL Kafka with Docker + BigQuery on Cloud

  git add .
  git commit -m "day5: Kafka streaming, windows, alerts, offset management"
  git push
""")
