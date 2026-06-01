# Day 5 — Kafka & Real-time Streaming
### Producers · Consumers · Offset management · Tumbling windows · Exactly-once semantics

---

## What this builds

A complete real-time streaming pipeline for blockchain data:
- Kafka producer publishing 10,000 DeFi price messages at 5,000+ msg/sec
- Kafka consumer with manual offset commit (exactly-once guarantee)
- 30-second tumbling windows on price feed
- 5-minute sliding window rolling averages
- Price anomaly detection (>2% spike alerts)
- High-value transaction alerting (>$50,000)
- Gas price spike detection (MEV bot indicator)
- Mempool pending → confirmed transition tracking
- 7 SQL queries on stream results

---

## Files

```
day5/
├── defi_price_feed.json          # 50,000 DeFi price messages
├── mempool_stream.json           # 30,000 mempool transaction events
├── kafka_consumer_offsets.csv    # 5,000 consumer offset records
├── stream_window_results.csv     # 2,000 pre-computed 30s windows
├── day5_streaming.py             # Main script — simulated Kafka + all exams
├── day5_kafka_real.py            # Real Kafka producer + consumer
├── docker-compose-kafka.yml      # Kafka + Zookeeper + Kafka UI
├── day5_queries.sql              # 7 SQL queries on stream results
└── README_day5.md                # This file
```

---

## Option A — Run simulated (no Kafka install needed)

```bash
pip install pandas psycopg2-binary sqlalchemy
python3 day5_streaming.py
```

Runs everything — producers, consumers, windows, alerts — using Python queues
to simulate Kafka. All 7 exam sections pass. No Docker required.

---

## Option B — Run real Kafka (recommended for experience)

### Step 1: Start Kafka with Docker
```bash
docker-compose -f docker-compose-kafka.yml up -d
```

Wait 30 seconds for Kafka to fully start, then verify:
```bash
docker ps
# Should show: zookeeper, kafka, kafka-ui all running
```

### Step 2: Open Kafka UI
```
http://localhost:8090
```
You will see your Kafka cluster, topics, partitions, and consumer groups live.

### Step 3: Install kafka-python
```bash
pip install kafka-python
```

### Step 4: Run the real producer + consumer
```bash
python3 day5_kafka_real.py
```

Watch messages appear in Kafka UI in real time as the producer publishes them.

### Step 5: Run main script (also works with real Kafka)
```bash
python3 day5_streaming.py
```

### Step 6: Run SQL queries
```bash
psql -U postgres -h localhost -d blockchain_db -f day5_queries.sql
```

### Step 7: Stop Kafka when done
```bash
docker-compose -f docker-compose-kafka.yml down
```

---

## Kafka concepts covered

### Topics and partitions
```
Topic: defi-prices (8 partitions)
├── partition 0: all ETH messages  → consumer thread 0
├── partition 1: all BTC messages  → consumer thread 1
├── partition 2: all SOL messages  → consumer thread 2
└── ...
```
Partitioning by token ensures all prices for the same token arrive in order
and are processed by the same consumer thread.

### Offset management
```
Message stream: [0][1][2][3][4][5][6][7][8][9]...
                              ↑
                    committed offset = 5
                    (consumer processed 0-5,
                     will restart from 6 after crash)
```
- `auto_commit=True` → fast but may reprocess or skip on crash
- `auto_commit=False` → commit manually after DB write = exactly-once

### Window types
| Type | Description | Use case |
|------|-------------|----------|
| Tumbling | Fixed non-overlapping (0-30s, 30-60s) | Price OHLC candles |
| Sliding | Overlapping windows | Rolling averages |
| Session | Gap-based | User session analytics |

### Exactly-once semantics
1. Poll messages from Kafka
2. Process + write to PostgreSQL inside a transaction
3. Commit Kafka offset inside same transaction
→ If crash: both rollback → no duplicates, no data loss

---

## What changes in Day 6 (production)

| Today (Day 5) | Day 6 (production) |
|---------------|-------------------|
| Simulated Kafka in Python | Real Kafka on AWS MSK |
| Local PostgreSQL | BigQuery / Redshift |
| Manual trigger | Airflow scheduled DAG |
| localhost:9092 | broker.kafka.aws.com:9092 |

The code is identical — only the connection strings change.

---

## Commit

```bash
git add .
git commit -m "day5: Kafka streaming, windows, alerts, exactly-once semantics"
git push
```

---

*Part of the 7-Day Data Engineer Roadmap — Blockchain Infrastructure Track*
