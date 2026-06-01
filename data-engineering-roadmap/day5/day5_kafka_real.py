"""
Day 5 — Real Kafka Producer & Consumer
========================================
Run ONLY after: docker-compose -f docker-compose-kafka.yml up -d
Requires: pip install kafka-python
This file runs the REAL Kafka pipeline (not simulated).
Check Kafka UI at: http://localhost:8090
"""

import json, time, threading
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP = "localhost:9092"

# ── Step 1: Create topics ─────────────────────────────────────
def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    topics = [
        NewTopic("defi-prices",      num_partitions=8, replication_factor=1),
        NewTopic("mempool-events",   num_partitions=4, replication_factor=1),
        NewTopic("alerts-high-value",num_partitions=2, replication_factor=1),
        NewTopic("agg-metrics",      num_partitions=2, replication_factor=1),
    ]
    try:
        admin.create_topics(topics)
        print("[INFO] Topics created: defi-prices, mempool-events, alerts-high-value, agg-metrics")
    except Exception as e:
        print(f"[INFO] Topics may already exist: {e}")
    admin.close()

# ── Step 2: Producer — publish DeFi prices ────────────────────
def run_producer(n_messages=1000):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",                  # wait for all replicas
        retries=3,
        linger_ms=5,                 # batch messages for 5ms
        batch_size=16384,            # 16KB batches
    )

    with open("defi_price_feed.json") as f:
        messages = json.load(f)[:n_messages]

    print(f"[KAFKA] Publishing {n_messages} messages to defi-prices...")
    t0 = time.time()
    for msg in messages:
        producer.send(
            topic="defi-prices",
            key=msg["token"],         # partition by token
            value=msg,
        )
    producer.flush()
    elapsed = time.time() - t0
    print(f"[KAFKA] Published {n_messages} msgs in {elapsed:.2f}s "
          f"({round(n_messages/elapsed):,} msg/sec)")
    producer.close()

# ── Step 3: Consumer — process prices + write aggregations ────
def run_consumer(max_messages=500, timeout_sec=30):
    consumer = KafkaConsumer(
        "defi-prices",
        bootstrap_servers=BOOTSTRAP,
        group_id="blockchain-analytics-cg",
        auto_offset_reset="earliest",
        enable_auto_commit=False,       # manual commit = exactly-once
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=100,
    )

    print(f"[KAFKA] Consumer started — group: blockchain-analytics-cg")
    print(f"[KAFKA] auto_commit=False — manual commit for exactly-once")

    processed = 0
    price_buffer = {}   # token → list of prices for windowing

    deadline = time.time() + timeout_sec
    while processed < max_messages and time.time() < deadline:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            continue

        for tp, msgs in records.items():
            for msg in msgs:
                data = msg.value
                token = data["token"]

                # aggregate in memory (30-second window simulation)
                if token not in price_buffer:
                    price_buffer[token] = []
                price_buffer[token].append(data["mid_price"])

                processed += 1

            # commit offset AFTER processing the batch
            consumer.commit()
            print(f"[KAFKA] Processed {processed} msgs | "
                  f"Partition {tp.partition} | "
                  f"Offset {msgs[-1].offset}")

    # print window results
    print(f"\n[KAFKA] Window results (avg price per token):")
    for token, prices in sorted(price_buffer.items()):
        avg = round(sum(prices)/len(prices), 4)
        print(f"  {token:6s}: avg=${avg:12.4f}  ({len(prices)} msgs)")

    consumer.close()
    print(f"\n[KAFKA] Consumer done — processed {processed} messages")

# ── Step 4: High-value alert consumer ────────────────────────
def run_alert_consumer(timeout_sec=20):
    """Separate consumer group just for high-value alerts"""
    consumer = KafkaConsumer(
        "mempool-events",
        bootstrap_servers=BOOTSTRAP,
        group_id="alert-consumer-cg",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    alerts = 0
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        records = consumer.poll(timeout_ms=1000)
        for tp, msgs in records.items():
            for msg in msgs:
                if msg.value.get("amount_usd",0) > 50000:
                    alerts += 1
                    print(f"  🚨 ALERT: {msg.value['token']} "
                          f"${msg.value['amount_usd']:,.0f} "
                          f"on {msg.value['chain']}")
    consumer.close()
    print(f"[ALERT] Total alerts fired: {alerts}")

# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*55)
    print("  Real Kafka Pipeline — Blockchain Streaming")
    print("="*55)

    try:
        print("\n[1/4] Creating Kafka topics...")
        create_topics()

        print("\n[2/4] Running producer (1,000 messages)...")
        run_producer(n_messages=1000)

        print("\n[3/4] Running consumer (500 messages)...")
        run_consumer(max_messages=500, timeout_sec=30)

        print("\n[4/4] Done!")
        print("\nOpen http://localhost:8090 to see topics in Kafka UI")

    except Exception as e:
        print(f"\n[ERROR] Kafka not reachable: {e}")
        print("Make sure Kafka is running:")
        print("  docker-compose -f docker-compose-kafka.yml up -d")
        print("  Wait 30 seconds then re-run this script")
