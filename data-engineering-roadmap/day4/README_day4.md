# Day 4 — PySpark & Big Data Processing
### 1M rows · Lazy evaluation · Partitioned Parquet · Window functions · Broadcast joins

---

## What this project builds

A big data processing pipeline using Apache Spark on 1 million on-chain blockchain events:
- Read 1M rows with explicit schema definition (no type inference)
- Filter, transform, and enrich using Spark's lazy evaluation engine
- Aggregate at scale: by chain, protocol, month, token
- Window functions: RANK() and running totals in Spark
- Write partitioned Parquet (chain × month = up to 72 partitions)
- Broadcast join: enrich events with small token reference table
- Compare pandas vs Spark performance on the same 1M dataset

---

## Files

```
day4/
├── blockchain_events_1m.csv     # 1,000,000 on-chain events (~285MB)
├── day4_spark.py                # Main PySpark script + exams
├── day4_queries.sql             # 7 SQL queries on 1M rows
└── README_day4.md               # This file
```

---

## Setup

```bash
pip install pyspark pyarrow pandas psycopg2-binary sqlalchemy
```

Java is required for PySpark:
```bash
# macOS
brew install openjdk@11
export JAVA_HOME=$(brew --prefix openjdk@11)

# verify
java -version
```

---

## Run

```bash
cd day4
python3 day4_spark.py
```

If PySpark is not installed, the script automatically falls back to pandas
on the same 1M dataset so all concepts still run.

```bash
# SQL queries (after script loads data)
psql -U postgres -h localhost -d blockchain_db -f day4_queries.sql
```

---

## Topics covered

### Lazy evaluation
Spark builds a DAG of transformations but executes nothing until an action
is called (`.count()`, `.show()`, `.write()`). This lets Spark optimize
the entire pipeline before running a single line.

```python
df_filtered = df.filter(col("status") == "confirmed")   # lazy
df_enriched = df_filtered.withColumn("gas_usd", ...)    # lazy
count = df_enriched.count()                             # ACTION — executes now
```

### Partitioned Parquet
Writing data partitioned by chain + month creates a folder structure:
```
blockchain_events_partitioned.parquet/
├── chain=ethereum/
│   ├── month=1/part-00000.parquet
│   ├── month=2/part-00000.parquet
│   └── ...
├── chain=polygon/
└── ...
```
When you query `WHERE chain='ethereum' AND month=1`, Spark reads ONLY
that one folder — skipping 71 others. This is predicate pushdown.

### Broadcast join
When joining a large table (1M rows) with a small table (15 tokens),
broadcast the small table to all executors so no data shuffle is needed:
```python
df.join(broadcast(df_tokens), df.token_symbol == df_tokens.symbol)
```
Shuffle is the most expensive operation in distributed computing.
Broadcast join eliminates it entirely for small tables.

### Window functions in Spark
```python
w = Window.partitionBy("chain").orderBy(desc("volume_usd"))
df.withColumn("rank", rank().over(w))
```
Same concept as SQL RANK() OVER PARTITION BY — but runs distributed
across all cores/machines.

---

## Key Spark concepts

| Concept | What it means |
|---------|--------------|
| SparkSession | Entry point — one per application |
| DataFrame | Distributed table — like pandas but across many machines |
| Transformation | Lazy operation: filter, withColumn, groupBy, join |
| Action | Triggers execution: count(), show(), write(), collect() |
| Partition | Unit of parallelism — data split across workers |
| Shuffle | Moving data between partitions — most expensive operation |
| Broadcast | Send small table to all workers — avoids shuffle |
| Parquet | Columnar binary format — 5-10x smaller than CSV, much faster |
| Predicate pushdown | Skip partitions/columns at read time based on WHERE clause |

---

## Commit

```bash
git add .
git commit -m "day4: PySpark 1M rows, partitioned Parquet, window functions, broadcast join"
git push
```

---

*Part of the 7-Day Data Engineer Roadmap — Blockchain Infrastructure Track*
