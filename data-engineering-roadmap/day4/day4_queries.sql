-- ============================================================
-- DAY 4 SQL  |  Big Data Concepts via SQL on 1M rows
-- Run AFTER day4_spark.py loads data into PostgreSQL
-- psql -U postgres -d blockchain_db -f day4_queries.sql
-- ============================================================

\echo '============================================================'
\echo ' DAY 4 — Big Data Analytics on 1M Blockchain Events'
\echo '============================================================'

-- ── Load 1M rows from CSV ─────────────────────────────────────

DROP TABLE IF EXISTS blockchain_events_1m;

CREATE TABLE blockchain_events_1m (
    tx_id          BIGINT PRIMARY KEY,
    tx_hash        VARCHAR(66) UNIQUE,
    block_number   BIGINT,
    chain          VARCHAR(30),
    event_type     VARCHAR(30),
    protocol       VARCHAR(30),
    from_address   VARCHAR(42),
    to_address     VARCHAR(42),
    token_symbol   VARCHAR(10),
    amount         NUMERIC(24,6),
    price_usd      NUMERIC(18,2),
    amount_usd     NUMERIC(18,2),
    gas_used       BIGINT,
    gas_price_gwei NUMERIC(10,2),
    gas_fee_eth    NUMERIC(18,8),
    status         VARCHAR(20),
    year           SMALLINT,
    month          SMALLINT,
    day            SMALLINT,
    hour           SMALLINT,
    timestamp      TIMESTAMP
);

\echo 'Loading 1M rows (takes ~30s)...'
\copy blockchain_events_1m FROM 'blockchain_events_1m.csv' CSV HEADER
\echo 'Done. Creating indexes...'

CREATE INDEX idx_1m_chain   ON blockchain_events_1m(chain);
CREATE INDEX idx_1m_status  ON blockchain_events_1m(status);
CREATE INDEX idx_1m_token   ON blockchain_events_1m(token_symbol);
CREATE INDEX idx_1m_month   ON blockchain_events_1m(year, month);
CREATE INDEX idx_1m_protocol ON blockchain_events_1m(protocol);

\echo 'Indexes created.'

-- ── Q1: Volume by chain ───────────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q1: Volume by chain (equivalent of Spark groupBy chain)'
\echo '------------------------------------------------------------'

SELECT
    chain,
    COUNT(*)                              AS transactions,
    ROUND(SUM(amount_usd)::numeric, 2)    AS volume_usd,
    ROUND(AVG(amount_usd)::numeric, 2)    AS avg_tx_usd,
    COUNT(DISTINCT from_address)          AS unique_wallets,
    COUNT(*) FILTER (WHERE status='confirmed') AS confirmed_txs
FROM blockchain_events_1m
GROUP BY chain
ORDER BY volume_usd DESC;

-- ── Q2: Top protocols ─────────────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q2: Top DeFi protocols by volume'
\echo '------------------------------------------------------------'

SELECT
    protocol,
    COUNT(*)                              AS transactions,
    ROUND(SUM(amount_usd)::numeric, 2)    AS volume_usd,
    ROUND(AVG(amount_usd)::numeric, 2)    AS avg_tx_usd,
    ROUND(AVG(gas_price_gwei)::numeric,2) AS avg_gwei,
    COUNT(*) FILTER (WHERE amount_usd > 10000) AS whale_txs
FROM blockchain_events_1m
WHERE status = 'confirmed'
  AND protocol != 'None'
GROUP BY protocol
ORDER BY volume_usd DESC;

-- ── Q3: Window — rank tokens per chain ───────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q3: Rank tokens by volume within each chain (window fn)'
\echo '------------------------------------------------------------'

WITH token_chain AS (
    SELECT
        chain,
        token_symbol,
        ROUND(SUM(amount_usd)::numeric,2) AS volume_usd,
        COUNT(*)                           AS txs
    FROM blockchain_events_1m
    WHERE status = 'confirmed'
    GROUP BY chain, token_symbol
)
SELECT
    chain,
    token_symbol,
    volume_usd,
    txs,
    RANK() OVER (PARTITION BY chain ORDER BY volume_usd DESC) AS rank_in_chain
FROM token_chain
WHERE RANK() OVER (PARTITION BY chain ORDER BY volume_usd DESC) <= 3
ORDER BY chain, rank_in_chain;

-- ── Q4: Running cumulative volume by month ───────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q4: Cumulative volume by month (running total window fn)'
\echo '------------------------------------------------------------'

WITH monthly AS (
    SELECT
        year, month,
        COUNT(*)                           AS transactions,
        ROUND(SUM(amount_usd)::numeric,2)  AS monthly_volume
    FROM blockchain_events_1m
    WHERE status = 'confirmed'
    GROUP BY year, month
)
SELECT
    year, month,
    transactions,
    monthly_volume,
    ROUND(SUM(monthly_volume) OVER (
        ORDER BY year, month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::numeric, 2)                         AS cumulative_volume,
    ROUND(AVG(monthly_volume) OVER (
        ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::numeric, 2)                         AS rolling_3m_avg
FROM monthly
ORDER BY year, month;

-- ── Q5: Partitioning simulation ───────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q5: Partition-aware query (ethereum + month=1 only)'
\echo '------------------------------------------------------------'

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COUNT(*), ROUND(SUM(amount_usd)::numeric,2) AS volume
FROM blockchain_events_1m
WHERE chain = 'ethereum'
  AND month = 1
  AND status = 'confirmed';

-- ── Q6: High-value whale transaction analysis ─────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q6: Whale wallet analysis (amount_usd > $10,000)'
\echo '------------------------------------------------------------'

WITH whale_wallets AS (
    SELECT
        from_address,
        COUNT(*)                             AS whale_txs,
        ROUND(SUM(amount_usd)::numeric,2)    AS total_usd,
        COUNT(DISTINCT chain)                AS chains_used,
        COUNT(DISTINCT token_symbol)         AS tokens_used,
        MAX(timestamp)                       AS last_tx
    FROM blockchain_events_1m
    WHERE amount_usd > 10000
      AND status = 'confirmed'
    GROUP BY from_address
    HAVING COUNT(*) >= 3
)
SELECT *,
    RANK() OVER (ORDER BY total_usd DESC) AS whale_rank
FROM whale_wallets
ORDER BY total_usd DESC
LIMIT 20;

-- ── Q7: Hourly gas price heatmap ─────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q7: Gas price by hour — find cheap transaction windows'
\echo '------------------------------------------------------------'

SELECT
    hour,
    ROUND(AVG(gas_price_gwei)::numeric,2)  AS avg_gwei,
    ROUND(MIN(gas_price_gwei)::numeric,2)  AS min_gwei,
    ROUND(MAX(gas_price_gwei)::numeric,2)  AS max_gwei,
    COUNT(*)                               AS transactions,
    REPEAT('█', (AVG(gas_price_gwei)/10)::INT) AS bar
FROM blockchain_events_1m
WHERE chain = 'ethereum'
  AND status = 'confirmed'
GROUP BY hour
ORDER BY hour;

\echo ''
\echo '============================================================'
\echo ' Day 4 SQL complete!'
\echo ' git add . && git commit -m "day4: big data SQL on 1M rows"'
\echo '============================================================'
