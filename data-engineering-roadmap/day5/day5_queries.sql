-- ============================================================
-- DAY 5 SQL  |  Stream Results Analytics
-- Run: psql -U postgres -d blockchain_db -f day5_queries.sql
-- Requires: day5_streaming.py to have loaded tables first
-- ============================================================

\echo '============================================================'
\echo ' DAY 5 — Stream Analytics & Kafka Monitoring Queries'
\echo '============================================================'

-- ── Q1: Price window analysis ─────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q1: 30-second price windows — avg by token'
\echo '------------------------------------------------------------'

SELECT
    token,
    COUNT(*)                                AS windows,
    ROUND(AVG(avg_price)::numeric, 4)       AS overall_avg_price,
    ROUND(MIN(min_price)::numeric, 4)       AS absolute_min,
    ROUND(MAX(max_price)::numeric, 4)       AS absolute_max,
    ROUND(AVG(msg_count)::numeric, 1)       AS avg_msgs_per_window,
    ROUND(AVG(avg_latency)::numeric, 2)     AS avg_latency_ms
FROM stream_price_windows
GROUP BY token
ORDER BY overall_avg_price DESC;

-- ── Q2: Alert frequency ───────────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q2: Alert frequency by token (price anomaly detection)'
\echo '------------------------------------------------------------'

SELECT
    token,
    chain,
    COUNT(*)                                AS total_windows,
    SUM(alert_triggered)                    AS alerts_fired,
    ROUND(100.0 * SUM(alert_triggered)
          / COUNT(*), 2)                    AS alert_rate_pct,
    ROUND(AVG(price_change_pct)::numeric,4) AS avg_price_change,
    ROUND(MAX(ABS(price_change_pct))::numeric,4) AS max_abs_change
FROM stream_alert_windows
GROUP BY token, chain
ORDER BY alerts_fired DESC;

-- ── Q3: Kafka consumer lag monitoring ────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q3: Consumer lag by topic — is our pipeline keeping up?'
\echo '------------------------------------------------------------'

SELECT
    topic,
    COUNT(DISTINCT partition)               AS partitions,
    ROUND(AVG(lag)::numeric, 1)             AS avg_lag,
    MAX(lag)                                AS max_lag,
    MIN(lag)                                AS min_lag,
    COUNT(*) FILTER (WHERE lag > 100)       AS high_lag_events,
    MAX(committed_at)                       AS last_committed
FROM stream_kafka_offsets
GROUP BY topic
ORDER BY avg_lag DESC;

-- ── Q4: Confirmed transactions stream ────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q4: Confirmed transactions — protocol volume (streaming)'
\echo '------------------------------------------------------------'

SELECT
    protocol,
    COUNT(*)                                AS txs,
    ROUND(SUM(amount_usd)::numeric, 2)      AS volume_usd,
    ROUND(AVG(amount_usd)::numeric, 2)      AS avg_usd,
    COUNT(*) FILTER (WHERE amount_usd > 50000) AS whale_txs,
    COUNT(DISTINCT chain)                   AS chains
FROM stream_confirmed_txs
WHERE protocol != 'None'
GROUP BY protocol
ORDER BY volume_usd DESC;

-- ── Q5: Exactly-once check ───────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q5: Exactly-once validation — no duplicate tx_hashes'
\echo '------------------------------------------------------------'

SELECT
    'total_confirmed'                       AS check_name,
    COUNT(*)::TEXT                          AS result
FROM stream_confirmed_txs
UNION ALL
SELECT 'unique_tx_hashes',
    COUNT(DISTINCT tx_hash)::TEXT
FROM stream_confirmed_txs
UNION ALL
SELECT 'duplicates_found',
    SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END)::TEXT
FROM (
    SELECT tx_hash, COUNT(*) AS cnt
    FROM stream_confirmed_txs
    GROUP BY tx_hash
) t;

-- ── Q6: Real-time price comparison across exchanges ──────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q6: Price spread across exchanges (arbitrage detection)'
\echo '------------------------------------------------------------'

SELECT
    token,
    exchange,
    ROUND(AVG(avg_price)::numeric, 6)       AS avg_price,
    ROUND(MIN(min_price)::numeric, 6)       AS min_seen,
    ROUND(MAX(max_price)::numeric, 6)       AS max_seen,
    COUNT(*)                                AS windows
FROM stream_price_windows
GROUP BY token, exchange
ORDER BY token, avg_price DESC;

-- ── Q7: Sliding window simulation in SQL ─────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q7: 5-window rolling average — SQL sliding window'
\echo '------------------------------------------------------------'

WITH numbered AS (
    SELECT
        token,
        window::TIMESTAMP                   AS window_ts,
        avg_price,
        ROW_NUMBER() OVER (
            PARTITION BY token ORDER BY window::TIMESTAMP
        ) AS rn
    FROM stream_price_windows
)
SELECT
    token,
    window_ts,
    ROUND(avg_price::numeric, 6)            AS current_price,
    ROUND(AVG(avg_price) OVER (
        PARTITION BY token
        ORDER BY rn
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )::numeric, 6)                          AS rolling_5w_avg,
    ROUND((avg_price - AVG(avg_price) OVER (
        PARTITION BY token
        ORDER BY rn
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ))::numeric, 6)                         AS deviation_from_avg
FROM numbered
ORDER BY token, window_ts
LIMIT 40;

\echo ''
\echo '============================================================'
\echo ' Day 5 SQL complete!'
\echo ' git add . && git commit -m "day5: streaming SQL analytics"'
\echo '============================================================'
