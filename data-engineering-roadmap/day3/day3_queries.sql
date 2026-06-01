-- ============================================================
-- DAY 3 SQL  |  ETL Validation + Pipeline Analytics
-- Run: psql -U postgres -d blockchain_db -f day3_queries.sql
-- ============================================================

\echo '============================================================'
\echo ' DAY 3 — ETL Pipeline Validation & Analytics'
\echo '============================================================'

-- ── Q1: Validate staged data quality ─────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q1: Staged data quality report'
\echo '------------------------------------------------------------'

SELECT
    'total_rows'           AS check_name,
    COUNT(*)::TEXT         AS result
FROM stg_blockchain_events
UNION ALL
SELECT 'confirmed_only',
    (COUNT(*) FILTER (WHERE status='confirmed') = COUNT(*))::TEXT
FROM stg_blockchain_events
UNION ALL
SELECT 'no_null_tx_hash',
    (SUM(CASE WHEN tx_hash IS NULL THEN 1 ELSE 0 END) = 0)::TEXT
FROM stg_blockchain_events
UNION ALL
SELECT 'no_negative_amount',
    (SUM(CASE WHEN amount_usd <= 0 THEN 1 ELSE 0 END) = 0)::TEXT
FROM stg_blockchain_events
UNION ALL
SELECT 'no_duplicate_tx',
    (COUNT(*) = COUNT(DISTINCT tx_hash))::TEXT
FROM stg_blockchain_events;

-- ── Q2: ETL pipeline summary ─────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q2: Volume by chain and token (post-ETL analytics)'
\echo '------------------------------------------------------------'

SELECT
    chain,
    token_symbol,
    COUNT(*)                              AS transactions,
    ROUND(SUM(amount_usd)::numeric, 2)    AS total_volume_usd,
    ROUND(AVG(amount_usd)::numeric, 2)    AS avg_tx_usd,
    ROUND(SUM(gas_fee_usd)::numeric, 4)   AS total_gas_usd,
    COUNT(*) FILTER (WHERE is_high_value=1) AS high_value_txs
FROM stg_blockchain_events
GROUP BY chain, token_symbol
ORDER BY total_volume_usd DESC
LIMIT 20;

-- ── Q3: Hourly transaction pattern ───────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q3: Hourly transaction pattern (find peak hours)'
\echo '------------------------------------------------------------'

SELECT
    hour,
    COUNT(*)                             AS transactions,
    ROUND(SUM(amount_usd)::numeric, 2)   AS volume_usd,
    ROUND(AVG(amount_usd)::numeric, 2)   AS avg_usd,
    REPEAT('█', (COUNT(*)/10)::INT)      AS bar
FROM stg_blockchain_events
GROUP BY hour
ORDER BY hour;

-- ── Q4: Monthly ETL load tracking ────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q4: Monthly event volume (ETL completeness check)'
\echo '------------------------------------------------------------'

SELECT
    year,
    month,
    COUNT(*)                              AS events_loaded,
    COUNT(DISTINCT chain)                 AS chains,
    COUNT(DISTINCT token_symbol)          AS tokens,
    ROUND(SUM(amount_usd)::numeric, 2)    AS volume_usd,
    MIN(pipeline_loaded_at)               AS first_loaded,
    MAX(pipeline_loaded_at)               AS last_loaded
FROM stg_blockchain_events
GROUP BY year, month
ORDER BY year, month;

-- ── Q5: High-value transaction alert query ───────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q5: High-value transactions >$10,000 (alert candidates)'
\echo '------------------------------------------------------------'

SELECT
    event_id,
    tx_hash,
    chain,
    token_symbol,
    ROUND(amount_usd::numeric, 2)         AS amount_usd,
    ROUND(amount_usd_market::numeric, 2)  AS market_value_usd,
    from_address,
    timestamp
FROM stg_blockchain_events
WHERE is_high_value = 1
ORDER BY amount_usd DESC
LIMIT 20;

-- ── Q6: Token price mart validation ──────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q6: Token price mart — year summary'
\echo '------------------------------------------------------------'

SELECT
    token,
    ROUND(avg_price_usd::numeric, 4)      AS avg_price,
    ROUND(min_price_usd::numeric, 4)      AS min_price,
    ROUND(max_price_usd::numeric, 4)      AS max_price,
    ROUND(price_range_usd::numeric, 4)    AS price_range,
    ROUND(avg_volume_usd::numeric, 0)     AS avg_daily_volume,
    days_tracked
FROM mart_token_prices
ORDER BY avg_price_usd DESC;

-- ── Q7: Pipeline run monitoring ──────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q7: Pipeline health — success rate by DAG'
\echo '------------------------------------------------------------'

SELECT
    dag_id,
    COUNT(*)                              AS total_runs,
    COUNT(*) FILTER (WHERE state='success') AS success,
    COUNT(*) FILTER (WHERE state='failed')  AS failed,
    ROUND(100.0 * COUNT(*) FILTER (WHERE state='success')
          / COUNT(*), 1)                  AS success_rate_pct,
    ROUND(AVG(duration_sec)
          FILTER (WHERE state='success')::numeric, 1) AS avg_duration_sec,
    MAX(run_date)                         AS last_run
FROM mart_pipeline_runs
GROUP BY dag_id
ORDER BY success_rate_pct ASC;

-- ── Q8: dbt-style staging model ──────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q8: dbt staging model — stg_events_daily (mart pattern)'
\echo '------------------------------------------------------------'

WITH stg_daily AS (
    SELECT
        year,
        month,
        day,
        chain,
        token_symbol,
        COUNT(*)                             AS daily_txs,
        ROUND(SUM(amount_usd)::numeric, 2)   AS daily_volume_usd,
        ROUND(AVG(amount_usd)::numeric, 2)   AS avg_tx_usd,
        COUNT(*) FILTER (WHERE is_high_value=1) AS high_value_count
    FROM stg_blockchain_events
    GROUP BY year, month, day, chain, token_symbol
)
SELECT
    *,
    ROUND(SUM(daily_volume_usd) OVER (
        PARTITION BY chain, token_symbol
        ORDER BY year, month, day
    )::numeric, 2)                         AS cumulative_volume_usd,
    RANK() OVER (
        PARTITION BY year, month, chain
        ORDER BY daily_volume_usd DESC
    )                                      AS daily_rank_in_month
FROM stg_daily
ORDER BY year, month, day, daily_volume_usd DESC
LIMIT 30;

\echo ''
\echo '============================================================'
\echo ' Day 3 SQL complete!'
\echo ' git add . && git commit -m "day3: ETL pipeline + validation queries"'
\echo '============================================================'
