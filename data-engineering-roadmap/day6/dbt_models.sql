-- ============================================================
-- DAY 6 — dbt Models (written as SQL, run via psql locally)
-- In production: dbt run --models stg+ mart+
-- Run: psql -U postgres -d blockchain_db -f dbt_models.sql
-- ============================================================

\echo '============================================================'
\echo ' DAY 6 — dbt Models + BigQuery-style Analytics'
\echo '============================================================'

-- ════════════════════════════════════════════════════════════
-- LAYER 1: STAGING (clean + rename raw sources)
-- ════════════════════════════════════════════════════════════

\echo ''
\echo '-------- LAYER 1: STAGING MODELS --------'

DROP VIEW IF EXISTS stg_blockchain_events CASCADE;
CREATE VIEW stg_blockchain_events AS
SELECT
    event_id                             AS event_key,
    tx_hash                              AS transaction_hash,
    chain,
    event_type,
    CASE WHEN protocol = 'None'
         THEN NULL ELSE protocol END     AS protocol,
    from_address,
    to_address,
    token_symbol,
    amount,
    amount_usd,
    gas_fee_usd,
    year, month, day, hour,
    timestamp,
    loaded_at,
    CASE WHEN protocol != 'None'
         THEN 1 ELSE 0 END              AS is_defi,
    CASE WHEN amount_usd > 10000
         THEN 1 ELSE 0 END              AS is_high_value,
    NOW()                               AS dbt_updated_at
FROM wh_events
WHERE status = 'confirmed'
  AND tx_hash IS NOT NULL
  AND amount_usd > 0;

\echo 'stg_blockchain_events view created'

DROP VIEW IF EXISTS stg_wallet_profiles CASCADE;
CREATE VIEW stg_wallet_profiles AS
SELECT
    wallet_id,
    wallet_address,
    UPPER(country)                       AS country,
    wallet_type,
    ROUND(risk_score::numeric, 2)        AS risk_score,
    CASE WHEN risk_score >= 75 THEN 'high'
         WHEN risk_score >= 40 THEN 'medium'
         ELSE 'low' END                 AS risk_tier,
    kyc_verified,
    total_txs,
    total_volume_usd,
    first_seen::DATE,
    last_seen::DATE,
    primary_chain,
    COALESCE(label, 'unknown')          AS label,
    NOW()                               AS dbt_updated_at
FROM wh_wallet_profiles;

\echo 'stg_wallet_profiles view created'

-- ════════════════════════════════════════════════════════════
-- LAYER 2: INTERMEDIATE (business logic aggregations)
-- ════════════════════════════════════════════════════════════

\echo ''
\echo '-------- LAYER 2: INTERMEDIATE MODELS --------'

DROP VIEW IF EXISTS int_daily_volume CASCADE;
CREATE VIEW int_daily_volume AS
SELECT
    year, month, day,
    chain,
    token_symbol,
    COUNT(*)                             AS daily_txs,
    ROUND(SUM(amount_usd)::numeric,2)    AS daily_vol_usd,
    ROUND(AVG(amount_usd)::numeric,2)    AS avg_tx_usd,
    ROUND(SUM(gas_fee_usd)::numeric,4)   AS daily_gas_usd,
    COUNT(DISTINCT from_address)         AS unique_wallets,
    COUNT(*) FILTER (WHERE is_defi=1)    AS defi_txs,
    COUNT(*) FILTER (WHERE is_high_value=1) AS whale_txs
FROM stg_blockchain_events
GROUP BY year, month, day, chain, token_symbol;

\echo 'int_daily_volume view created'

DROP VIEW IF EXISTS int_wallet_metrics CASCADE;
CREATE VIEW int_wallet_metrics AS
SELECT
    e.from_address,
    COUNT(*)                             AS total_txs,
    ROUND(SUM(e.amount_usd)::numeric,2)  AS total_vol_usd,
    COUNT(DISTINCT e.chain)              AS chains_used,
    COUNT(DISTINCT e.token_symbol)       AS tokens_used,
    COUNT(DISTINCT e.protocol)           AS protocols_used,
    ROUND(AVG(e.amount_usd)::numeric,2)  AS avg_tx_usd,
    ROUND(SUM(e.gas_fee_usd)::numeric,4) AS total_gas_usd,
    MAX(e.timestamp)                     AS last_active,
    MIN(e.timestamp)                     AS first_active,
    COUNT(*) FILTER (WHERE e.is_defi=1)  AS defi_txs,
    COUNT(*) FILTER (WHERE e.is_high_value=1) AS whale_txs
FROM stg_blockchain_events e
GROUP BY e.from_address;

\echo 'int_wallet_metrics view created'

-- ════════════════════════════════════════════════════════════
-- LAYER 3: MARTS (business-facing tables)
-- ════════════════════════════════════════════════════════════

\echo ''
\echo '-------- LAYER 3: MART MODELS --------'

\echo ''
\echo '--- mart_chain_analytics ---'
DROP TABLE IF EXISTS mart_chain_analytics CASCADE;
CREATE TABLE mart_chain_analytics AS
SELECT
    chain,
    COUNT(*)                             AS total_txs,
    ROUND(SUM(amount_usd)::numeric,2)    AS total_vol_usd,
    ROUND(AVG(amount_usd)::numeric,2)    AS avg_tx_usd,
    ROUND(SUM(gas_fee_usd)::numeric,2)   AS total_gas_usd,
    COUNT(DISTINCT from_address)         AS unique_wallets,
    COUNT(DISTINCT token_symbol)         AS unique_tokens,
    COUNT(*) FILTER (WHERE is_defi=1)    AS defi_txs,
    COUNT(*) FILTER (WHERE is_high_value=1) AS whale_txs,
    ROUND(100.0*COUNT(*) FILTER (WHERE is_defi=1)/COUNT(*),1) AS defi_pct,
    ROUND(100.0*SUM(amount_usd)/SUM(SUM(amount_usd)) OVER(),1) AS revenue_share_pct
FROM stg_blockchain_events
GROUP BY chain
ORDER BY total_vol_usd DESC;

SELECT * FROM mart_chain_analytics;

\echo ''
\echo '--- mart_token_summary ---'
DROP TABLE IF EXISTS mart_token_summary CASCADE;
CREATE TABLE mart_token_summary AS
SELECT
    token_symbol,
    COUNT(*)                             AS total_txs,
    ROUND(SUM(amount_usd)::numeric,2)    AS total_vol_usd,
    ROUND(AVG(amount_usd)::numeric,2)    AS avg_tx_usd,
    ROUND(AVG(gas_fee_usd)::numeric,4)   AS avg_gas_usd,
    COUNT(DISTINCT chain)                AS chains_present,
    COUNT(DISTINCT from_address)         AS unique_senders,
    RANK() OVER (ORDER BY SUM(amount_usd) DESC) AS volume_rank
FROM stg_blockchain_events
GROUP BY token_symbol
ORDER BY total_vol_usd DESC;

SELECT * FROM mart_token_summary;

\echo ''
\echo '--- mart_wallet_leaderboard ---'
DROP TABLE IF EXISTS mart_wallet_leaderboard CASCADE;
CREATE TABLE mart_wallet_leaderboard AS
WITH wallet_stats AS (
    SELECT
        m.*,
        w.risk_tier,
        w.kyc_verified,
        w.country,
        w.label,
        w.wallet_type
    FROM int_wallet_metrics m
    LEFT JOIN stg_wallet_profiles w ON m.from_address = w.wallet_address
)
SELECT
    ROW_NUMBER() OVER (ORDER BY total_vol_usd DESC) AS rank,
    from_address,
    total_txs,
    ROUND(total_vol_usd::numeric,2)          AS total_vol_usd,
    chains_used,
    tokens_used,
    ROUND(avg_tx_usd::numeric,2)             AS avg_tx_usd,
    COALESCE(risk_tier,'unknown')            AS risk_tier,
    COALESCE(kyc_verified::TEXT,'unknown')   AS kyc_verified,
    COALESCE(country,'unknown')              AS country,
    COALESCE(label,'unknown')                AS label,
    whale_txs,
    defi_txs
FROM wallet_stats
ORDER BY total_vol_usd DESC
LIMIT 50;

SELECT * FROM mart_wallet_leaderboard LIMIT 10;

\echo ''
\echo '--- mart_defi_protocol_stats ---'
DROP TABLE IF EXISTS mart_defi_protocol_stats CASCADE;
CREATE TABLE mart_defi_protocol_stats AS
SELECT
    protocol,
    COUNT(*)                              AS total_txs,
    ROUND(SUM(amount_usd)::numeric,2)     AS total_vol_usd,
    ROUND(AVG(amount_usd)::numeric,2)     AS avg_tx_usd,
    ROUND(SUM(gas_fee_usd)::numeric,2)    AS total_gas_usd,
    COUNT(DISTINCT from_address)          AS unique_users,
    COUNT(DISTINCT chain)                 AS chains,
    COUNT(DISTINCT token_symbol)          AS tokens,
    RANK() OVER (ORDER BY SUM(amount_usd) DESC) AS volume_rank
FROM stg_blockchain_events
WHERE protocol IS NOT NULL
GROUP BY protocol
ORDER BY total_vol_usd DESC;

SELECT * FROM mart_defi_protocol_stats;

-- ════════════════════════════════════════════════════════════
-- dbt TESTS (schema validation)
-- ════════════════════════════════════════════════════════════

\echo ''
\echo '-------- dbt TESTS --------'

SELECT 'not_null: event_key'          AS test,
    (COUNT(*) FILTER (WHERE event_key IS NULL) = 0)::TEXT AS passed
FROM stg_blockchain_events
UNION ALL
SELECT 'unique: transaction_hash',
    (COUNT(*) = COUNT(DISTINCT transaction_hash))::TEXT
FROM stg_blockchain_events
UNION ALL
SELECT 'accepted_values: chain',
    (COUNT(*) FILTER (WHERE chain NOT IN
        ('ethereum','polygon','solana','arbitrum','optimism')) = 0)::TEXT
FROM stg_blockchain_events
UNION ALL
SELECT 'positive_values: amount_usd',
    (COUNT(*) FILTER (WHERE amount_usd <= 0) = 0)::TEXT
FROM stg_blockchain_events
UNION ALL
SELECT 'mart_chain: exactly 5 rows',
    (COUNT(*) = 5)::TEXT
FROM mart_chain_analytics
UNION ALL
SELECT 'mart_chain: revenue_share sums to 100',
    (ABS(SUM(revenue_share_pct) - 100) < 1)::TEXT
FROM mart_chain_analytics;

\echo ''
\echo '============================================================'
\echo ' Day 6 dbt models complete!'
\echo ' git add . && git commit -m "day6: cloud, dbt, Terraform"'
\echo '============================================================'
