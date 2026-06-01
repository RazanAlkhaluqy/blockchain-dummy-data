-- ============================================================
-- DAY 7 SQL  |  Governance, Quality & Capstone Analytics
-- Run: psql -U postgres -d blockchain_db -f day7_queries.sql
-- Requires: day7_capstone.py to have loaded tables first
-- ============================================================

\echo '============================================================'
\echo ' DAY 7 — Governance, Quality & Capstone Final Queries'
\echo '============================================================'

-- ── Q1: Data quality scorecard ───────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q1: Data quality scorecard — 90-day check history'
\echo '------------------------------------------------------------'

SELECT
    model,
    COUNT(*)                                      AS total_checks,
    COUNT(*) FILTER (WHERE status='pass')         AS passed,
    COUNT(*) FILTER (WHERE status='fail')         AS failed,
    COUNT(*) FILTER (WHERE status='warn')         AS warned,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status='pass')
          / COUNT(*), 1)                          AS quality_score_pct,
    ROUND(AVG(duration_sec)::numeric, 2)          AS avg_check_sec,
    MAX(run_date)                                 AS last_checked
FROM cap_dq_history
GROUP BY model
ORDER BY quality_score_pct ASC;

-- ── Q2: Failing checks detail ─────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q2: Recent failing checks — what needs fixing?'
\echo '------------------------------------------------------------'

SELECT
    check_id,
    run_date,
    model,
    check_type,
    column,
    rows_tested,
    rows_failed,
    ROUND(100.0 * rows_failed / NULLIF(rows_tested, 0), 3) AS failure_rate_pct,
    error_msg
FROM cap_dq_history
WHERE status = 'fail'
ORDER BY run_date DESC
LIMIT 20;

-- ── Q3: Pipeline health report ────────────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q3: Pipeline monitoring — SLA compliance by DAG'
\echo '------------------------------------------------------------'

SELECT
    dag_id,
    COUNT(*)                                      AS total_runs,
    COUNT(*) FILTER (WHERE status='success')      AS successes,
    COUNT(*) FILTER (WHERE status='failed')       AS failures,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status='success')
          / COUNT(*), 1)                          AS success_rate_pct,
    SUM(sla_breach)                               AS sla_breaches,
    ROUND(AVG(duration_sec)
          FILTER (WHERE status='success')::numeric, 1) AS avg_dur_sec,
    ROUND(SUM(cost_usd)::numeric, 4)              AS total_cost_usd,
    SUM(rows_processed)                           AS total_rows_processed
FROM cap_pipeline_metrics
GROUP BY dag_id
ORDER BY success_rate_pct ASC;

-- ── Q4: Capstone chain analytics mart ────────────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q4: Final capstone mart — chain analytics'
\echo '------------------------------------------------------------'

SELECT
    chain,
    txs,
    ROUND(volume_usd::numeric, 2)                 AS volume_usd,
    ROUND(gas_usd::numeric, 4)                    AS gas_usd,
    unique_wallets,
    ROUND(100.0 * volume_usd::numeric
          / SUM(volume_usd) OVER (), 1)            AS volume_share_pct,
    RANK() OVER (ORDER BY volume_usd DESC)         AS chain_rank
FROM cap_chain_mart
ORDER BY volume_usd DESC;

-- ── Q5: Token performance with window functions ───────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q5: Token performance — volume rank + cumulative share'
\echo '------------------------------------------------------------'

SELECT
    token_symbol,
    txs,
    ROUND(volume_usd::numeric, 2)                 AS volume_usd,
    ROUND(avg_usd::numeric, 2)                    AS avg_tx_usd,
    RANK() OVER (ORDER BY volume_usd DESC)         AS volume_rank,
    ROUND(100.0 * volume_usd::numeric
          / SUM(volume_usd) OVER (), 1)            AS market_share_pct,
    ROUND(SUM(volume_usd::numeric) OVER (
        ORDER BY volume_usd DESC
    ), 2)                                          AS cumulative_volume
FROM cap_token_mart
ORDER BY volume_usd DESC;

-- ── Q6: GDPR audit — verify no PII in warehouse ──────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q6: GDPR compliance audit — PII check on cap_events'
\echo '------------------------------------------------------------'

SELECT
    'from_address column exists'          AS check_name,
    EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='cap_events'
          AND column_name='from_address'
    )::TEXT                               AS result,
    'FAIL if true — PII must be removed'  AS note
UNION ALL
SELECT
    'to_address column exists',
    EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='cap_events'
          AND column_name='to_address'
    )::TEXT,
    'FAIL if true — PII must be removed'
UNION ALL
SELECT
    'wallet_token column exists',
    EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='cap_events'
          AND column_name='wallet_token'
    )::TEXT,
    'PASS — pseudonymised token present'
UNION ALL
SELECT
    'no duplicate event_ids',
    (COUNT(*) = COUNT(DISTINCT event_id))::TEXT,
    'PASS — data is deduplicated'
FROM cap_events
UNION ALL
SELECT
    'all amounts positive',
    (SUM(CASE WHEN amount_usd <= 0 THEN 1 ELSE 0 END) = 0)::TEXT,
    'PASS — data quality validated'
FROM cap_events;

-- ── Q7: End-to-end pipeline lineage in SQL ───────────────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q7: Data completeness — events per source'
\echo '------------------------------------------------------------'

SELECT
    source,
    COUNT(*)                              AS event_count,
    ROUND(SUM(amount_usd)::numeric, 2)    AS volume_usd,
    COUNT(DISTINCT chain)                 AS chains,
    COUNT(DISTINCT token_symbol)          AS tokens,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM cap_events
GROUP BY source
ORDER BY event_count DESC;

-- ── Q8: Final portfolio query — full platform KPIs ───────────

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q8: Blockchain Platform KPIs — Portfolio Showcase Query'
\echo '------------------------------------------------------------'

WITH platform_summary AS (
    SELECT
        COUNT(DISTINCT event_id)          AS total_events,
        ROUND(SUM(amount_usd)::numeric,2) AS total_volume_usd,
        COUNT(DISTINCT chain)             AS chains_covered,
        COUNT(DISTINCT token_symbol)      AS tokens_covered,
        COUNT(DISTINCT wallet_token)      AS unique_wallets,
        COUNT(*) FILTER (WHERE is_defi=1) AS defi_events,
        COUNT(*) FILTER (WHERE is_high_value=1) AS whale_events,
        MIN(timestamp)                    AS earliest_event,
        MAX(timestamp)                    AS latest_event
    FROM cap_events
)
SELECT
    *,
    ROUND(100.0 * defi_events / total_events, 1)  AS defi_pct,
    ROUND(100.0 * whale_events / total_events, 1) AS whale_pct,
    ROUND(total_volume_usd / total_events, 2)      AS avg_tx_usd
FROM platform_summary;

\echo ''
\echo '============================================================'
\echo ' Day 7 complete — 7-Day Blockchain Data Engineering Roadmap'
\echo ' git add . && git commit -m "day7: governance + capstone"'
\echo '============================================================'
