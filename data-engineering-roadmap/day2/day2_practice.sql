-- ============================================================
-- DAY 2 PRACTICE SQL  |  Star Schema + Data Modelling
-- Run: psql -U postgres -d blockchain_db -f day2_practice.sql
-- ============================================================

\echo '============================================================'
\echo ' DAY 2 — Star Schema Queries & Data Modelling'
\echo '============================================================'

-- ── DROP & RECREATE SCHEMA ────────────────────────────────────

DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_wallet_scd    CASCADE;
DROP TABLE IF EXISTS dim_wallet        CASCADE;
DROP TABLE IF EXISTS dim_date          CASCADE;
DROP TABLE IF EXISTS dim_token         CASCADE;
DROP TABLE IF EXISTS dim_product       CASCADE;

-- ── DIMENSION TABLES ─────────────────────────────────────────

CREATE TABLE dim_date (
    date_id     INTEGER PRIMARY KEY,
    full_date   DATE NOT NULL,
    day         SMALLINT,
    month       SMALLINT,
    month_name  VARCHAR(10),
    quarter     SMALLINT,
    year        SMALLINT,
    day_of_week VARCHAR(10),
    is_weekend  SMALLINT
);

CREATE TABLE dim_wallet (
    wallet_id      SERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) UNIQUE NOT NULL,
    wallet_type    VARCHAR(20),
    primary_chain  VARCHAR(30),
    country        CHAR(2),
    risk_level     VARCHAR(10),
    first_seen_date DATE,
    is_kyc_verified SMALLINT,
    label          VARCHAR(30)
);

CREATE TABLE dim_token (
    token_id          SERIAL PRIMARY KEY,
    symbol            VARCHAR(10) NOT NULL,
    name              VARCHAR(50),
    chain             VARCHAR(30),
    decimals          SMALLINT,
    current_price_usd NUMERIC(18,4)
);

CREATE TABLE dim_product (
    product_id     SERIAL PRIMARY KEY,
    product_name   VARCHAR(100),
    product_type   VARCHAR(20),
    category       VARCHAR(30),
    base_price_usd NUMERIC(10,2),
    billing_cycle  VARCHAR(20)
);

-- ── SCD TYPE 2 DIMENSION ─────────────────────────────────────

CREATE TABLE dim_wallet_scd (
    scd_id         SERIAL PRIMARY KEY,
    wallet_id      INTEGER NOT NULL,
    wallet_address VARCHAR(42),
    risk_level     VARCHAR(10),
    kyc_verified   SMALLINT,
    valid_from     DATE NOT NULL,
    valid_to       DATE NOT NULL,
    is_current     SMALLINT DEFAULT 1,
    CHECK (valid_from < valid_to)
);

-- ── FACT TABLE ────────────────────────────────────────────────

CREATE TABLE fact_transactions (
    tx_id           SERIAL PRIMARY KEY,
    tx_hash         VARCHAR(66) UNIQUE NOT NULL,
    date_id         INTEGER REFERENCES dim_date(date_id),
    from_wallet_id  INTEGER REFERENCES dim_wallet(wallet_id),
    to_wallet_id    INTEGER REFERENCES dim_wallet(wallet_id),
    token_id        INTEGER REFERENCES dim_token(token_id),
    product_id      INTEGER REFERENCES dim_product(product_id),
    tx_type         VARCHAR(20),
    amount_token    NUMERIC(24,6),
    token_price_usd NUMERIC(18,4),
    amount_usd      NUMERIC(18,2),
    gas_fee_usd     NUMERIC(10,4),
    chain           VARCHAR(30),
    status          VARCHAR(20),
    block_number    BIGINT,
    created_at      TIMESTAMP
);

-- ── LOAD DATA ─────────────────────────────────────────────────

\echo 'Loading dimension tables...'
\copy dim_date       FROM '/tmp/dim_date.csv'       CSV HEADER
\copy dim_wallet     FROM '/tmp/dim_wallet.csv'     CSV HEADER
\copy dim_token      FROM '/tmp/dim_token.csv'      CSV HEADER
\copy dim_product    FROM '/tmp/dim_product.csv'    CSV HEADER
\copy dim_wallet_scd FROM '/tmp/dim_wallet_scd.csv' CSV HEADER
\echo 'Loading fact table...'
\copy fact_transactions FROM '/tmp/fact_transactions.csv' CSV HEADER
\echo 'All tables loaded.'

-- ── INDEXES ───────────────────────────────────────────────────

\echo 'Creating indexes...'
CREATE INDEX idx_fact_date_id   ON fact_transactions(date_id);
CREATE INDEX idx_fact_wallet_id ON fact_transactions(from_wallet_id);
CREATE INDEX idx_fact_token_id  ON fact_transactions(token_id);
CREATE INDEX idx_fact_status    ON fact_transactions(status);
CREATE INDEX idx_fact_chain     ON fact_transactions(chain);
CREATE INDEX idx_fact_type      ON fact_transactions(tx_type);
CREATE INDEX idx_scd_wallet     ON dim_wallet_scd(wallet_id);
CREATE INDEX idx_scd_current    ON dim_wallet_scd(is_current);
\echo 'Indexes created.'

-- ════════════════════════════════════════════════════════════

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q1: Monthly on-chain volume by token (OLAP star join)'
\echo '------------------------------------------------------------'

SELECT
    d.month_name,
    d.quarter,
    t.symbol,
    t.chain                            AS token_chain,
    COUNT(*)                           AS transactions,
    ROUND(SUM(f.amount_usd), 2)        AS volume_usd,
    ROUND(AVG(f.amount_usd), 2)        AS avg_tx_usd,
    ROUND(AVG(f.gas_fee_usd), 4)       AS avg_gas_usd
FROM fact_transactions f
JOIN dim_date  d ON f.date_id  = d.date_id
JOIN dim_token t ON f.token_id = t.token_id
WHERE f.status = 'confirmed'
GROUP BY d.month, d.month_name, d.quarter, t.symbol, t.chain
ORDER BY d.month, volume_usd DESC;

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q2: Wallet risk exposure analysis (high vs medium vs low)'
\echo '------------------------------------------------------------'

SELECT
    w.risk_level,
    w.wallet_type,
    COUNT(DISTINCT f.from_wallet_id)     AS unique_wallets,
    COUNT(*)                             AS transactions,
    ROUND(SUM(f.amount_usd), 2)          AS total_volume_usd,
    ROUND(AVG(f.amount_usd), 2)          AS avg_tx_usd,
    ROUND(SUM(f.gas_fee_usd), 2)         AS total_gas_usd,
    ROUND(100.0 * COUNT(*) /
          SUM(COUNT(*)) OVER (), 1)      AS pct_of_all_txs
FROM fact_transactions f
JOIN dim_wallet w ON f.from_wallet_id = w.wallet_id
WHERE f.status = 'confirmed'
GROUP BY w.risk_level, w.wallet_type
ORDER BY total_volume_usd DESC;

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q3: Star schema join — top 20 wallets by USD volume'
\echo '------------------------------------------------------------'

SELECT
    w.wallet_address,
    w.wallet_type,
    w.risk_level,
    w.country,
    t.symbol                              AS top_token,
    COUNT(*)                              AS tx_count,
    ROUND(SUM(f.amount_usd), 2)           AS total_volume_usd,
    ROUND(AVG(f.gas_fee_usd), 4)          AS avg_gas_usd,
    MIN(d.full_date)                      AS first_tx_date,
    MAX(d.full_date)                      AS last_tx_date
FROM fact_transactions f
JOIN dim_wallet w ON f.from_wallet_id = w.wallet_id
JOIN dim_token  t ON f.token_id       = t.token_id
JOIN dim_date   d ON f.date_id        = d.date_id
WHERE f.status = 'confirmed'
GROUP BY w.wallet_address, w.wallet_type, w.risk_level, w.country, t.symbol
ORDER BY total_volume_usd DESC
LIMIT 20;

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q4: SCD Type 2 — current wallet state query'
\echo '------------------------------------------------------------'

SELECT
    s.wallet_id,
    s.wallet_address,
    s.risk_level                         AS current_risk,
    s.kyc_verified                       AS current_kyc,
    s.valid_from                         AS upgraded_on,
    s.valid_to
FROM dim_wallet_scd s
WHERE s.is_current = 1
ORDER BY s.valid_from DESC
LIMIT 20;

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q5: SCD audit — who changed risk level and when?'
\echo '------------------------------------------------------------'

SELECT
    old.wallet_address,
    old.risk_level                        AS risk_before,
    new.risk_level                        AS risk_after,
    old.kyc_verified                      AS kyc_before,
    new.kyc_verified                      AS kyc_after,
    old.valid_to                          AS changed_on
FROM dim_wallet_scd old
JOIN dim_wallet_scd new
    ON old.wallet_id = new.wallet_id
    AND old.is_current = 0
    AND new.is_current = 1
ORDER BY old.valid_to DESC
LIMIT 20;

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q6: Indexing proof — EXPLAIN ANALYZE on large join'
\echo '------------------------------------------------------------'

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    d.month_name,
    t.symbol,
    SUM(f.amount_usd) AS volume
FROM fact_transactions f
JOIN dim_date  d ON f.date_id  = d.date_id
JOIN dim_token t ON f.token_id = t.token_id
WHERE f.chain = 'ethereum'
  AND f.status = 'confirmed'
GROUP BY d.month_name, t.symbol;

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q7: Slowly Changing Dimension — point-in-time query'
\echo ' (What was each wallet risk level on 2024-06-01?)'
\echo '------------------------------------------------------------'

SELECT
    s.wallet_id,
    s.wallet_address,
    s.risk_level                          AS risk_on_june_1,
    s.kyc_verified,
    s.valid_from,
    s.valid_to
FROM dim_wallet_scd s
WHERE '2024-06-01' BETWEEN s.valid_from AND s.valid_to
ORDER BY s.wallet_id
LIMIT 20;

\echo ''
\echo '------------------------------------------------------------'
\echo ' Q8: OLAP — quarterly revenue with window functions'
\echo '------------------------------------------------------------'

WITH quarterly AS (
    SELECT
        d.quarter,
        d.year,
        t.symbol,
        ROUND(SUM(f.amount_usd), 2)     AS volume_usd,
        COUNT(*)                         AS tx_count
    FROM fact_transactions f
    JOIN dim_date  d ON f.date_id  = d.date_id
    JOIN dim_token t ON f.token_id = t.token_id
    WHERE f.status = 'confirmed'
    GROUP BY d.quarter, d.year, t.symbol
)
SELECT
    quarter,
    year,
    symbol,
    volume_usd,
    tx_count,
    ROUND(SUM(volume_usd) OVER (PARTITION BY symbol ORDER BY quarter), 2)
        AS cumulative_symbol_volume,
    RANK() OVER (PARTITION BY quarter ORDER BY volume_usd DESC)
        AS rank_in_quarter
FROM quarterly
ORDER BY quarter, rank_in_quarter;

\echo ''
\echo '============================================================'
\echo ' Day 2 SQL complete!'
\echo ' git add . && git commit -m "day2: star schema + SCD + indexing"'
\echo '============================================================'
