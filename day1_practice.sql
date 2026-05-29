-- ============================================================
-- DAY 1 PRACTICE SQL  |  Blockchain Startup Analytics
-- Run against PostgreSQL after loading orders.csv & users.csv
-- ============================================================

-- ── SETUP ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    user_id        VARCHAR(10) PRIMARY KEY,
    email          VARCHAR(100),
    wallet_address VARCHAR(42),
    country        CHAR(2),
    plan_tier      VARCHAR(20),
    registered_at  TIMESTAMP,
    kyc_verified   BOOLEAN
);

CREATE TABLE IF NOT EXISTS orders (
    order_id         VARCHAR(10) PRIMARY KEY,
    user_id          VARCHAR(10) REFERENCES users(user_id),
    wallet_address   VARCHAR(42),
    product_name     VARCHAR(100),
    product_type     VARCHAR(20),
    amount_usd       NUMERIC(10,2),
    discount_usd     NUMERIC(10,2),
    final_amount_usd NUMERIC(10,2),
    payment_method   VARCHAR(20),
    chain            VARCHAR(30),
    tx_hash          VARCHAR(66),
    gas_fee_usd      NUMERIC(10,4),
    status           VARCHAR(20),
    created_at       TIMESTAMP,
    country          CHAR(2)
);

-- Load with psql:
--   \copy users  FROM 'users.csv'  CSV HEADER
--   \copy orders FROM 'orders.csv' CSV HEADER

-- ── QUERY 1 ─ Revenue by month ────────────────────────────────
-- Goal: understand growth trend across 2024

SELECT
    DATE_TRUNC('month', created_at)         AS month,
    COUNT(*)                                AS total_orders,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed_orders,
    ROUND(SUM(final_amount_usd)
          FILTER (WHERE status = 'completed'), 2) AS revenue_usd,
    ROUND(AVG(final_amount_usd)
          FILTER (WHERE status = 'completed'), 2) AS avg_order_usd
FROM orders
GROUP BY 1
ORDER BY 1;

-- ── QUERY 2 ─ Top 10 customers by lifetime value ──────────────
-- Goal: identify your highest-value wallets (key in blockchain biz)

SELECT
    u.user_id,
    u.wallet_address,
    u.plan_tier,
    u.country,
    COUNT(o.order_id)                     AS total_orders,
    ROUND(SUM(o.final_amount_usd), 2)     AS lifetime_value_usd,
    MAX(o.created_at)                     AS last_order_at
FROM users u
JOIN orders o ON u.user_id = o.user_id
WHERE o.status = 'completed'
GROUP BY u.user_id, u.wallet_address, u.plan_tier, u.country
ORDER BY lifetime_value_usd DESC
LIMIT 10;

-- ── QUERY 3 ─ Revenue by payment method & chain ───────────────
-- Goal: which chains drive the most revenue? (critical for infra decisions)

SELECT
    COALESCE(payment_method, 'unknown')   AS payment_method,
    COALESCE(chain, 'fiat')               AS chain,
    COUNT(*)                              AS orders,
    ROUND(SUM(final_amount_usd), 2)       AS revenue_usd,
    ROUND(AVG(gas_fee_usd), 4)            AS avg_gas_usd
FROM orders
WHERE status = 'completed'
GROUP BY payment_method, chain
ORDER BY revenue_usd DESC;

-- ── QUERY 4 ─ Conversion funnel by product ────────────────────
-- Goal: which products have the worst drop-off / refund rate?

SELECT
    product_name,
    COUNT(*) FILTER (WHERE status = 'completed')  AS completed,
    COUNT(*) FILTER (WHERE status = 'pending')    AS pending,
    COUNT(*) FILTER (WHERE status = 'refunded')   AS refunded,
    COUNT(*) FILTER (WHERE status = 'failed')     AS failed,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status = 'completed')
        / NULLIF(COUNT(*), 0), 1
    )                                              AS completion_rate_pct,
    ROUND(SUM(final_amount_usd)
          FILTER (WHERE status = 'completed'), 2)  AS total_revenue_usd
FROM orders
GROUP BY product_name
ORDER BY total_revenue_usd DESC;

-- ── QUERY 5 ─ Window function: 7-day rolling revenue ─────────
-- Goal: spot revenue spikes (e.g. product launches, token events)

WITH daily AS (
    SELECT
        DATE(created_at)              AS day,
        ROUND(SUM(final_amount_usd), 2) AS daily_revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY 1
)
SELECT
    day,
    daily_revenue,
    ROUND(AVG(daily_revenue) OVER (
        ORDER BY day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                             AS rolling_7d_avg,
    ROUND(SUM(daily_revenue) OVER (
        ORDER BY day
    ), 2)                             AS cumulative_revenue
FROM daily
ORDER BY day;

-- ── BONUS ─ KYC gap analysis ─────────────────────────────────
-- Goal: how much revenue is at risk from unverified users?

SELECT
    u.kyc_verified,
    COUNT(DISTINCT u.user_id)             AS users,
    COUNT(o.order_id)                     AS orders,
    ROUND(SUM(o.final_amount_usd), 2)     AS revenue_usd,
    ROUND(AVG(o.final_amount_usd), 2)     AS avg_order_usd
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id AND o.status = 'completed'
GROUP BY u.kyc_verified;
