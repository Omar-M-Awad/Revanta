-- ============================================================
-- FACT: CUSTOMER RISK SCORING
-- ============================================================
-- Purpose: Customer risk assessment for churn prediction
-- Grain: 1 row per customer
-- ============================================================

DELETE FROM analytics_customer_risk_scoring;

INSERT INTO analytics_customer_risk_scoring (
    customer_sk,
    customer_unique_id,
    last_purchase_date,
    days_since_last_purchase,
    purchase_frequency,
    average_order_value,
    lifetime_value,
    risk_score,
    risk_category,
    risk_reason,
    alert_flag
)
SELECT
    dc.customer_sk,
    dc.customer_unique_id,
    dc.last_order_date,
    CAST(julianday('now') - julianday(dc.last_order_date) AS INTEGER),
    dc.total_orders,
    ROUND(dc.total_spent / NULLIF(dc.total_orders, 0), 2),
    dc.total_spent,
    -- Risk Score (0-1): Higher = more risk
    ROUND(
        (CAST(julianday('now') - julianday(dc.last_order_date) AS REAL) / 365.0 * 0.4) +
        (CASE WHEN dc.total_orders < 3 THEN 0.3 ELSE 0 END) +
        (CASE WHEN dc.total_spent < 100 THEN 0.2 ELSE 0 END) +
        (CASE WHEN is_active = 0 THEN 0.1 ELSE 0 END),
        2
    ),
    -- Risk Category
    CASE
        WHEN (CAST(julianday('now') - julianday(dc.last_order_date) AS INTEGER) > 90 AND dc.total_spent > 500)
            THEN 'CRITICAL'
        WHEN (CAST(julianday('now') - julianday(dc.last_order_date) AS INTEGER) > 90)
            THEN 'HIGH'
        WHEN (CAST(julianday('now') - julianday(dc.last_order_date) AS INTEGER) > 60)
            THEN 'MEDIUM'
        WHEN (CAST(julianday('now') - julianday(dc.last_order_date) AS INTEGER) > 30)
            THEN 'LOW'
        ELSE 'VERY_LOW'
    END,
    -- Risk Reason
    CASE
        WHEN (CAST(julianday('now') - julianday(dc.last_order_date) AS INTEGER) > 90)
            THEN 'Inactive > 90 days'
        WHEN dc.total_orders < 3
            THEN 'Low purchase frequency'
        WHEN dc.total_spent < 100
            THEN 'Low lifetime value'
        ELSE 'Active customer'
    END,
    -- Alert Flag
    CASE
        WHEN (CAST(julianday('now') - julianday(dc.last_order_date) AS INTEGER) > 90 AND dc.total_spent > 500)
            THEN 1
        ELSE 0
    END
FROM dim_customers dc;