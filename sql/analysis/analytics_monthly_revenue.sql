-- ============================================================
-- ANALYTICS: MONTHLY REVENUE
-- ============================================================
-- Purpose: Monthly revenue aggregation for trends analysis
-- Grain: 1 row per month
-- ============================================================

DELETE FROM analytics_monthly_revenue;

INSERT INTO analytics_monthly_revenue (
    year_month,
    total_revenue,
    total_orders,
    avg_order_value
)
SELECT
    strftime('%Y-%m', d.date) AS year_month,
    ROUND(SUM(f.total_order_value), 2) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(AVG(f.total_order_value), 2) AS avg_order_value
FROM fct_sales f
JOIN dim_date d
    ON f.order_date_sk = d.date_sk
WHERE f.is_delivered = 1
GROUP BY year_month
ORDER BY year_month;