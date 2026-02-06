-- ============================================================
-- REVANTA - RFM ANALYSIS
-- ============================================================
-- Purpose: Customer segmentation using RFM analysis
-- Grain: 1 row per customer with RFM metrics
-- ============================================================

DELETE FROM analytics_customer_rfm;

INSERT INTO analytics_customer_rfm (
    customer_sk,
    customer_unique_id,
    recency_days,
    frequency_count,
    monetary_value,
    rfm_score,
    rfm_segment
)
WITH customer_orders AS (
    SELECT 
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS frequency_count,
        DATE(MAX(o.order_purchase_timestamp)) AS last_order_date,
        DATE(MIN(o.order_purchase_timestamp)) AS first_order_date,
        SUM(oi.item_total_value) AS monetary_value
    FROM stg_customers c
    JOIN stg_orders o 
        ON c.customer_id = o.customer_id
        AND o.order_status IN ('delivered', 'shipped', 'approved')
    LEFT JOIN stg_order_items oi 
        ON o.order_id = oi.order_id
    GROUP BY c.customer_unique_id
),
recency_score AS (
    SELECT 
        customer_unique_id,
        frequency_count,
        monetary_value,
        CAST(julianday('now') - julianday(last_order_date) AS INTEGER) AS recency_days,
        NTILE(5) OVER (
            ORDER BY julianday('now') - julianday(last_order_date)
        ) AS recency_score
    FROM customer_orders
    WHERE last_order_date IS NOT NULL
),
frequency_score AS (
    SELECT 
        customer_unique_id,
        recency_days,
        frequency_count,
        monetary_value,
        recency_score,
        NTILE(5) OVER (ORDER BY frequency_count DESC) AS frequency_score
    FROM recency_score
),
monetary_score AS (
    SELECT 
        customer_unique_id,
        recency_days,
        frequency_count,
        monetary_value,
        recency_score,
        frequency_score,
        NTILE(5) OVER (ORDER BY monetary_value DESC) AS monetary_score
    FROM frequency_score
)
SELECT 
    dc.customer_sk,
    ms.customer_unique_id,
    ms.recency_days,
    ms.frequency_count,
    ROUND(ms.monetary_value, 2),
    CAST(ms.recency_score AS TEXT) || '-' ||
    CAST(ms.frequency_score AS TEXT) || '-' ||
    CAST(ms.monetary_score AS TEXT),
    CASE
        WHEN ms.recency_score >= 4 AND ms.frequency_score >= 4 AND ms.monetary_score >= 4 THEN 'Champions'
        WHEN ms.recency_score >= 4 AND ms.frequency_score >= 4 THEN 'Loyal'
        WHEN ms.recency_score >= 4 AND ms.monetary_score >= 4 THEN 'Potential'
        WHEN ms.frequency_score >= 4 AND ms.monetary_score >= 4 THEN 'At Risk'
        WHEN ms.recency_score >= 4 THEN 'Need Activation'
        WHEN ms.frequency_score >= 4 OR ms.monetary_score >= 4 THEN 'Cant Lose Them'
        WHEN ms.recency_score <= 2 THEN 'Lost'
        ELSE 'Others'
    END
FROM monetary_score ms
JOIN dim_customers dc 
    ON ms.customer_unique_id = dc.customer_unique_id;