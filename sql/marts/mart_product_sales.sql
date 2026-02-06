-- ============================================================
-- MART: PRODUCT SALES PERFORMANCE
-- Grain: 1 row per product category
-- ============================================================

DELETE FROM mart_product_sales;

INSERT INTO mart_product_sales (
    product_category,
    total_revenue,
    total_items_sold,
    avg_item_price
)
SELECT
    dp.product_category_name_english AS product_category,
    ROUND(SUM(foi.item_total_value), 2) AS total_revenue,
    COUNT(*) AS total_items_sold,
    ROUND(AVG(foi.item_price), 2) AS avg_item_price
FROM fct_order_items foi
JOIN dim_products dp
    ON foi.product_sk = dp.product_sk
GROUP BY dp.product_category_name_english
ORDER BY total_revenue DESC;
