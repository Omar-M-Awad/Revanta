-- ============================================================
-- REVANTA - STAGING PRODUCTS VALIDATION
-- ============================================================
-- Purpose: Additional validation and quality checks for products
-- ============================================================

-- Validation: Ensure product_id is unique
SELECT 
    COUNT(*) as duplicate_product_count
FROM (
    SELECT product_id, COUNT(*) as cnt
    FROM stg_products
    GROUP BY product_id
    HAVING cnt > 1
);

-- This should return 0 if all products are unique