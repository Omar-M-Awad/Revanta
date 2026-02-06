-- ============================================================
-- REVANTA - STAGING CUSTOMERS VALIDATION
-- ============================================================
-- Purpose: Additional validation and quality checks for customers
-- ============================================================

-- Validation: Ensure customer_unique_id is unique
SELECT 
    COUNT(*) as duplicate_customer_count
FROM (
    SELECT customer_unique_id, COUNT(*) as cnt
    FROM stg_customers
    GROUP BY customer_unique_id
    HAVING cnt > 1
);

-- This should return 0 if all customers are unique