-- ============================================================
-- REVANTA - STAGING ORDERS VALIDATION
-- ============================================================
-- Purpose: Additional validation and quality checks for orders
-- Note: Basic cleaning already done in Python transform layer
-- ============================================================

-- Validation: Ensure order dates are reasonable
SELECT 
    COUNT(*) as invalid_order_count
FROM stg_orders
WHERE order_delivered_customer_date < order_purchase_timestamp
   OR order_approved_at < order_purchase_timestamp;

-- This should return 0 if all dates are valid