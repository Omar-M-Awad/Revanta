-- ============================================================
-- REVANTA - RETAIL ANALYTICS DATABASE SCHEMA
-- ============================================================
-- Purpose: Complete schema for Staging, Dimension, and Fact tables
-- Database: SQLite
-- ============================================================

-- ============================================================
-- STAGING LAYER (Raw transformed data)
-- ============================================================

-- Staging: Orders
-- Grain: 1 row per order
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT NOT NULL,
    order_purchase_timestamp DATETIME NOT NULL,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Staging: Customers
-- Grain: 1 row per unique customer
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_unique_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Staging: Order Items
-- Grain: 1 row per order line item
CREATE TABLE IF NOT EXISTS stg_order_items (
    order_id TEXT NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    seller_id TEXT NOT NULL,
    shipping_limit_date DATETIME,
    price REAL NOT NULL,
    freight_value REAL NOT NULL,
    item_total_value REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES stg_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES stg_products(product_id)
);

-- Staging: Products
-- Grain: 1 row per product
CREATE TABLE IF NOT EXISTS stg_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT NOT NULL,
    product_name_lenght REAL,
    product_description_lenght REAL,
    product_photos_qty REAL,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL,
    product_volume_cm3 REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Staging: Category Translation
-- Grain: 1 row per product category (Portuguese → English mapping)
CREATE TABLE IF NOT EXISTS stg_category_translation (
    product_category_name TEXT PRIMARY KEY,
    product_category_name_english TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Dim: Customers
-- Enhanced customer dimension with aggregated metrics
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_unique_id TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    customer_city TEXT,
    customer_state TEXT,
    customer_zip_code_prefix TEXT,
    first_order_date DATE,
    last_order_date DATE,
    total_orders INTEGER DEFAULT 0,
    total_spent REAL DEFAULT 0,
    is_active BOOLEAN DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Dim: Products
-- Enhanced product dimension with category translations
CREATE TABLE IF NOT EXISTS dim_products (
    product_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL UNIQUE,
    product_category_name TEXT,
    product_category_name_english TEXT,
    product_name_lenght REAL,
    product_description_lenght REAL,
    product_photos_qty REAL,
    product_weight_g REAL,
    product_volume_cm3 REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Dim: Date
-- Calendar dimension for time-based analysis
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT 0
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- Fact: Sales (Order-level facts)
-- Grain: 1 row per order
CREATE TABLE IF NOT EXISTS fct_sales (
    sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,
    customer_sk INTEGER NOT NULL,
    order_date_sk INTEGER NOT NULL,
    order_status TEXT NOT NULL,
    total_price REAL NOT NULL,
    total_freight REAL NOT NULL,
    total_order_value REAL NOT NULL,
    order_item_count INTEGER NOT NULL,
    days_to_delivery INTEGER,
    is_delivered BOOLEAN,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk),
    FOREIGN KEY (order_date_sk) REFERENCES dim_date(date_sk)
);

-- Fact: Order Items (Line-level facts)
-- Grain: 1 row per order line item
CREATE TABLE IF NOT EXISTS fct_order_items (
    order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    product_sk INTEGER NOT NULL,
    item_sequence INTEGER NOT NULL,
    item_price REAL NOT NULL,
    item_freight REAL NOT NULL,
    item_total_value REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES fct_sales(order_id),
    FOREIGN KEY (product_sk) REFERENCES dim_products(product_sk)
);

-- ============================================================
-- BUSINESS LAYER (Marts & Analytics Tables)
-- ============================================================

-- Mart: Monthly Sales
-- Grain: 1 row per month (all customers aggregated)
CREATE TABLE IF NOT EXISTS mart_monthly_sales (
    year_month TEXT PRIMARY KEY,
    total_revenue REAL NOT NULL,
    total_orders INTEGER NOT NULL,
    avg_order_value REAL NOT NULL,
    calculated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Mart: Product Sales Performance
-- Grain: 1 row per product category
CREATE TABLE IF NOT EXISTS mart_product_sales (
    product_category TEXT PRIMARY KEY,
    total_revenue REAL NOT NULL,
    total_items_sold INTEGER NOT NULL,
    avg_item_price REAL NOT NULL,
    calculated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Analytics: Customer RFM Scores
-- Grain: 1 row per customer with RFM metrics
CREATE TABLE IF NOT EXISTS analytics_customer_rfm (
    customer_sk INTEGER PRIMARY KEY,
    customer_unique_id TEXT NOT NULL,
    recency_days INTEGER,
    frequency_count INTEGER,
    monetary_value REAL,
    rfm_score TEXT,
    rfm_segment TEXT,
    risk_level TEXT,
    calculated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk)
);

-- Analytics: Customer Risk Scoring
-- Grain: 1 row per customer with risk assessment
CREATE TABLE IF NOT EXISTS analytics_customer_risk_scoring (
    customer_sk INTEGER PRIMARY KEY,
    customer_unique_id TEXT NOT NULL,
    last_purchase_date DATE,
    days_since_last_purchase INTEGER,
    purchase_frequency INTEGER,
    average_order_value REAL,
    lifetime_value REAL,
    risk_score REAL,
    risk_category TEXT,
    risk_reason TEXT,
    alert_flag BOOLEAN DEFAULT 0,
    calculated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk)
);

-- Analytics: Monthly Revenue (Overall Trends)
-- Grain: 1 row per month (all customers aggregated)
-- ✅ FIXED: Removed customer_sk, matches SQL output
CREATE TABLE IF NOT EXISTS analytics_monthly_revenue (
    year_month TEXT PRIMARY KEY,
    total_revenue REAL NOT NULL,
    total_orders INTEGER NOT NULL,
    avg_order_value REAL NOT NULL,
    calculated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Analytics: Product Performance
-- Grain: 1 row per product with performance metrics
CREATE TABLE IF NOT EXISTS analytics_product_performance (
    product_sk INTEGER PRIMARY KEY,
    product_id TEXT NOT NULL,
    category_english TEXT,
    total_units_sold INTEGER DEFAULT 0,
    total_revenue REAL DEFAULT 0,
    avg_price REAL,
    avg_rating REAL,
    total_reviews INTEGER DEFAULT 0,
    calculated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_sk) REFERENCES dim_products(product_sk)
);

-- ============================================================
-- INDEXES (Performance Optimization)
-- ============================================================

-- Staging table indexes
CREATE INDEX IF NOT EXISTS idx_stg_orders_customer_id 
    ON stg_orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_orders_status 
    ON stg_orders(order_status);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_order_id 
    ON stg_order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_product_id 
    ON stg_order_items(product_id);

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fct_sales_customer_sk 
    ON fct_sales(customer_sk);

CREATE INDEX IF NOT EXISTS idx_fct_sales_order_date_sk 
    ON fct_sales(order_date_sk);

CREATE INDEX IF NOT EXISTS idx_fct_sales_order_status 
    ON fct_sales(order_status);

CREATE INDEX IF NOT EXISTS idx_fct_order_items_order_id 
    ON fct_order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_fct_order_items_product_sk 
    ON fct_order_items(product_sk);

-- Analytics table indexes
CREATE INDEX IF NOT EXISTS idx_analytics_rfm_segment 
    ON analytics_customer_rfm(rfm_segment);

CREATE INDEX IF NOT EXISTS idx_analytics_risk_category 
    ON analytics_customer_risk_scoring(risk_category);

CREATE INDEX IF NOT EXISTS idx_analytics_risk_alert 
    ON analytics_customer_risk_scoring(alert_flag);

CREATE INDEX IF NOT EXISTS idx_analytics_revenue_month 
    ON analytics_monthly_revenue(year_month);

-- ============================================================
-- END OF SCHEMA
-- ============================================================