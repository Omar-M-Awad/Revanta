"""
Revanta SQL Transformations Executor
===================================

Purpose:
- Build full analytics warehouse from staging tables
- Safe to re-run (idempotent)
- Production-style orchestration

Execution Flow:
Staging Tables (stg_*)
    ↓
Dimensions (dim_*)
    ↓
Facts (fct_*)
    ↓
Marts (mart_*)
    ↓
Analytics (analytics_*)

Usage:
    python database/run_sql_transformations.py
"""

import sqlite3
import logging
from pathlib import Path

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Paths
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "database" / "revanta.db"
SQL_DIR = BASE_DIR / "sql"

SCHEMA_FILE = BASE_DIR / "database" / "schema.sql"
MARTS_SQL_DIR = SQL_DIR / "marts"
ANALYSIS_SQL_DIR = SQL_DIR / "analysis"

# --------------------------------------------------
# DB Utilities
# --------------------------------------------------
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    logger.info(f"Connected to DB: {DB_PATH}")
    return conn


def read_sql(file_path: Path) -> str:
    """Read SQL file and return content"""
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


def execute_sql(conn, sql, description=""):
    """Execute SQL safely with error handling"""
    try:
        logger.info(f"▶ {description}")
        conn.executescript(sql)
        conn.commit()
        logger.info("   ✓ Done")
        return True
    except sqlite3.Error as e:
        logger.error(f"   ✗ SQL Error: {e}")
        conn.rollback()
        return False


def log_table_count(conn, table_name):
    """Log row count for a table"""
    try:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"   → {table_name}: {count:,} rows")
    except Exception as e:
        logger.warning(f"   ⚠ Could not count rows in {table_name}: {e}")

# --------------------------------------------------
# Schema
# --------------------------------------------------
def create_schema(conn):
    """Create all tables from schema.sql"""
    logger.info("\n📋 CREATING SCHEMA")
    return execute_sql(conn, read_sql(SCHEMA_FILE), "Create all tables")

# --------------------------------------------------
# Dimensions
# --------------------------------------------------
def build_dim_customers(conn):
    """Build customer dimension"""
    sql = """
    DELETE FROM dim_customers;

    INSERT INTO dim_customers (
        customer_unique_id,
        customer_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix,
        first_order_date,
        last_order_date,
        total_orders,
        total_spent,
        is_active
    )
    SELECT
        c.customer_unique_id,
        c.customer_id,
        c.customer_city,
        c.customer_state,
        c.customer_zip_code_prefix,
        DATE(MIN(o.order_purchase_timestamp)),
        DATE(MAX(o.order_purchase_timestamp)),
        COUNT(DISTINCT o.order_id),
        COALESCE(SUM(oi.item_total_value), 0),
        CASE
            WHEN julianday('now') - julianday(MAX(o.order_purchase_timestamp)) <= 90 THEN 1
            ELSE 0
        END
    FROM stg_customers c
    LEFT JOIN stg_orders o
        ON c.customer_id = o.customer_id
       AND o.order_status IN ('delivered', 'shipped', 'approved')
    LEFT JOIN stg_order_items oi
        ON o.order_id = oi.order_id
    GROUP BY c.customer_unique_id, c.customer_id;
    """
    execute_sql(conn, sql, "Build dim_customers")
    log_table_count(conn, "dim_customers")


def build_dim_products(conn):
    """Build product dimension with English translations"""
    sql = """
    DELETE FROM dim_products;

    INSERT INTO dim_products (
        product_id,
        product_category_name,
        product_category_name_english,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_volume_cm3
    )
    SELECT
        p.product_id,
        p.product_category_name,
        ct.product_category_name_english,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_volume_cm3
    FROM stg_products p
    LEFT JOIN stg_category_translation ct
        ON p.product_category_name = ct.product_category_name;
    """
    execute_sql(conn, sql, "Build dim_products")
    log_table_count(conn, "dim_products")


def build_dim_date(conn):
    """Build calendar dimension"""
    sql = """
    DELETE FROM dim_date;

    INSERT INTO dim_date (
        date_sk, date, year, quarter, month, day,
        day_of_week, week_of_year, is_weekend
    )
    WITH RECURSIVE calendar AS (
        SELECT DATE(MIN(order_purchase_timestamp)) AS d
        FROM stg_orders
        UNION ALL
        SELECT DATE(d, '+1 day')
        FROM calendar
        WHERE d < (SELECT DATE(MAX(order_purchase_timestamp)) FROM stg_orders)
    )
    SELECT
        CAST(strftime('%Y%m%d', d) AS INTEGER),
        d,
        CAST(strftime('%Y', d) AS INTEGER),
        ((CAST(strftime('%m', d) AS INTEGER) - 1) / 3) + 1,
        CAST(strftime('%m', d) AS INTEGER),
        CAST(strftime('%d', d) AS INTEGER),
        CAST(strftime('%w', d) AS INTEGER),
        CAST(strftime('%W', d) AS INTEGER),
        CASE WHEN strftime('%w', d) IN ('0','6') THEN 1 ELSE 0 END
    FROM calendar;
    """
    execute_sql(conn, sql, "Build dim_date")
    log_table_count(conn, "dim_date")

# --------------------------------------------------
# Facts
# --------------------------------------------------
def build_fct_sales(conn):
    """Build sales fact table"""
    sql = """
    DELETE FROM fct_sales;

    INSERT INTO fct_sales (
        order_id,
        customer_sk,
        order_date_sk,
        order_status,
        total_price,
        total_freight,
        total_order_value,
        order_item_count,
        days_to_delivery,
        is_delivered
    )
    SELECT
        o.order_id,
        dc.customer_sk,
        CAST(strftime('%Y%m%d', o.order_purchase_timestamp) AS INTEGER),
        o.order_status,
        COALESCE(SUM(oi.price), 0),
        COALESCE(SUM(oi.freight_value), 0),
        COALESCE(SUM(oi.item_total_value), 0),
        COUNT(DISTINCT oi.order_item_id),
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
            THEN CAST(julianday(o.order_delivered_customer_date)
                   - julianday(o.order_purchase_timestamp) AS INTEGER)
        END,
        CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END
    FROM stg_orders o
    JOIN stg_customers sc ON o.customer_id = sc.customer_id
    JOIN dim_customers dc ON sc.customer_unique_id = dc.customer_unique_id
    LEFT JOIN stg_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status IN ('delivered', 'shipped', 'approved')
    GROUP BY o.order_id;
    """
    execute_sql(conn, sql, "Build fct_sales")
    log_table_count(conn, "fct_sales")


def build_fct_order_items(conn):
    """Build order items fact table"""
    sql = """
    DELETE FROM fct_order_items;

    INSERT INTO fct_order_items (
        order_id,
        product_sk,
        item_sequence,
        item_price,
        item_freight,
        item_total_value
    )
    SELECT
        oi.order_id,
        dp.product_sk,
        oi.order_item_id,
        oi.price,
        oi.freight_value,
        oi.item_total_value
    FROM stg_order_items oi
    JOIN dim_products dp
        ON oi.product_id = dp.product_id;
    """
    execute_sql(conn, sql, "Build fct_order_items")
    log_table_count(conn, "fct_order_items")


def build_fct_customer_risk_scoring(conn):
    """Build customer risk scoring from marts SQL"""
    sql = read_sql(MARTS_SQL_DIR / "fct_customer_risk_scoring.sql")
    execute_sql(conn, sql, "Build fct_customer_risk_scoring")
    log_table_count(conn, "fct_customer_risk_scoring")

# --------------------------------------------------
# Marts (Business Logic)
# --------------------------------------------------
def build_mart_monthly_sales(conn):
    """Build monthly sales mart"""
    sql = read_sql(MARTS_SQL_DIR / "mart_monthly_sales.sql")
    execute_sql(conn, sql, "Build mart_monthly_sales")
    log_table_count(conn, "mart_monthly_sales")


def build_mart_product_sales(conn):
    """Build product sales mart"""
    sql = read_sql(MARTS_SQL_DIR / "mart_product_sales.sql")
    execute_sql(conn, sql, "Build mart_product_sales")
    log_table_count(conn, "mart_product_sales")

# --------------------------------------------------
# Analytics (Analysis Queries)
# --------------------------------------------------
def build_analytics_customer_rfm(conn):
    """Build RFM segmentation"""
    sql = read_sql(ANALYSIS_SQL_DIR / "rfm_analysis.sql")
    execute_sql(conn, sql, "Build analytics_customer_rfm")
    log_table_count(conn, "analytics_customer_rfm")


def build_analytics_monthly_revenue(conn):
    """Build monthly revenue analytics"""
    sql = read_sql(ANALYSIS_SQL_DIR / "analytics_monthly_revenue.sql")
    execute_sql(conn, sql, "Build analytics_monthly_revenue")
    log_table_count(conn, "analytics_monthly_revenue")


def build_analytics_customer_risk_scoring(conn):
    """Build customer risk scoring analytics"""
    sql = """
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
    """
    execute_sql(conn, sql, "Build analytics_customer_risk_scoring")
    log_table_count(conn, "analytics_customer_risk_scoring")


def build_analytics_product_performance(conn):
    """Build product performance analytics"""
    sql = """
    DELETE FROM analytics_product_performance;

    INSERT INTO analytics_product_performance (
        product_sk,
        product_id,
        category_english,
        total_units_sold,
        total_revenue,
        avg_price
    )
    SELECT
        dp.product_sk,
        dp.product_id,
        dp.product_category_name_english,
        COUNT(*) AS total_units_sold,
        ROUND(SUM(foi.item_total_value), 2) AS total_revenue,
        ROUND(AVG(foi.item_price), 2) AS avg_price
    FROM fct_order_items foi
    JOIN dim_products dp ON foi.product_sk = dp.product_sk
    GROUP BY dp.product_sk, dp.product_id, dp.product_category_name_english;
    """
    execute_sql(conn, sql, "Build analytics_product_performance")
    log_table_count(conn, "analytics_product_performance")

# --------------------------------------------------
# Orchestrator
# --------------------------------------------------
def run_all_sql_transformations():
    """Main orchestrator - runs all transformations in correct order"""
    logger.info("\n" + "="*60)
    logger.info("🚀 STARTING REVANTA SQL TRANSFORMATIONS")
    logger.info("="*60)
    
    conn = get_connection()

    try:
        create_schema(conn)

        logger.info("\n📐 BUILDING DIMENSIONS")
        build_dim_customers(conn)
        build_dim_products(conn)
        build_dim_date(conn)

        logger.info("\n📊 BUILDING FACTS")
        build_fct_sales(conn)
        build_fct_order_items(conn)
        build_fct_customer_risk_scoring(conn)

        logger.info("\n🏪 BUILDING MARTS")
        build_mart_monthly_sales(conn)
        build_mart_product_sales(conn)

        logger.info("\n📈 BUILDING ANALYTICS")
        build_analytics_customer_rfm(conn)
        build_analytics_monthly_revenue(conn)
        build_analytics_customer_risk_scoring(conn)
        build_analytics_product_performance(conn)

        logger.info("\n" + "="*60)
        logger.info("✅ ALL TRANSFORMATIONS COMPLETED SUCCESSFULLY!")
        logger.info("="*60 + "\n")

    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        raise
    finally:
        conn.close()
        logger.info("DB connection closed\n")


if __name__ == "__main__":
    run_all_sql_transformations()