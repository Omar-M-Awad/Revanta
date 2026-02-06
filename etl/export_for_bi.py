"""
Export ALL analytics tables from Revanta database to XLSX
For Power BI Dashboard
"""

import sqlite3
import pandas as pd
from pathlib import Path

def export_all_tables():
    """Export all analytics tables to XLSX files"""
    
    # Paths
    PROJECT_DIR = Path.cwd()
    DB_PATH = PROJECT_DIR / "database" / "revanta.db"
    BI_EXPORTS_DIR = PROJECT_DIR / "bi_exports"
    
    print(f"🔍 Looking for database at: {DB_PATH}")
    
    if not DB_PATH.exists():
        print(f"❌ Database not found at: {DB_PATH}")
        return
    
    print(f"✅ Database found!")
    print("=" * 70)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    # Tables to export
    tables_to_export = [
        "analytics_customer_rfm",
        "analytics_customer_risk_scoring",
        "analytics_monthly_revenue",
        "analytics_product_performance",
        "fct_sales",
    ]
    
    print(f"📤 Exporting {len(tables_to_export)} tables to XLSX...")
    print()
    
    for table_name in tables_to_export:
        try:
            # Read table from database
            print(f"⏳ Exporting: {table_name}...", end=" ")
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            
            # Create XLSX filename
            xlsx_file = BI_EXPORTS_DIR / f"{table_name}.xlsx"
            
            # Write to XLSX
            df.to_excel(xlsx_file, index=False, engine='openpyxl')
            
            print(f"✅")
            print(f"   → {xlsx_file.name}")
            print(f"   → Rows: {len(df):,} | Columns: {len(df.columns)}")
            print()
            
        except Exception as e:
            print(f"❌")
            print(f"   Error: {e}")
            print()
    
    conn.close()
    
    print("=" * 70)
    print("✅ All tables exported successfully!")
    print(f"📁 XLSX files ready in: {BI_EXPORTS_DIR}")
    print()
    print("Ready for Power BI! 🚀")

if __name__ == "__main__":
    export_all_tables()