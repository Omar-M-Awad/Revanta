"""
Convert CSV files to XLSX format
For use with Power BI
"""

import pandas as pd
from pathlib import Path

def convert_csv_to_xlsx():
    """Convert all CSV files in bi_exports to XLSX"""
    
    # Get the Revanta root directory (where this script is run from)
    # Look for bi_exports in current directory
    bi_exports_dir = Path.cwd() / "bi_exports"
    
    # If not found, try parent directory
    if not bi_exports_dir.exists():
        bi_exports_dir = Path.cwd().parent / "bi_exports"
    
    print(f"🔍 Looking in: {bi_exports_dir}")
    
    # Find all CSV files
    csv_files = list(bi_exports_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ No CSV files found in {bi_exports_dir}")
        print(f"   Make sure you're running from Revanta root directory")
        return
    
    print(f"✅ Found {len(csv_files)} CSV files")
    print("=" * 70)
    
    for csv_file in csv_files:
        try:
            # Read CSV
            print(f"⏳ Converting: {csv_file.name}...")
            df = pd.read_csv(csv_file)
            
            # Create XLSX filename
            xlsx_file = bi_exports_dir / f"{csv_file.stem}.xlsx"
            
            # Write XLSX
            df.to_excel(xlsx_file, index=False, engine='openpyxl')
            
            print(f"   ✅ Converted: {csv_file.name}")
            print(f"      → {xlsx_file.name}")
            print(f"      → Rows: {len(df):,} | Columns: {len(df.columns)}")
            print()
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            print()
    
    print("=" * 70)
    print("✅ All conversions complete!")
    print(f"📁 XLSX files ready in: {bi_exports_dir}")
    print()
    print("Next step: Use these XLSX files in Power BI!")

if __name__ == "__main__":
    convert_csv_to_xlsx()