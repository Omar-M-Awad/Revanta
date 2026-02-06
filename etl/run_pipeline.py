"""
Revanta ETL Pipeline Orchestrator
==================================

Main entry point for the complete ETL pipeline:
Extract → Transform → Load → SQL Transformations

Usage:
    python etl/run_pipeline.py
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.extract import extract_all
from etl.transform import transform_all
from etl.load import load_all
from database.run_sql_transformations import run_all_sql_transformations
from etl.export_for_bi import export_all

# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
LOG_DIR = Path(__file__).resolve().parents[0] / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# --------------------------------------------------
# Pipeline Execution
# --------------------------------------------------

def run_etl_pipeline() -> bool:
    """
    Execute the complete ETL pipeline.
    
    Steps:
        1. Extract raw data from CSV files
        2. Transform data (cleaning, standardization, validation)
        3. Load into SQLite staging tables
        4. Execute SQL transformations (staging → marts)
        5. Export marts to CSV for BI tools
    
    Returns:
        bool: True if pipeline succeeds, False if it fails
    """
    
    logger.info("=" * 80)
    logger.info("STARTING REVANTA ETL PIPELINE")
    logger.info("=" * 80)
    
    try:
        # ==========================================
        # STEP 1: EXTRACT
        # ==========================================
        logger.info("\n[STEP 1] EXTRACTING RAW DATA")
        logger.info("-" * 80)
        
        raw_data = extract_all()
        
        logger.info("Extraction complete:")
        logger.info(f"   Orders: {raw_data['orders'].shape[0]} rows")
        logger.info(f"   Customers: {raw_data['customers'].shape[0]} rows")
        logger.info(f"   Order Items: {raw_data['order_items'].shape[0]} rows")
        logger.info(f"   Products: {raw_data['products'].shape[0]} rows")
        logger.info(f"   Category Translation: {raw_data['category_translation'].shape[0]} rows")
        
        # ==========================================
        # STEP 2: TRANSFORM
        # ==========================================
        logger.info("\n[STEP 2] TRANSFORMING DATA")
        logger.info("-" * 80)
        
        transformed_data = transform_all(raw_data)
        
        logger.info("Transformation complete:")
        logger.info(f"   Orders: {transformed_data['orders'].shape[0]} rows")
        logger.info(f"   Customers: {transformed_data['customers'].shape[0]} rows")
        logger.info(f"   Order Items: {transformed_data['order_items'].shape[0]} rows")
        logger.info(f"   Products: {transformed_data['products'].shape[0]} rows")
        logger.info(f"   Category Translation: {transformed_data['category_translation'].shape[0]} rows")
        
        # ==========================================
        # STEP 3: LOAD
        # ==========================================
        logger.info("\n[STEP 3] LOADING TO STAGING TABLES")
        logger.info("-" * 80)
        
        load_all(transformed_data)
        
        logger.info("Loading complete - All data in staging tables")
        
        # ==========================================
        # STEP 4: SQL TRANSFORMATIONS (MARTS)
        # ==========================================
        logger.info("\n[STEP 4] EXECUTING SQL TRANSFORMATIONS")
        logger.info("-" * 80)
        
        run_all_sql_transformations()
        
        logger.info("SQL transformations complete - Marts ready for BI")
        
        # ==========================================
        # STEP 5: EXPORT FOR BI
        # ==========================================
        logger.info("\n[STEP 5] EXPORTING DATA FOR BI TOOLS")
        logger.info("-" * 80)
        
        export_all()
        
        logger.info("Export complete - CSV files ready for Power BI")
        
        # ==========================================
        # FINAL SUMMARY
        # ==========================================
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"Database: database/revanta.db")
        logger.info(f"BI Exports: bi_exports/")
        logger.info(f"Logs: {log_file}")
        logger.info("=" * 80)
        
        return True
    
    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("PIPELINE FAILED!")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error(f"Check log for details: {log_file}")
        logger.error("=" * 80)
        
        return False


def main():
    """
    Main entry point for the pipeline.
    """
    
    logger.info(f"Pipeline execution started at {datetime.now()}")
    
    success = run_etl_pipeline()
    
    if success:
        logger.info(f"Pipeline execution finished at {datetime.now()}")
        sys.exit(0)
    else:
        logger.error(f"Pipeline execution failed at {datetime.now()}")
        sys.exit(1)


if __name__ == "__main__":
    main()