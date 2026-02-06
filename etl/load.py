import sqlite3
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------------------------------
# Paths
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "database" / "revanta.db"

# --------------------------------------------------
# Load Functions
# --------------------------------------------------

def load_orders(df: pd.DataFrame, table_name: str = "stg_orders") -> None:
    """
    Load transformed orders data into SQLite staging table.
    
    Args:
        df: Transformed orders DataFrame
        table_name: Target table name (default: stg_orders)
    
    Returns:
        None
    """
    logging.info(f"Loading data into {table_name}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )
    
    logging.info(
        f"Loaded {df.shape[0]} records into {table_name}"
    )


def load_customers(df: pd.DataFrame, table_name: str = "stg_customers") -> None:
    """
    Load transformed customers data into SQLite staging table.
    
    Args:
        df: Transformed customers DataFrame
        table_name: Target table name (default: stg_customers)
    
    Returns:
        None
    """
    logging.info(f"Loading data into {table_name}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )
    
    logging.info(
        f"Loaded {df.shape[0]} records into {table_name}"
    )


def load_order_items(df: pd.DataFrame, table_name: str = "stg_order_items") -> None:
    """
    Load transformed order items data into SQLite staging table.
    
    Args:
        df: Transformed order items DataFrame
        table_name: Target table name (default: stg_order_items)
    
    Returns:
        None
    """
    logging.info(f"Loading data into {table_name}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )
    
    logging.info(
        f"Loaded {df.shape[0]} records into {table_name}"
    )


def load_products(df: pd.DataFrame, table_name: str = "stg_products") -> None:
    """
    Load transformed products data into SQLite staging table.
    
    Args:
        df: Transformed products DataFrame
        table_name: Target table name (default: stg_products)
    
    Returns:
        None
    """
    logging.info(f"Loading data into {table_name}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )
    
    logging.info(
        f"Loaded {df.shape[0]} records into {table_name}"
    )


def load_category_translation(df: pd.DataFrame, table_name: str = "stg_category_translation") -> None:
    """
    Load category translation data into SQLite staging table.
    
    Args:
        df: Category translation DataFrame
        table_name: Target table name (default: stg_category_translation)
    
    Returns:
        None
    """
    logging.info(f"Loading data into {table_name}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )
    
    logging.info(
        f"Loaded {df.shape[0]} records into {table_name}"
    )


# --------------------------------------------------
# Load All (Batch Loading)
# --------------------------------------------------

def load_all(transformed_data: dict) -> None:
    """
    Load all transformed dataframes into their respective staging tables.
    
    Args:
        transformed_data: Dictionary containing transformed DataFrames
                         Expected keys: orders, customers, order_items, products, category_translation
    
    Returns:
        None
    
    Example:
        >>> from extract import extract_all
        >>> from transform import transform_orders, transform_customers, ...
        >>> from load import load_all
        >>> 
        >>> raw_data = extract_all()
        >>> transformed = {
        ...     "orders": transform_orders(raw_data["orders"]),
        ...     "customers": transform_customers(raw_data["customers"]),
        ...     "order_items": transform_order_items(raw_data["order_items"]),
        ...     "products": transform_products(raw_data["products"]),
        ...     "category_translation": raw_data["category_translation"]
        ... }
        >>> load_all(transformed)
    """
    logging.info("=" * 60)
    logging.info("Starting batch load operation...")
    logging.info("=" * 60)
    
    try:
        # Load orders
        if "orders" in transformed_data:
            load_orders(transformed_data["orders"])
        
        # Load customers
        if "customers" in transformed_data:
            load_customers(transformed_data["customers"])
        
        # Load order items
        if "order_items" in transformed_data:
            load_order_items(transformed_data["order_items"])
        
        # Load products
        if "products" in transformed_data:
            load_products(transformed_data["products"])
        
        # Load category translation
        if "category_translation" in transformed_data:
            load_category_translation(transformed_data["category_translation"])
        
        logging.info("=" * 60)
        logging.info("✅ All data loaded successfully!")
        logging.info("=" * 60)
    
    except Exception as e:
        logging.error(f"❌ Error during batch load: {str(e)}")
        raise


if __name__ == "__main__":
    logging.info("Load module initialized. Use load_all() or individual load functions.")