import pandas as pd
from pathlib import Path
import logging

# ----------------------------
# Logging Configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# Paths
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

# ----------------------------
# Extract Functions
# ----------------------------
def extract_orders() -> pd.DataFrame:
    """
    Extract raw orders data.
    """
    file_path = RAW_DATA_DIR / "olist_orders_dataset.csv"
    logging.info("Extracting orders data...")
    df = pd.read_csv(file_path)
    logging.info(f"Orders extracted: {df.shape[0]} rows")
    return df


def extract_customers() -> pd.DataFrame:
    """
    Extract raw customers data.
    """
    file_path = RAW_DATA_DIR / "olist_customers_dataset.csv"
    logging.info("Extracting customers data...")
    df = pd.read_csv(file_path)
    logging.info(f"Customers extracted: {df.shape[0]} rows")
    return df


def extract_order_items() -> pd.DataFrame:
    """
    Extract raw order items data.
    """
    file_path = RAW_DATA_DIR / "olist_order_items_dataset.csv"
    logging.info("Extracting order items data...")
    df = pd.read_csv(file_path)
    logging.info(f"Order items extracted: {df.shape[0]} rows")
    return df


def extract_products() -> pd.DataFrame:
    """
    Extract raw products data.
    """
    file_path = RAW_DATA_DIR / "olist_products_dataset.csv"
    logging.info("Extracting products data...")
    df = pd.read_csv(file_path)
    logging.info(f"Products extracted: {df.shape[0]} rows")
    return df


def extract_category_translation() -> pd.DataFrame:
    """
    Extract product category name translation data.
    """
    file_path = RAW_DATA_DIR / "product_category_name_translation.csv"
    logging.info("Extracting product category translation data...")
    df = pd.read_csv(file_path)
    logging.info(f"Category translations extracted: {df.shape[0]} rows")
    return df


# ----------------------------
# Extract All
# ----------------------------
def extract_all() -> dict:
    """
    Extract all raw datasets and return them as a dictionary.
    """
    return {
        "orders": extract_orders(),
        "customers": extract_customers(),
        "order_items": extract_order_items(),
        "products": extract_products(),
        "category_translation": extract_category_translation()
    }


if __name__ == "__main__":
    extract_all()
