import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------------------------------
# Orders Transformation
# --------------------------------------------------
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize orders data.
    This function performs data hygiene only (no business logic).
    Grain: 1 row = 1 order
    """

    logging.info("Starting orders transformation...")

    df = df.copy()

    # 1️⃣ Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    # 2️⃣ Cast datetime columns
    datetime_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # 3️⃣ Filter valid order statuses
    valid_statuses = [
        "delivered",
        "shipped",
        "approved"
    ]

    initial_rows = df.shape[0]
    df = df[df["order_status"].isin(valid_statuses)]
    logging.info(
        f"Filtered orders by status: {initial_rows} → {df.shape[0]}"
    )

    # 4️⃣ Remove duplicate orders
    before_dedup = df.shape[0]
    df = df.drop_duplicates(subset=["order_id"])
    logging.info(
        f"Deduplicated orders: {before_dedup} → {df.shape[0]}"
    )

    # 5️⃣ Enforce grain & integrity checks
    assert df["order_id"].is_unique, (
        "order_id is not unique after transformation"
    )

    logging.info("Orders transformation completed successfully")

    return df


# --------------------------------------------------
# Customers Transformation
# --------------------------------------------------
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize customers data.
    Data hygiene only – no business logic.
    Grain: 1 row = 1 customer_unique_id
    """

    logging.info("Starting customers transformation...")

    df = df.copy()

    # 1️⃣ Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    # 2️⃣ Select relevant columns
    required_columns = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ]

    df = df[required_columns]

    # 3️⃣ Remove duplicate customers
    before_dedup = df.shape[0]
    df = df.drop_duplicates(subset=["customer_unique_id"])
    logging.info(
        f"Deduplicated customers: {before_dedup} → {df.shape[0]}"
    )

    # 4️⃣ Enforce grain & integrity checks
    assert df["customer_unique_id"].is_unique, (
        "customer_unique_id is not unique after transformation"
    )

    logging.info("Customers transformation completed successfully")

    return df


# --------------------------------------------------
# Order Items Transformation
# --------------------------------------------------
def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize order items data.
    Data hygiene only – no business logic.
    Grain: 1 row = 1 order_item (order can have multiple items)
    """

    logging.info("Starting order items transformation...")

    df = df.copy()

    # 1️⃣ Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    # 2️⃣ Select relevant columns
    required_columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    ]

    df = df[required_columns]

    # 3️⃣ Cast datetime columns
    df["shipping_limit_date"] = pd.to_datetime(
        df["shipping_limit_date"],
        errors="coerce"
    )

    # 4️⃣ Validate price and freight values
    initial_rows = df.shape[0]
    df = df[
        (df["price"] > 0) &
        (df["freight_value"] >= 0)
    ]
    logging.info(
        f"Filtered by valid price/freight: {initial_rows} → {df.shape[0]}"
    )

    # 5️⃣ Calculate total value per item
    df["item_total_value"] = df["price"] + df["freight_value"]

    # 6️⃣ Remove duplicates (order_id + order_item_id combination should be unique)
    before_dedup = df.shape[0]
    df = df.drop_duplicates(
        subset=["order_id", "order_item_id"],
        keep="first"
    )
    logging.info(
        f"Deduplicated order items: {before_dedup} → {df.shape[0]}"
    )

    # 7️⃣ Enforce grain & integrity checks
    assert (df["price"] > 0).all(), "Found non-positive prices"
    assert (df["freight_value"] >= 0).all(), "Found negative freight values"

    logging.info("Order items transformation completed successfully")

    return df


# --------------------------------------------------
# Products Transformation
# --------------------------------------------------
def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize products data.
    Data hygiene only – no business logic.
    Grain: 1 row = 1 product_id
    """

    logging.info("Starting products transformation...")

    df = df.copy()

    # 1️⃣ Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    # 2️⃣ Select relevant columns
    required_columns = [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    df = df[required_columns]

    # 3️⃣ Handle missing values in product_category_name
    initial_rows = df.shape[0]
    df = df.dropna(subset=["product_category_name"])
    logging.info(
        f"Removed rows with missing category: {initial_rows} → {df.shape[0]}"
    )

    # 4️⃣ Fill missing numeric values with 0 (they represent physical attributes)
    numeric_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    for col in numeric_cols:
        df[col] = df[col].fillna(0)

    # 5️⃣ Convert numeric columns to correct dtypes
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 6️⃣ Calculate product volume (for potential analysis)
    df["product_volume_cm3"] = (
        df["product_length_cm"] *
        df["product_height_cm"] *
        df["product_width_cm"]
    )

    # 7️⃣ Remove duplicate products
    before_dedup = df.shape[0]
    df = df.drop_duplicates(subset=["product_id"])
    logging.info(
        f"Deduplicated products: {before_dedup} → {df.shape[0]}"
    )

    # 8️⃣ Enforce grain & integrity checks
    assert df["product_id"].is_unique, (
        "product_id is not unique after transformation"
    )
    assert (df[numeric_cols] >= 0).all().all(), (
        "Found negative values in numeric columns"
    )

    logging.info("Products transformation completed successfully")

    return df


# --------------------------------------------------
# Category Translation Transformation
# --------------------------------------------------
def transform_category_translation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize product category translation data.
    Data hygiene only – no business logic.
    Grain: 1 row = 1 category (Portuguese → English mapping)
    """

    logging.info("Starting category translation transformation...")

    df = df.copy()

    # 1️⃣ Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    # 2️⃣ Select relevant columns
    required_columns = [
        "product_category_name",
        "product_category_name_english"
    ]

    df = df[required_columns]

    # 3️⃣ Remove rows with missing translations
    initial_rows = df.shape[0]
    df = df.dropna(subset=required_columns)
    logging.info(
        f"Removed rows with missing translations: {initial_rows} → {df.shape[0]}"
    )

    # 4️⃣ Remove duplicate categories
    before_dedup = df.shape[0]
    df = df.drop_duplicates(subset=["product_category_name"])
    logging.info(
        f"Deduplicated categories: {before_dedup} → {df.shape[0]}"
    )

    # 5️⃣ Enforce grain & integrity checks
    assert df["product_category_name"].is_unique, (
        "product_category_name is not unique after transformation"
    )

    logging.info("Category translation transformation completed successfully")

    return df


# --------------------------------------------------
# Transform All (Batch Transformation)
# --------------------------------------------------
def transform_all(raw_data: dict) -> dict:
    """
    Transform all raw dataframes using their respective transformation functions.

    Args:
        raw_data: Dictionary containing raw DataFrames
                 Expected keys: orders, customers, order_items, products, category_translation

    Returns:
        Dictionary containing transformed DataFrames with same keys

    Example:
        >>> from extract import extract_all
        >>> from transform import transform_all
        >>> 
        >>> raw_data = extract_all()
        >>> transformed_data = transform_all(raw_data)
    """

    logging.info("=" * 60)
    logging.info("Starting batch transformation operation...")
    logging.info("=" * 60)

    transformed_data = {}

    try:
        # Transform orders
        if "orders" in raw_data:
            transformed_data["orders"] = transform_orders(raw_data["orders"])

        # Transform customers
        if "customers" in raw_data:
            transformed_data["customers"] = transform_customers(raw_data["customers"])

        # Transform order items
        if "order_items" in raw_data:
            transformed_data["order_items"] = transform_order_items(raw_data["order_items"])

        # Transform products
        if "products" in raw_data:
            transformed_data["products"] = transform_products(raw_data["products"])

        # Transform category translation
        if "category_translation" in raw_data:
            transformed_data["category_translation"] = transform_category_translation(
                raw_data["category_translation"]
            )

        logging.info("=" * 60)
        logging.info("✅ All transformations completed successfully!")
        logging.info("=" * 60)

        return transformed_data

    except Exception as e:
        logging.error(f"❌ Error during batch transformation: {str(e)}")
        raise


if __name__ == "__main__":
    logging.info("Transform module initialized. Use transform_all() or individual transform functions.")