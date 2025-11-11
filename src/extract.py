# extract.py
import pandas as pd
from pathlib import Path

def load_raw_data(data_path: Path):
    """
    Load all raw CSV files from the project /data directory.
    Returns a dictionary of DataFrames.
    """
    def load(name, parse_dates=None):
        fp = data_path / name
        if not fp.exists():
            raise FileNotFoundError(f"Missing file: {fp}")
        return pd.read_csv(fp, parse_dates=parse_dates)

    customers = load("olist_customers_dataset.csv")
    orders = load("olist_orders_dataset.csv",
                  parse_dates=[
                      "order_purchase_timestamp",
                      "order_approved_at",
                      "order_delivered_carrier_date",
                      "order_delivered_customer_date",
                      "order_estimated_delivery_date"
                  ])
    order_items = load("olist_order_items_dataset.csv")
    products = load("olist_products_dataset.csv")

    # Optional files
    try:
        cat_map = load("product_category_name_translation.csv")
    except:
        cat_map = None

    return {
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "products": products,
        "cat_map": cat_map
    }
