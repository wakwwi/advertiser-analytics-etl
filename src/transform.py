import pandas as pd

# ---------------------------------------------------------
# CLEAN ORDERS
# ---------------------------------------------------------
def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Convert timestamps to datetime and filter valid statuses.
    Creates order_date and order_month fields.
    """
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for c in date_cols:
        if c in orders.columns:
            orders[c] = pd.to_datetime(orders[c], errors="coerce")

    valid_status = orders["order_status"].isin(
        ["delivered", "shipped", "invoiced", "approved"]
    )

    orders_clean = orders.loc[valid_status].copy()
    orders_clean["order_date"] = orders_clean["order_purchase_timestamp"].dt.date
    orders_clean["order_month"] = (
        orders_clean["order_purchase_timestamp"]
        .dt.to_period("M")
        .astype(str)
    )

    return orders_clean


# ---------------------------------------------------------
# BUILD FACT TABLE
# ---------------------------------------------------------
def build_fact_table(order_items, orders_clean, products, cat_map=None):
    """
    Merge order_items + orders_clean + products.
    Create category mapping & revenue fields.
    """
    items = order_items.merge(
        orders_clean[
            ["order_id", "customer_id", "order_purchase_timestamp", "order_status"]
        ],
        on="order_id",
        how="inner",
    )

    # Attach product category
    items = items.merge(
        products[["product_id", "product_category_name"]],
        on="product_id",
        how="left",
    )

    if cat_map is not None and "product_category_name_english" in cat_map.columns:
        items = items.merge(cat_map, on="product_category_name", how="left")
        items["category"] = items["product_category_name_english"].fillna(
            items["product_category_name"]
        )
    else:
        items["category"] = items["product_category_name"]

    items["order_date"] = pd.to_datetime(
        items["order_purchase_timestamp"]
    ).dt.date

    items["order_month"] = (
        pd.to_datetime(items["order_purchase_timestamp"])
        .dt.to_period("M")
        .astype(str)
    )

    # Revenue
    items["line_revenue"] = items["price"] + items.get("freight_value", 0)

    # Rename for advertiser
    fact = items.rename(columns={"seller_id": "advertiser_id"}).copy()
    return fact


# ---------------------------------------------------------
# COMPUTE KPIs
# ---------------------------------------------------------
def compute_kpis(fact: pd.DataFrame) -> dict:
    """
    Compute daily & monthly advertiser-level KPIs.
    Returns dict with dataframes.
    """
    # Daily
    daily = (
        fact.groupby(["advertiser_id", "order_date"], as_index=False)
        .agg(
            orders=("order_id", "nunique"),
            lines=("order_item_id", "count"),
            revenue=("line_revenue", "sum"),
            customers=("customer_id", "nunique"),
        )
    )

    # Monthly
    monthly = (
        fact.groupby(["advertiser_id", "order_month"], as_index=False)
        .agg(
            orders=("order_id", "nunique"),
            lines=("order_item_id", "count"),
            revenue=("line_revenue", "sum"),
            customers=("customer_id", "nunique"),
        )
    )

    return {
        "daily_kpis": daily,
        "monthly_kpis": monthly,
    }
