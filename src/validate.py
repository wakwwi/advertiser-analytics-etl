import pandera as pa
from pandera import Column, Check, DataFrameSchema

# Schema for your fact table
fact_schema = DataFrameSchema({
    "advertiser_id": Column(object, nullable=False),
    "order_id": Column(object, nullable=False),
    "order_item_id": Column(int, nullable=False),
    "customer_id": Column(object, nullable=False),
    "order_date": Column(object, nullable=False),
    "order_month": Column(str, Check.str_matches(r"\d{4}-\d{2}")),
    "line_revenue": Column(float, Check.ge(0)),
})

def validate_fact(df):
    """
    Validates the fact table using Pandera schema checks.
    Raises an error if data quality issues are detected.
    """
    fact_schema.validate(df, lazy=True)
    return df
