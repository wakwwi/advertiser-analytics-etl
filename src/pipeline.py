# src/pipeline.py
from pathlib import Path
from src.extract import load_raw_data
from src.transform import clean_orders, build_fact_table, compute_kpis
from src.load import save_outputs
from src.logger import get_logger
from src.config import LOG_LEVEL
from src.validate import validate_fact


log = get_logger("pipeline", LOG_LEVEL)

PROJ = Path(__file__).resolve().parents[1]
DATA = PROJ / "data"
OUT  = PROJ / "output"

def run_pipeline():
    log.info("🚀 Starting ETL pipeline...")
    log.info(f"Project folder: {PROJ}")

    # EXTRACT
    tables = load_raw_data(DATA)
    log.info("Extract complete")

    # TRANSFORM
    orders_clean = clean_orders(tables["orders"])
    fact = build_fact_table(
        tables["order_items"],
        orders_clean,
        tables["products"],
        tables.get("cat_map")
    )
    fact = validate_fact(fact)
    log.info("Fact table validation passed")
    kpis = compute_kpis(fact)
    log.info("Transform complete")

    # LOAD
    save_outputs(kpis["daily_kpis"], kpis["monthly_kpis"], OUT)
    log.info("Load complete")

if __name__ == "__main__":
    run_pipeline()
