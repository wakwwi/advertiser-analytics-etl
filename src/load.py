# src/load.py
from pathlib import Path
import pandas as pd

def save_outputs(daily_df: pd.DataFrame, monthly_df: pd.DataFrame, out_path: Path):
    out_path.mkdir(exist_ok=True)
    daily_fp   = out_path / "daily_advertiser_kpis.csv"
    monthly_fp = out_path / "monthly_advertiser_kpis.csv"
    daily_df.to_csv(daily_fp, index=False)
    monthly_df.to_csv(monthly_fp, index=False)
    print(f"✅ Wrote {daily_fp} ({len(daily_df):,} rows)")
    print(f"✅ Wrote {monthly_fp} ({len(monthly_df):,} rows)")
