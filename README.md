# 📊 Advertiser Analytics ETL Pipeline  
### End-to-End Data Engineering Project (Python, Pandas, Logging, Validation)

This project builds a **production-style ETL pipeline** that processes raw marketplace data (Olist dataset) and transforms it into **analytics-ready advertiser KPIs**.  
It is structured like a real engineering codebase used at Microsoft Advertising, Amazon Marketplace Analytics, and other retail media platforms.

---

## 🚀 Project Summary

This pipeline:

✅ Loads raw marketplace data (orders, items, customers, products)  
✅ Cleans & validates the datasets  
✅ Builds an advertiser-level **fact table**  
✅ Computes daily & monthly KPIs  
✅ Enforces data quality with **Pandera schema validation**  
✅ Logs every stage using a production-style logger  
✅ Outputs clean CSVs ready for BI dashboards or analytics  

The structure follows real DE standards with a modular **src/** package.

---

## 🧱 Tech Stack

- **Python 3.11**
- **Pandas** (data transformation)
- **Pandera** (data validation)
- **Logging** (observability)
- **Pathlib** (file handling)
- **Conda environment** (reproducibility)

---

## 📁 Project Structure

```
ms_ad_analytics_project/
│
├── data/                           # Raw input CSVs (ignored by Git)
├── output/                         # Final KPI outputs
│
├── src/
│   ├── extract.py                  # Extract step
│   ├── transform.py                # Clean, merge, build fact table
│   ├── validate.py                 # Pandera schemas
│   ├── load.py                     # Save outputs
│   ├── logger.py                   # Custom logger
│   ├── config.py                   # Config + log level
│   ├── pipeline.py                 # Main ETL pipeline
│   └── __init__.py
│
├── advertiser_spend_analytics.ipynb # Notebook version
├── .gitignore
└── README.md
```

---

## 🏗️ Pipeline Architecture (ASCII Diagram)

```
Raw CSV Files
    │
    ▼
[ Extract ]
    │
    ▼
[ Clean & Normalize Orders ]
    │
    ▼
[ Build Advertiser Fact Table ]
    │
    ▼
[ Validate Data (Pandera) ]
    │
    ▼
[ Compute KPIs ]
  • Daily KPIs
  • Monthly KPIs
    │
    ▼
[ Load Outputs to /output ]
  • daily_advertiser_kpis.csv
  • monthly_advertiser_kpis.csv
```

---

## 📊 KPIs Produced

Each advertiser receives:

| Metric | Description |
|--------|-------------|
| **orders** | Unique order count |
| **lines** | Items sold |
| **revenue** | price + freight (line-level revenue) |
| **customers** | Unique buyers |

Outputs are available at two time grains:

✅ **Daily KPIs**  
✅ **Monthly KPIs**

---

## ✅ Fact Table (Advertiser-Level)

Key columns include:

- `advertiser_id` (seller)  
- `order_id`  
- `customer_id`  
- `order_item_id`  
- `order_date`  
- `order_month`  
- `line_revenue`  

This mirrors a real **fact_sales** table used in enterprise analytics.

---

## ⚙️ How to Run the Pipeline

### 1. Create & activate environment  
```
conda create -n msad python=3.11 -y
conda activate msad
```

### 2. Install dependencies  
```
pip install pandas pandera pyarrow python-dotenv pytest
```

### 3. Run the pipeline  
```
python -m src.pipeline
```

### 4. Outputs will appear here:  
```
output/daily_advertiser_kpis.csv
output/monthly_advertiser_kpis.csv
```

---

## 🧪 Data Validation with Pandera

The fact table is validated using a schema that checks:

- Column presence  
- Data types  
- Non-negative revenue  
- Valid advertiser/order/customer IDs  
- Monthly format correctness (`YYYY-MM`)  
- No invalid timestamps  

If validation fails, the pipeline exits — this matches production behavior.

---

## 📜 Logging (Production-Style)

Example log:

```
INFO | Starting ETL pipeline...
INFO | Extract completed successfully.
INFO | Fact table validation passed.
INFO | KPI computation complete.
INFO | Load complete. Files saved to /output.
```

---

## 🎯 Why This Project Matters

This project demonstrates skills required for:

- **Data Engineering**  
- **Analytics Engineering**  
- **Business Analytics**  
- **Data Analytics**

Key competencies you demonstrate:

- ETL design  
- Fact table modeling  
- Data cleaning / normalization  
- KPI engineering  
- Validation & error handling  
- Modular Python code  
- Logging & observability  
- Reproducible environments  

---

## 🌱 Future Enhancements (Optional)

- Add incremental loading (watermark-based)  
- Add pytest unit tests  
- Convert transformations to DuckDB or dbt  
- Schedule using Airflow or Prefect  
- Add a Power BI or Tableau dashboard  

---

## 👤 Author  
**Errol Brown**  
Data Engineering / Analytics  
