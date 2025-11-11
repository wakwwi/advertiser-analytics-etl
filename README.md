# Advertiser Analytics ETL Pipeline  
### End-to-End Data Engineering Project (Python, Pandas, Logging, Validation)

This project implements a production-style ETL pipeline that processes raw marketplace data (Olist) and produces advertiser-level daily and monthly KPIs.  
It simulates real analytics workflows used in organizations such as Microsoft Advertising, Amazon Marketplace Analytics, LinkedIn Marketing Solutions, and retail media teams.

---

## 1. Project Overview

The pipeline performs:

- Extraction of raw CSV datasets  
- Cleaning and standardization of order data  
- Fact table creation at the advertiser level  
- KPI computation (daily + monthly)  
- Data validation using Pandera schemas  
- Logging for observability  
- Output of clean, analytics-ready tables  

This is structured like a real Data Engineering codebase and demonstrates core DE skills.

---

## 2. Project Goals

- Build a complete ETL workflow in Python  
- Clean and transform raw marketplace data  
- Implement a reusable, analytics-friendly fact table  
- Generate advertiser performance KPIs  
- Enforce data quality using validation schemas  
- Introduce logging for traceability  
- Create a maintainable Python package  
- Produce clear, reproducible output tables  

---

## 3. Repository Structure

```
ms_ad_analytics_project/
│
├── data/                       # Raw Olist datasets (.csv)
├── output/                     # Final KPI outputs
│
├── src/
│   ├── extract.py              # Load raw data
│   ├── transform.py            # Cleaning, fact table creation, KPIs
│   ├── load.py                 # Write output files
│   ├── validate.py             # Pandera schema validation
│   ├── logger.py               # Logging utilities
│   ├── config.py               # Config & settings
│   ├── pipeline.py             # Main ETL execution
│   └── __init__.py             # Package initializer
│
└── README.md
```

---

## 4. Environment Setup

### Create a Conda environment
```
conda create -n msad python=3.11 -y
conda activate msad
```

### Install dependencies
```
pip install pandas pandera pyarrow pytest python-dotenv
```

---

## 5. Running the Pipeline

From Anaconda Prompt:

```
conda activate msad
cd "%USERPROFILE%\OneDrive\Desktop\ms_ad_analytics_project"
python -m src.pipeline
```

Output files will appear in:

```
output/
│
├── daily_advertiser_kpis.csv
└── monthly_advertiser_kpis.csv
```

---

## 6. Data Model Summary

### Fact Table (Advertiser-Level)

| Column            | Description                          |
|------------------|--------------------------------------|
| advertiser_id     | Seller / advertiser ID               |
| order_id          | Unique order identifier              |
| customer_id       | Unique buyer ID                      |
| order_item_id     | Line item within the order           |
| order_date        | Daily granularity                    |
| order_month       | Monthly granularity (YYYY-MM)        |
| line_revenue      | price + freight                      |

### KPI Outputs

- **orders**: number of unique orders  
- **lines**: number of items sold  
- **revenue**: total revenue  
- **customers**: number of unique buyers  

---

## 7. Data Validation (Pandera)

The pipeline validates the fact table using Pandera schemas.  
Checks include:

- Required columns  
- Correct data types  
- Non-negative revenue  
- Valid timestamps  
- Monthly field in `YYYY-MM` format  
- No missing advertiser/order values  

If validation fails, the pipeline stops with detailed error messages.

---

## 8. Logging

Structured logging provides visibility into each stage:

```
INFO | Starting ETL pipeline...
INFO | Extract complete
INFO | Fact table validation passed
INFO | Transform complete
INFO | Load complete
```

This mirrors real production logging behavior.

---

## 9. Why This Project Matters

This project demonstrates real-world Data Engineering experience:

- End-to-end ETL development  
- Data cleaning and preprocessing  
- Fact table modeling  
- Validation and data quality enforcement  
- Logging and observability  
- Python packaging structure  
- Reproducible environment  
- Business-oriented analytics generation  

It follows patterns used by engineering teams at Microsoft, Amazon, LinkedIn, DoorDash, and Netflix.

---

## 10. Optional Future Enhancements

- CLI arguments (`--start` / `--end`)  
- Incremental loading with a watermark  
- DuckDB or dbt transformation layer  
- Scheduling with Airflow or Prefect  
- Power BI or Tableau dashboard  

---

## Author

**Errol Brown**  
Data Engineering & Analytics  
Microsoft Business Analytics Associate Candidate