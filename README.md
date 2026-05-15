# E-Commerce Data Pipeline

End-to-end data engineering portfolio project — raw CSV data to a live analytics dashboard.

## Architecture

```
Kaggle Dataset (CSV)
       │
       ▼
Python Ingestion Script
       │
       ▼
Snowflake (RAW schema)
       │
       ▼
dbt Transformations
  ├── Staging Views (stg_orders, stg_customers, stg_products, ...)
  └── Mart Tables  (fct_orders, mart_sales_summary, mart_top_products)
       │
       ▼
Streamlit Dashboard
```

## Stack

| Layer | Tool |
|---|---|
| Data Source | Olist Brazilian E-Commerce (Kaggle) |
| Ingestion | Python, pandas, snowflake-connector-python |
| Storage | Snowflake |
| Transformation | dbt (Snowflake adapter) |
| Orchestration | Airflow (next phase) |
| Dashboard | Streamlit + Plotly |

## Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k+ orders across 7 tables:
- orders, order_items, order_payments, customers, products, sellers, order_reviews

## Project Structure

```
ecommerce-pipeline/
├── ingestion/
│   └── load_to_snowflake.py   # loads CSVs into Snowflake RAW schema
├── ecommerce_dbt/
│   └── models/
│       ├── staging/           # lightweight views over raw tables
│       └── marts/             # business-level tables for the dashboard
├── dashboard/
│   └── app.py                 # Streamlit dashboard
├── .streamlit/
│   └── config.toml
├── .env.example
└── requirements.txt
```

## Setup

### 1. Clone the repo
```bash
git clone <repo-url>
cd ecommerce-pipeline
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Snowflake
Copy `.env.example` to `.env` and fill in your credentials:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=ecommerce_pipeline
SNOWFLAKE_SCHEMA=raw
SNOWFLAKE_WAREHOUSE=ecommerce_wh
```

### 3. Run ingestion
```bash
python ingestion/load_to_snowflake.py
```

### 4. Run dbt transformations
```bash
cd ecommerce_dbt
dbt run
```

### 5. Launch dashboard
```bash
streamlit run dashboard/app.py
```

## Dashboard Metrics

- Total orders, revenue, avg order value, avg delivery days
- Monthly revenue and order trends
- Top 10 product categories by revenue
- Order status breakdown

## Key Results

- 550,000+ rows ingested across 7 tables
- 8 dbt models built (5 staging views + 3 mart tables)
- 23 months of sales data transformed and visualized
