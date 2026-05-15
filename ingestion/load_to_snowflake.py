import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

DATASETS = {
    "orders":          "data/olist_orders_dataset.csv",
    "order_items":     "data/olist_order_items_dataset.csv",
    "order_payments":  "data/olist_order_payments_dataset.csv",
    "customers":       "data/olist_customers_dataset.csv",
    "products":        "data/olist_products_dataset.csv",
    "sellers":         "data/olist_sellers_dataset.csv",
    "order_reviews":   "data/olist_order_reviews_dataset.csv",
}


def get_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    conn.cursor().execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    return conn


def create_table_from_df(cursor, table_name, df):
    type_map = {
        "int64": "NUMBER",
        "float64": "FLOAT",
        "object": "VARCHAR",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP_NTZ",
    }
    cols = ", ".join(
        f"{col} {type_map.get(str(df[col].dtype), 'VARCHAR')}"
        for col in df.columns
    )
    cursor.execute(f"CREATE OR REPLACE TABLE {table_name} ({cols})")


def load_table(cursor, table_name, df):
    print(f"  Loading {table_name} ({len(df):,} rows)...")
    create_table_from_df(cursor, table_name, df)

    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

    rows = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False)]
    cursor.executemany(insert_sql, rows)
    print(f"  Done: {table_name}")


def main():
    print("Connecting to Snowflake...")
    conn = get_connection()
    cursor = conn.cursor()

    for table_name, filepath in DATASETS.items():
        if not os.path.exists(filepath):
            print(f"  Skipping {table_name} — file not found: {filepath}")
            continue
        df = pd.read_csv(filepath)
        load_table(cursor, table_name, df)

    cursor.close()
    conn.close()
    print("\nAll tables loaded successfully.")


if __name__ == "__main__":
    main()
