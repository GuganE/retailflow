import os
import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def get_secret(key):
    # Streamlit Cloud secrets take priority, fall back to .env for local dev
    if key in st.secrets:
        return st.secrets[key]
    return os.getenv(key)

st.set_page_config(page_title="E-Commerce Pipeline Dashboard", layout="wide")

@st.cache_data(ttl=600)
def run_query(sql):
    conn = snowflake.connector.connect(
        account=get_secret("SNOWFLAKE_ACCOUNT"),
        user=get_secret("SNOWFLAKE_USER"),
        password=get_secret("SNOWFLAKE_PASSWORD"),
        database=get_secret("SNOWFLAKE_DATABASE"),
        warehouse=get_secret("SNOWFLAKE_WAREHOUSE"),
        role="SYSADMIN",
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {get_secret('SNOWFLAKE_WAREHOUSE')}")
    cur.execute(sql)
    df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    conn.close()
    return df


# ── Header ────────────────────────────────────────────────────────────────────
st.title("Brazilian E-Commerce Pipeline")
st.caption("End-to-end DE project · Python → Snowflake → dbt → Streamlit")
st.divider()

# ── KPI Cards ─────────────────────────────────────────────────────────────────
summary = run_query("""
    select
        sum(total_orders)      as total_orders,
        sum(total_revenue)     as total_revenue,
        avg(avg_order_value)   as avg_order_value,
        avg(avg_delivery_days) as avg_delivery_days
    from ecommerce_pipeline.staging_marts.mart_sales_summary
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders",     f"{int(summary['TOTAL_ORDERS'][0]):,}")
col2.metric("Total Revenue",    f"${summary['TOTAL_REVENUE'][0]:,.0f}")
col3.metric("Avg Order Value",  f"${summary['AVG_ORDER_VALUE'][0]:,.2f}")
col4.metric("Avg Delivery Days",f"{summary['AVG_DELIVERY_DAYS'][0]:.1f} days")

st.divider()

# ── Revenue Over Time ──────────────────────────────────────────────────────────
monthly = run_query("""
    select month, total_orders, total_revenue, avg_order_value
    from ecommerce_pipeline.staging_marts.mart_sales_summary
    order by month
""")
monthly["MONTH"] = pd.to_datetime(monthly["MONTH"])

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Monthly Revenue")
    fig = px.bar(monthly, x="MONTH", y="TOTAL_REVENUE",
                 labels={"MONTH": "Month", "TOTAL_REVENUE": "Revenue ($)"},
                 color_discrete_sequence=["#29B5E8"])
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Monthly Orders")
    fig2 = px.line(monthly, x="MONTH", y="TOTAL_ORDERS",
                   labels={"MONTH": "Month", "TOTAL_ORDERS": "Orders"},
                   markers=True, color_discrete_sequence=["#FF6B6B"])
    st.plotly_chart(fig2, use_container_width=True)

# ── Top Product Categories ─────────────────────────────────────────────────────
st.subheader("Top 10 Product Categories by Revenue")
top_products = run_query("""
    select
        coalesce(product_category_name, 'Unknown') as category,
        sum(total_revenue)  as revenue,
        sum(total_orders)   as orders
    from ecommerce_pipeline.staging_marts.mart_top_products
    group by 1
    order by revenue desc
    limit 10
""")

fig3 = px.bar(top_products, x="REVENUE", y="CATEGORY", orientation="h",
              labels={"REVENUE": "Revenue ($)", "CATEGORY": "Category"},
              color="REVENUE", color_continuous_scale="Blues")
fig3.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig3, use_container_width=True)

# ── Order Status Breakdown ─────────────────────────────────────────────────────
st.subheader("Order Status Breakdown")
status = run_query("""
    select order_status, count(*) as order_count
    from ecommerce_pipeline.staging_marts.fct_orders
    group by 1
    order by 2 desc
""")

fig4 = px.pie(status, names="ORDER_STATUS", values="ORDER_COUNT",
              color_discrete_sequence=px.colors.sequential.Blues_r)
st.plotly_chart(fig4, use_container_width=True)
