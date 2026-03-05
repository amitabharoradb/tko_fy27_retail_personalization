import os
import streamlit as st
from databricks.sdk.core import Config
from databricks import sql

st.set_page_config(page_title="Customer Lookup", layout="wide")
st.header("Customer Lookup")

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"


@st.cache_resource
def get_connection():
    cfg = Config()
    token = st.context.headers.get("x-forwarded-access-token")
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        access_token=token,
    )


search_type = st.radio("Search by", ["Customer ID", "Name"], horizontal=True)
search_val = st.text_input(
    "Enter customer ID (e.g. CUST-00001)" if search_type == "Customer ID"
    else "Enter first or last name"
)

if st.button("Search") and search_val:
    conn = get_connection()
    with conn.cursor() as cur:
        if search_type == "Customer ID":
            cur.execute(
                f"SELECT * FROM {CATALOG}.{SCHEMA}.customer_profiles "
                f"WHERE customer_id = %s",
                [search_val.strip()],
            )
        else:
            cur.execute(
                f"SELECT * FROM {CATALOG}.{SCHEMA}.customer_profiles "
                f"WHERE lower(first_name) LIKE %s OR lower(last_name) LIKE %s",
                [f"%{search_val.lower()}%", f"%{search_val.lower()}%"],
            )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    if rows:
        import pandas as pd
        df = pd.DataFrame(rows, columns=cols)
        st.dataframe(df, use_container_width=True)
        if len(rows) == 1:
            st.session_state["selected_customer"] = rows[0][cols.index("customer_id")]
            st.success(f"Selected: {st.session_state['selected_customer']}")
    else:
        st.warning("No customers found.")
