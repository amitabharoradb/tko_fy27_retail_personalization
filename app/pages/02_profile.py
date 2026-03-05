import os
import streamlit as st
from databricks.sdk.core import Config
from databricks import sql

st.set_page_config(page_title="Profile & Loyalty", layout="wide")
st.header("Profile & Loyalty")

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


customer_id = st.session_state.get("selected_customer", "")
customer_id = st.text_input("Customer ID", value=customer_id)

if customer_id:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {CATALOG}.{SCHEMA}.customer_profiles WHERE customer_id = %s",
            [customer_id],
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]

    if row:
        data = dict(zip(cols, row))
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Loyalty Tier", data.get("loyalty_tier", "N/A"))
        col2.metric("Points", f"{data.get('loyalty_points', 0):,}")
        col3.metric("LTV Score", f"{data.get('ltv_score', 0):.1f}")
        col4.metric("High Value", "Yes" if data.get("is_high_value") else "No")

        st.subheader("Profile Details")
        st.json({
            "customer_id": data.get("customer_id"),
            "age_bucket": data.get("age_bucket"),
            "gender": data.get("gender"),
            "preferred_categories": data.get("preferred_categories"),
            "last_purchase_date": str(data.get("last_purchase_date")),
        })

        # Purchase summary
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) AS cnt, sum(total_amount) AS total "
                f"FROM {CATALOG}.{SCHEMA}.purchase_history WHERE customer_id = %s",
                [customer_id],
            )
            prow = cur.fetchone()
        if prow:
            st.subheader("Purchase Summary")
            c1, c2 = st.columns(2)
            c1.metric("Total Orders", prow[0])
            c2.metric("Total Spend", f"${prow[1]:,.2f}" if prow[1] else "$0")
    else:
        st.warning("Customer not found.")
