import streamlit as st
import psycopg2
import pandas as pd
from databricks.sdk import WorkspaceClient
from components.customer_selector import render_customer_selector
from components.db import get_connection

st.set_page_config(page_title="Active Offers", layout="wide")
st.header("Active Offers")

LAKEBASE_HOST = "instance-b431eef3-5aa5-4fb2-9342-644528625843.database.cloud.databricks.com"
LAKEBASE_DB = "databricks_postgres"
LAKEBASE_INSTANCE = "retail-state"


def get_lakebase_conn():
    w = WorkspaceClient()
    token_resp = w.api_client.do(
        "POST",
        "/api/2.0/database-instances/credential",
        body={"instance_names": [LAKEBASE_INSTANCE]},
    )
    token = token_resp.get("access_token", "")
    user = w.current_user.me().user_name
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        database=LAKEBASE_DB,
        user=user,
        password=token,
        port=5432,
        sslmode="require",
    )


sql_conn = get_connection()
selected = render_customer_selector(sql_conn)

customer_id = st.session_state.get("selected_customer", "")
customer_id = st.text_input("Customer ID", value=customer_id)

if customer_id:
    try:
        conn = get_lakebase_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT offer_id, offer_code, product_id, relevance_score,
                       offer_type, discount_pct, expires_at, created_at
                FROM personalized_offers
                WHERE customer_id = %s AND expires_at > NOW()
                ORDER BY relevance_score DESC
                """,
                [customer_id],
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

        if rows:
            df = pd.DataFrame(rows, columns=cols)
            st.dataframe(df, use_container_width=True)

            st.subheader("Offer Summary")
            c1, c2, c3 = st.columns(3)
            c1.metric("Active Offers", len(rows))
            c2.metric("Avg Relevance", f"{df['relevance_score'].mean():.2f}")
            c3.metric("Max Discount", f"{df['discount_pct'].max()}%")
        else:
            st.info("No active offers for this customer.")
    except Exception as e:
        st.error(f"Lakebase connection error: {e}")
