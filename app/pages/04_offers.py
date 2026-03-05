import os
import streamlit as st
import psycopg2
import pandas as pd

st.set_page_config(page_title="Active Offers", layout="wide")
st.header("Active Offers")


@st.cache_resource
def get_lakebase_conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        database=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        port=os.getenv("PGPORT", "5432"),
    )


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
