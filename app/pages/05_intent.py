import time
import streamlit as st
import pandas as pd
from components.customer_selector import render_customer_selector
from components.db import get_connection

st.set_page_config(page_title="Intent Signals", layout="wide")
st.header("Intent Signals")

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"

conn = get_connection()
selected = render_customer_selector(conn)

customer_id = st.session_state.get("selected_customer", "")
customer_id = st.text_input("Customer ID", value=customer_id)

placeholder = st.empty()

if customer_id:
    conn = get_connection()

    # Auto-refresh loop
    while True:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT category, intent_score, rank, updated_at
                FROM {CATALOG}.{SCHEMA}.customer_current_interests
                WHERE customer_id = %s
                ORDER BY rank
                """,
                [customer_id],
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

        with placeholder.container():
            if rows:
                df = pd.DataFrame(rows, columns=cols)
                st.subheader("Currently Interested In")
                for _, r in df.iterrows():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    col1.write(f"**#{int(r['rank'])} {r['category']}**")
                    col2.write(f"Score: {r['intent_score']:.2f}")
                    col3.write(f"Updated: {r['updated_at']}")

                st.bar_chart(df.set_index("category")["intent_score"])
            else:
                st.info("No intent signals found for this customer.")

            st.caption(f"Last refreshed: {time.strftime('%H:%M:%S')} (auto-refreshes every 10s)")

        time.sleep(10)
        st.rerun()
