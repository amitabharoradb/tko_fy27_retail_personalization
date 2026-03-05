import json
import requests
import streamlit as st
from databricks.sdk.core import Config
from components.customer_selector import render_customer_selector
from components.db import get_connection

st.set_page_config(page_title="Recommendations", layout="wide")
st.header("Style Recommendations")

ENDPOINT_NAME = "style-assistant-endpoint"

conn = get_connection()
selected = render_customer_selector(conn)

customer_id = st.session_state.get("selected_customer", "")
customer_id = st.text_input("Customer ID", value=customer_id)

if st.button("Get Recommendations") and customer_id:
    cfg = Config()
    token = st.context.headers.get("x-forwarded-access-token")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    with st.spinner("Generating recommendations..."):
        resp = requests.post(
            f"https://{cfg.host}/serving-endpoints/{ENDPOINT_NAME}/invocations",
            headers=headers,
            json={
                "input": [{"role": "user", "content": customer_id}],
            },
            timeout=30,
        )

    if resp.status_code == 200:
        result = resp.json()
        # Extract text from ResponsesAgent output
        output_items = result.get("output", [])
        for item in output_items:
            content = item.get("content", [{}])
            if isinstance(content, list):
                for c in content:
                    text = c.get("text", "")
                    if text:
                        try:
                            recs = json.loads(text)
                            st.success(recs.get("reasoning", ""))
                            st.subheader("Recommended Products")
                            for p in recs.get("recommended_products", []):
                                with st.container():
                                    c1, c2, c3 = st.columns([2, 1, 3])
                                    c1.write(f"**{p['name']}**")
                                    c2.write(f"${p['price']:.2f}")
                                    c3.write(p["reasoning"])
                        except json.JSONDecodeError:
                            st.markdown(text)
    else:
        st.error(f"Error {resp.status_code}: {resp.text}")
