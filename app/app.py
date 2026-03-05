import streamlit as st

st.set_page_config(
    page_title="Retail Customer Portal",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Retail Customer Portal")
st.markdown(
    "Hyper-personalized loyalty engine powered by Databricks. "
    "Select a page from the sidebar to explore customer data."
)

st.sidebar.success("Select a page above.")

st.markdown("### Quick Links")
col1, col2, col3 = st.columns(3)
with col1:
    st.info("**Customer Lookup** — Search by ID or name")
with col2:
    st.info("**Recommendations** — AI-powered style suggestions")
with col3:
    st.info("**Intent Signals** — Real-time browsing interests")
