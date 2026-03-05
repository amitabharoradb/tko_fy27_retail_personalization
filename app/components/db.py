"""Shared DBSQL connection — cached once across all pages."""

import os

import streamlit as st
from databricks import sql
from databricks.sdk.core import Config


@st.cache_resource
def get_connection():
    cfg = Config()
    token = st.context.headers.get("x-forwarded-access-token")
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        access_token=token,
    )
