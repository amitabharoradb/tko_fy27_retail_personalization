"""Shared customer selector widget with filterable table and row selection."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"


def render_customer_selector(conn, *, expanded: bool = False) -> str | None:
    """Render a filterable customer table with single-row selection.

    Returns selected customer_id or None.
    """
    with st.expander("Browse & Select Customer", expanded=expanded):
        # Row 1: text filters
        c1, c2, c3, c4 = st.columns(4)
        f_first = c1.text_input("First name", key="_cs_first_name")
        f_last = c2.text_input("Last name", key="_cs_last_name")
        f_email = c3.text_input("Email", key="_cs_email")
        f_categories = c4.text_input("Preferred categories", key="_cs_categories")

        # Row 2: multiselects
        c5, c6, c7 = st.columns(3)
        f_age = c5.multiselect(
            "Age bucket", ["18-24", "25-34", "35-44", "45+"], key="_cs_age_bucket"
        )
        f_gender = c6.multiselect(
            "Gender",
            ["M", "F", "Non-binary", "Prefer not to say"],
            key="_cs_gender",
        )
        f_tier = c7.multiselect(
            "Loyalty tier",
            ["Bronze", "Silver", "Gold", "Platinum"],
            key="_cs_loyalty_tier",
        )

        # Row 3: sliders + date range
        c8, c9, c10 = st.columns(3)
        f_points = c8.slider(
            "Loyalty points", 0, 50000, (0, 50000), key="_cs_loyalty_points"
        )
        f_ltv = c9.slider("LTV score", 0.0, 100.0, (0.0, 100.0), key="_cs_ltv_score")
        today = date.today()
        f_dates = c10.date_input(
            "Last purchase date range",
            value=(today - timedelta(days=365), today),
            key="_cs_last_purchase_date",
        )

        if st.button("Search Customers", key="_cs_search"):
            conditions = []
            params = []

            if f_first:
                conditions.append("lower(first_name) LIKE %s")
                params.append(f"%{f_first.lower()}%")
            if f_last:
                conditions.append("lower(last_name) LIKE %s")
                params.append(f"%{f_last.lower()}%")
            if f_email:
                conditions.append("lower(email) LIKE %s")
                params.append(f"%{f_email.lower()}%")
            if f_categories:
                conditions.append("array_join(preferred_categories, ',') LIKE %s")
                params.append(f"%{f_categories}%")
            if f_age:
                placeholders = ",".join(["%s"] * len(f_age))
                conditions.append(f"age_bucket IN ({placeholders})")
                params.extend(f_age)
            if f_gender:
                placeholders = ",".join(["%s"] * len(f_gender))
                conditions.append(f"gender IN ({placeholders})")
                params.extend(f_gender)
            if f_tier:
                placeholders = ",".join(["%s"] * len(f_tier))
                conditions.append(f"loyalty_tier IN ({placeholders})")
                params.extend(f_tier)
            if f_points != (0, 50000):
                conditions.append("loyalty_points BETWEEN %s AND %s")
                params.extend([f_points[0], f_points[1]])
            if f_ltv != (0.0, 100.0):
                conditions.append("ltv_score BETWEEN %s AND %s")
                params.extend([f_ltv[0], f_ltv[1]])
            if isinstance(f_dates, (list, tuple)) and len(f_dates) == 2:
                conditions.append("last_purchase_date BETWEEN %s AND %s")
                params.extend([str(f_dates[0]), str(f_dates[1])])

            where = " AND ".join(conditions) if conditions else "1=1"
            query = (
                f"SELECT customer_id, first_name, last_name, email, age_bucket, "
                f"gender, loyalty_tier, loyalty_points, ltv_score, "
                f"preferred_categories, last_purchase_date "
                f"FROM {CATALOG}.{SCHEMA}.customer_profiles "
                f"WHERE {where} LIMIT 200"
            )

            with conn.cursor() as cur:
                cur.execute(query, params if params else None)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

            if rows:
                df = pd.DataFrame(rows, columns=cols)
                st.session_state["_cs_results"] = df
            else:
                st.session_state.pop("_cs_results", None)
                st.warning("No customers matched the filters.")

        # Display results table with row selection
        if "_cs_results" in st.session_state:
            df = st.session_state["_cs_results"]
            event = st.dataframe(
                df,
                use_container_width=True,
                on_select="rerun",
                selection_mode="single-row",
                key="_cs_table",
            )
            selected_rows = event.selection.rows
            if selected_rows:
                cid = df.iloc[selected_rows[0]]["customer_id"]
                st.session_state["selected_customer"] = cid
                st.success(f"Selected: {cid}")
                return cid

    return None
