"""Shared Streamlit layout helpers + global filter sidebar."""

from __future__ import annotations

from datetime import date, timedelta

import streamlit as st


# Color palette aligned with the executive dashboard SQL view tier labels.
RISK_COLORS = {
    "low": "#2ecc71",
    "moderate": "#f1c40f",
    "high": "#e67e22",
    "critical": "#e74c3c",
}


def apply_layout(*, page_title: str, page_icon: str = "🚚") -> None:
    """Page-level config; safe to call at the top of every page."""
    st.set_page_config(
        page_title=page_title,
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )


def render_sidebar_filters() -> None:
    """Global filters. Persisted in st.session_state so pages can read them."""
    with st.sidebar:
        st.header("Filters")

        # Date range — default last 30 days.
        today = date.today()
        default_start = today - timedelta(days=30)
        date_range = st.date_input(
            "Date range",
            value=(default_start, today),
            key="filter_date_range",
        )
        # Normalize tuple/list to (start, end).
        if isinstance(date_range, tuple) and len(date_range) == 2:
            st.session_state["filter_start"], st.session_state["filter_end"] = date_range

        # Tenant filter — populated from DB by individual pages, but we keep
        # the input here so it persists across navigation.
        tenants_text = st.text_input(
            "Tenant ids (comma-separated, blank = all)",
            value=st.session_state.get("filter_tenants_text", ""),
            key="filter_tenants_text",
        )
        st.session_state["filter_tenant_ids"] = [
            int(x.strip()) for x in tenants_text.split(",") if x.strip().isdigit()
        ]

        st.caption("Filters apply to every page.")
