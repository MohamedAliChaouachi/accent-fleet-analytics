"""
Streamlit landing page.

Multi-page apps in Streamlit: any .py file under dashboard/pages/ becomes a
sidebar link automatically. This Home file is just the welcome screen plus
the global filter side-bar (tenant / date range).
"""

from __future__ import annotations

import streamlit as st

from dashboard.lib.cache import last_etl_run_at
from dashboard.lib.db import get_engine
from dashboard.lib.theme import apply_layout, render_sidebar_filters


def main() -> None:
    apply_layout(page_title="Accent Fleet Analytics", page_icon="🚚")
    render_sidebar_filters()

    st.title("Accent Fleet Analytics")
    st.markdown(
        """
Welcome. Use the left sidebar to navigate:

| Page | Source |
|---|---|
| **Executive overview** | `marts.v_executive_dashboard` |
| **Operations** | `marts.v_operational_dashboard` |
| **Maintenance** | `marts.v_maintenance_dashboard` |
| **Risk & behavior** | `marts.v_fleet_risk_dashboard`, `marts.v_device_risk_profile` |
| **What-if scoring** | `POST /score/risk` on the FastAPI service |
"""
    )

    # Connectivity panel — quick at-a-glance for ops.
    col_db, col_run = st.columns(2)
    with col_db:
        try:
            get_engine().connect().close()
            st.success("Database: connected")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Database: {type(exc).__name__}: {exc}")

    with col_run:
        ts = last_etl_run_at()
        if ts:
            st.info(f"Last ETL run: **{ts}**")
        else:
            st.warning("No ETL runs recorded yet.")


if __name__ == "__main__":
    main()
