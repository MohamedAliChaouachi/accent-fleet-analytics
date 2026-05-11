"""
Streamlit landing page.

Multi-page apps in Streamlit: any .py file under dashboard/pages/ becomes a
sidebar link automatically. This Home file is the welcome screen plus the
global filter sidebar (tenant / date range) and an ops health strip.
"""

from __future__ import annotations

import streamlit as st

from dashboard.lib.cache import last_etl_run_at, read_sql
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
| **Risk & behavior** | `marts.v_fleet_risk_dashboard`, `marts.v_device_risk_profile`, `marts.fact_device_cluster_assignment` |
| **What-if scoring** | `POST /score/risk`, `POST /score/cluster` |
"""
    )

    # Connectivity panel — quick at-a-glance for ops.
    col_db, col_run, col_clusters = st.columns(3)

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

    with col_clusters:
        # Cluster-assignment freshness — surfaces whether the batch scorer
        # has produced rows yet. Best-effort; an empty table is normal pre-train.
        try:
            df = read_sql(
                """
                SELECT COUNT(*)                 AS rows,
                       MAX(scored_at)::text     AS scored_at,
                       MAX(model_version)       AS model_version
                  FROM marts.fact_device_cluster_assignment
                """
            )
            if not df.empty and (df.iloc[0]["rows"] or 0) > 0:
                row = df.iloc[0]
                st.success(
                    f"Clusters: **{int(row['rows']):,}** rows  \n"
                    f"Model: `{row['model_version']}`  \n"
                    f"Latest: {row['scored_at']}"
                )
            else:
                st.warning("No cluster assignments yet — train the model.")
        except Exception:  # noqa: BLE001
            # Table may not exist on a fresh DB. Don't break the page.
            st.warning("Cluster table not initialised.")


if __name__ == "__main__":
    main()
