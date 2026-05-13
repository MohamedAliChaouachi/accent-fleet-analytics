"""Maintenance dashboard — reads marts.v_maintenance_dashboard."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Maintenance")
filters = render_sidebar_filters()

st.title("Maintenance")
st.caption("Upcoming work, fueling, fault counts from `v_maintenance_dashboard`.")

query = f"""
    SELECT *
      FROM marts.v_maintenance_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     ORDER BY year_month DESC, total_cost DESC NULLS LAST
"""
df = read_sql(query, params=filters.params())

if df.empty:
    st.warning("No maintenance rows in the selected window.")
    st.stop()


def _safe_sum(col: str, fmt: str = "{:,}") -> str:
    if col not in df.columns:
        return "—"
    return fmt.format(int(df[col].sum()))


c1, c2, c3, c4 = st.columns(4)
c1.metric("Maintenance events", _safe_sum("maintenance_events"))
c2.metric("Open work orders",   _safe_sum("open_maintenance_count"))
c3.metric("Fueling events",     _safe_sum("fueling_events"))
c4.metric("Total cost",         _safe_sum("total_cost", fmt="{:,.0f}"))

# Cost-per-km bubble chart for vehicle leaderboard
st.subheader("Highest-cost vehicles")
top = df.head(20)
if {"matricule", "total_cost"} <= set(top.columns):
    color_col = "vehicle_class" if "vehicle_class" in top.columns else None
    fig = px.bar(
        top.sort_values("total_cost", ascending=True),
        x="total_cost",
        y="matricule",
        color=color_col,
        orientation="h",
    )
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Detail")
st.dataframe(df, use_container_width=True, hide_index=True)
