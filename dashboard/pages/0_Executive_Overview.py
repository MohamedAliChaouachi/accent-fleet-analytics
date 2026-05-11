"""Executive overview — reads marts.v_executive_dashboard."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Executive overview")
filters = render_sidebar_filters()

st.title("Executive overview")
st.caption("Fleet-wide KPIs sourced from `marts.v_executive_dashboard`.")

# Filtered SELECT. The view itself is small (≤12 months × tenants) so a
# WHERE clause is cheap; we still scope to the chosen window for tidiness.
query = f"""
    SELECT *
      FROM marts.v_executive_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     ORDER BY year_month
"""
df = read_sql(query, params=filters.params())

if df.empty:
    st.warning(
        "No data for the current filters. Try widening the date range or "
        "clearing the tenant filter. If the table is empty, run "
        "`make seed` to populate marts."
    )
    st.stop()

# KPI strip — use the latest row in scope (most recent year_month).
latest = df.iloc[-1]


def _kpi(col, label: str, source_key: str, fmt: str = "{:,.0f}") -> None:
    """Render a metric only if the source column exists."""
    if source_key in df.columns:
        value = latest.get(source_key) or 0
        col.metric(label, fmt.format(value))
    else:
        col.metric(label, "—")


c1, c2, c3, c4 = st.columns(4)
_kpi(c1, "Active devices",   "active_devices")
_kpi(c2, "Total trips",      "total_trips")
_kpi(c3, "Distance (km)",    "total_distance_km")
_kpi(c4, "Avg risk score",   "avg_risk_score", fmt="{:.1f}")

st.subheader("Trip volume over time")
if {"year_month", "total_trips"} <= set(df.columns):
    fig = px.line(df, x="year_month", y="total_trips", markers=True)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("`total_trips` not available in this view.")

st.subheader("Operating cost trend")
if {"year_month", "total_operating_cost"} <= set(df.columns):
    fig = px.bar(df, x="year_month", y="total_operating_cost")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Raw table")
st.dataframe(df, use_container_width=True, hide_index=True)
