"""Operations dashboard — reads marts.v_operational_dashboard."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Operations")
filters = render_sidebar_filters()

st.title("Operations")
st.caption("Trip volume, harsh events, overspeed hotspots from `v_operational_dashboard`.")

# fleet_date is a date column → filter via the date_clause helper.
query = f"""
    SELECT *
      FROM marts.v_operational_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.date_clause('fleet_date')}
     ORDER BY fleet_date
"""
df = read_sql(query, params=filters.params())

if df.empty:
    st.warning("No operational rows in the selected window. Try a wider date range.")
    st.stop()

# KPI strip
def _sum(col_name: str) -> int:
    return int(df[col_name].sum()) if col_name in df.columns else 0


c1, c2, c3, c4 = st.columns(4)
c1.metric("Trips",          f"{_sum('total_trips'):,}")
c2.metric("Distance (km)",  f"{int(df['total_distance_km'].sum()) if 'total_distance_km' in df.columns else 0:,}")
c3.metric("Alerts",         f"{_sum('total_alerts'):,}")
c4.metric("Overspeed",      f"{_sum('total_overspeed') if 'total_overspeed' in df.columns else _sum('overspeed_count'):,}")

st.subheader("Daily trip count")
if {"fleet_date", "total_trips"} <= set(df.columns):
    fig = px.bar(df, x="fleet_date", y="total_trips")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Daily harsh-event volume")
harsh_cols = [c for c in ("harsh_events_per_100km", "total_harsh_events") if c in df.columns]
if harsh_cols:
    fig = px.line(df, x="fleet_date", y=harsh_cols, markers=True)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No harsh-event columns in this view yet — populate `fact_harsh_event` first.")

st.subheader("Raw table")
st.dataframe(df, use_container_width=True, hide_index=True)
