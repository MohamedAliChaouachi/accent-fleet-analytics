"""Operations dashboard — reads marts.v_operational_dashboard."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Operations")
render_sidebar_filters()

st.title("Operations")
st.caption("Trip volume, harsh events, overspeed hotspots from `v_operational_dashboard`.")

df = read_sql("SELECT * FROM marts.v_operational_dashboard")

if df.empty:
    st.warning("No data yet.")
    st.stop()

st.subheader("Daily trip count")
if "activity_date" in df.columns and "trip_count" in df.columns:
    fig = px.bar(df.sort_values("activity_date"), x="activity_date", y="trip_count")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Harsh events vs overspeed")
event_cols = [c for c in ["harsh_event_count", "overspeed_count"] if c in df.columns]
if event_cols:
    st.line_chart(df.set_index("activity_date")[event_cols])

st.subheader("Raw table")
st.dataframe(df, use_container_width=True, hide_index=True)
