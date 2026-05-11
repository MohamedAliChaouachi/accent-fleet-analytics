"""Executive overview — reads marts.v_executive_dashboard."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Executive overview")
render_sidebar_filters()

st.title("Executive overview")
st.caption("Fleet-wide KPIs sourced from `marts.v_executive_dashboard`.")

df = read_sql("SELECT * FROM marts.v_executive_dashboard ORDER BY year_month")

if df.empty:
    st.warning("No data yet. Run the ETL bootstrap + backfill to populate marts.")
    st.stop()

latest = df.iloc[-1]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Active devices", int(latest.get("active_devices") or 0))
c2.metric("Total trips", int(latest.get("total_trips") or 0))
c3.metric("Total distance (km)", f"{(latest.get('total_distance_km') or 0):,.0f}")
c4.metric("Avg risk score", f"{(latest.get('avg_risk_score') or 0):.1f}")

st.subheader("Trip volume over time")
if "year_month" in df.columns and "total_trips" in df.columns:
    fig = px.line(df, x="year_month", y="total_trips", markers=True)
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Raw table")
st.dataframe(df, use_container_width=True, hide_index=True)
