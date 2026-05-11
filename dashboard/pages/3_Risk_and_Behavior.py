"""Risk & behavior — reads v_fleet_risk_dashboard + v_device_risk_profile."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import RISK_COLORS, apply_layout, render_sidebar_filters

apply_layout(page_title="Risk & behavior")
render_sidebar_filters()

st.title("Risk & behavior")
st.caption("Fleet-level + per-device 3-month rolling risk.")

fleet_df = read_sql("SELECT * FROM marts.v_fleet_risk_dashboard")
risk_df = read_sql("SELECT * FROM marts.v_device_risk_profile ORDER BY risk_score DESC")

if risk_df.empty:
    st.warning("No risk data yet. Run the ETL backfill so v_device_risk_profile has rows.")
    st.stop()

# Risk distribution
st.subheader("Risk category distribution")
if "risk_category" in risk_df.columns:
    counts = risk_df["risk_category"].value_counts().reset_index()
    counts.columns = ["category", "device_count"]
    fig = px.bar(
        counts,
        x="category",
        y="device_count",
        color="category",
        color_discrete_map=RISK_COLORS,
    )
    st.plotly_chart(fig, use_container_width=True)

# Top 20 risk
st.subheader("Top 20 highest-risk devices")
st.dataframe(risk_df.head(20), use_container_width=True, hide_index=True)

# Device drill-down
st.subheader("Per-device drill-down")
device_id = st.selectbox(
    "Pick a device", options=risk_df["device_id"].tolist()
)
if device_id:
    history = read_sql(
        """
        SELECT year_month::text AS year_month,
               total_trips, total_distance_km,
               overspeed_per_100km, high_speed_trip_ratio,
               night_trip_ratio, avg_max_speed_kmh
          FROM marts.mart_device_monthly_behavior
         WHERE device_id = :did
         ORDER BY year_month
        """,
        params={"did": int(device_id)},
    )
    if history.empty:
        st.info("No monthly history for this device.")
    else:
        st.line_chart(history.set_index("year_month")[["overspeed_per_100km", "night_trip_ratio"]])
        st.dataframe(history, use_container_width=True, hide_index=True)

# Fleet-level metrics
if not fleet_df.empty:
    st.subheader("Fleet risk summary")
    st.dataframe(fleet_df, use_container_width=True, hide_index=True)
