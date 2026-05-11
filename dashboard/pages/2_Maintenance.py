"""Maintenance dashboard — reads marts.v_maintenance_dashboard."""

from __future__ import annotations

import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Maintenance")
render_sidebar_filters()

st.title("Maintenance")
st.caption("Upcoming work, fueling, fault counts from `v_maintenance_dashboard`.")

df = read_sql("SELECT * FROM marts.v_maintenance_dashboard")

if df.empty:
    st.warning("No data yet.")
    st.stop()

c1, c2, c3 = st.columns(3)
if "open_maintenance_count" in df.columns:
    c1.metric("Open maintenance", int(df["open_maintenance_count"].sum()))
if "fueling_events" in df.columns:
    c2.metric("Fueling events", int(df["fueling_events"].sum()))
if "fault_count" in df.columns:
    c3.metric("Faults", int(df["fault_count"].sum()))

st.subheader("Detail")
st.dataframe(df, use_container_width=True, hide_index=True)
