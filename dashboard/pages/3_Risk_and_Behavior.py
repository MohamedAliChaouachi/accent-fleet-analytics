"""Risk & behavior — reads v_fleet_risk_dashboard + v_device_risk_profile.

Now also overlays the per-device cluster assignment from
marts.fact_device_cluster_assignment so users can see *why* a device sits
where it does in the risk distribution.
"""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import RISK_COLORS, apply_layout, render_sidebar_filters

apply_layout(page_title="Risk & behavior")
filters = render_sidebar_filters()

st.title("Risk & behavior")
st.caption("Fleet-level + per-device 3-month rolling risk.")

# Fleet rollup — no date column on this view; apply tenant filter only.
fleet_df = read_sql(
    f"""
    SELECT *
      FROM marts.v_fleet_risk_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
    """,
    params=filters.params(),
)

# Device-level: filter on latest_month (CHAR(7)) against the chosen window.
risk_df = read_sql(
    f"""
    SELECT *
      FROM marts.v_device_risk_profile
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('latest_month')}
     ORDER BY risk_score DESC
    """,
    params=filters.params(),
)

if risk_df.empty:
    st.warning(
        "No risk data for the current filters. Widen the date range or "
        "remove the tenant filter — the view derives from a 3-month "
        "rolling window so the earliest data must be at least 1 month old."
    )
    st.stop()

# ---------- Risk distribution ----------
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

# ---------- Top 20 ----------
st.subheader("Top 20 highest-risk devices")
st.dataframe(risk_df.head(20), use_container_width=True, hide_index=True)

# ---------- Cluster overlay (new in Sprint 2) ----------
st.subheader("Device clusters")
st.caption(
    "Pulled from `marts.fact_device_cluster_assignment` — populated by the "
    "batch scorer at the end of every incremental flow."
)
cluster_df = read_sql(
    f"""
    SELECT a.tenant_id,
           a.device_id,
           a.year_month,
           a.cluster_id,
           a.distance_to_centroid,
           a.model_version,
           r.risk_score,
           r.risk_category
      FROM marts.fact_device_cluster_assignment a
      LEFT JOIN marts.v_device_risk_profile r
        ON r.tenant_id = a.tenant_id
       AND r.device_id = a.device_id
     WHERE 1=1
       {filters.tenant_clause('a')}
       {filters.month_clause('a.year_month')}
    """,
    params=filters.params(),
)

if cluster_df.empty:
    st.info(
        "No cluster assignments yet. Train the model with "
        "`python scripts/train_clustering.py` (or wait for the next "
        "incremental flow that follows a successful training run)."
    )
else:
    sizes = cluster_df["cluster_id"].value_counts().sort_index().reset_index()
    sizes.columns = ["cluster_id", "devices"]
    c1, c2 = st.columns([1, 2])
    with c1:
        st.metric("Clusters", int(cluster_df["cluster_id"].nunique()))
        st.metric("Scored devices", int(len(cluster_df)))
        versions = cluster_df["model_version"].dropna().unique()
        if len(versions):
            st.caption(f"Model version(s): `{', '.join(map(str, versions))}`")
    with c2:
        fig = px.bar(sizes, x="cluster_id", y="devices", color="cluster_id")
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    # Cluster × risk-category crosstab — answers "do clusters track risk?"
    if "risk_category" in cluster_df.columns:
        st.markdown("**Cluster × risk-category crosstab**")
        ct = (
            cluster_df.dropna(subset=["risk_category"])
            .groupby(["cluster_id", "risk_category"])
            .size()
            .reset_index(name="devices")
        )
        if not ct.empty:
            fig = px.bar(
                ct,
                x="cluster_id",
                y="devices",
                color="risk_category",
                color_discrete_map=RISK_COLORS,
                barmode="stack",
            )
            st.plotly_chart(fig, use_container_width=True)

# ---------- Per-device drill-down ----------
st.subheader("Per-device drill-down")
device_id = st.selectbox(
    "Pick a device",
    options=risk_df["device_id"].tolist(),
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
        st.line_chart(
            history.set_index("year_month")[
                ["overspeed_per_100km", "night_trip_ratio"]
            ]
        )
        st.dataframe(history, use_container_width=True, hide_index=True)

# ---------- Fleet rollup ----------
if not fleet_df.empty:
    st.subheader("Fleet risk summary")
    st.dataframe(fleet_df, use_container_width=True, hide_index=True)
