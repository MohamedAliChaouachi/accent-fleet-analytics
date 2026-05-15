"""
Ad-hoc what-if scoring — calls POST /score/risk and POST /score/cluster
on the FastAPI service.

Lets an analyst tweak feature values for a device and see how the composite
risk score moves and which cluster the device would land in, without
re-running the SQL view or batch scorer.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.lib.api import API_BASE, post_json
from dashboard.lib.theme import RISK_COLORS, apply_layout, render_sidebar_filters

apply_layout(page_title="What-if scoring")
render_sidebar_filters()  # filters not strictly used, but keep sidebar consistent

st.title("What-if scoring")
st.caption(
    f"Hits `POST {API_BASE}/v1/score/risk` and `/v1/score/cluster` "
    "(bearer auth handled by `dashboard.lib.api`). Tweak the sliders and re-score."
)

with st.form("what_if"):
    col_a, col_b = st.columns(2)
    with col_a:
        overspeed_per_100km = st.slider("overspeed_per_100km", 0.0, 20.0, 3.0, 0.1)
        overspeed_count = st.slider("overspeed_count", 0, 500, 50, 1)
        overspeed_severity_high = st.slider("overspeed_severity_high", 0, 200, 10, 1)
        overspeed_severity_extreme = st.slider("overspeed_severity_extreme", 0, 100, 2, 1)
    with col_b:
        high_speed_trip_ratio = st.slider("high_speed_trip_ratio", 0.0, 1.0, 0.10, 0.01)
        speed_alert_per_100km = st.slider("speed_alert_per_100km", 0.0, 40.0, 5.0, 0.1)
        night_trip_ratio = st.slider("night_trip_ratio", 0.0, 1.0, 0.15, 0.01)
        avg_max_speed_kmh = st.slider("avg_max_speed_kmh", 0, 220, 110, 1)

    submitted = st.form_submit_button("Score")

if not submitted:
    st.info("Set values and click **Score** to call the API.")
    st.stop()

payload = {
    "overspeed_per_100km": overspeed_per_100km,
    "overspeed_count": overspeed_count,
    "overspeed_severity_high": overspeed_severity_high,
    "overspeed_severity_extreme": overspeed_severity_extreme,
    "high_speed_trip_ratio": high_speed_trip_ratio,
    "speed_alert_per_100km": speed_alert_per_100km,
    "night_trip_ratio": night_trip_ratio,
    "avg_max_speed_kmh": avg_max_speed_kmh,
}

# ---- Risk score ----
try:
    r = post_json("/v1/score/risk", payload)
    r.raise_for_status()
    risk_data = r.json()
except Exception as exc:  # noqa: BLE001
    st.error(f"Risk API call failed: {exc}")
    st.stop()

score = risk_data["risk_score"]
category = risk_data["category"]
color = RISK_COLORS.get(category, "#777")

st.markdown(
    f"""
### Risk score: **{score:.1f}** &nbsp;
<span style='background:{color};padding:4px 10px;border-radius:6px;color:white;'>
{category.upper()}
</span>
""",
    unsafe_allow_html=True,
)

st.subheader("Per-factor contribution")
components = risk_data.get("components", {})
if components:
    bar_df = pd.DataFrame(
        {"factor": list(components.keys()), "contribution": list(components.values())}
    )
    fig = px.bar(bar_df, x="factor", y="contribution")
    st.plotly_chart(fig, use_container_width=True)

# ---- Cluster prediction (best-effort; 503 if no model is registered) ----
st.subheader("Cluster prediction")
try:
    r = post_json("/v1/score/cluster", payload)
    if r.status_code == 503:
        st.info(
            "Cluster model not yet available — the API returned 503. "
            "Run `python scripts/train_clustering.py` to train and register one."
        )
    else:
        r.raise_for_status()
        cluster_data = r.json()
        cid = cluster_data.get("cluster_id")
        dist = cluster_data.get("distance")
        ver = cluster_data.get("version", "?")
        st.metric("Cluster", f"#{cid}", help=f"Distance to centroid: {dist:.3f}")
        st.caption(f"Model version: `{ver}`")
except Exception as exc:  # noqa: BLE001
    st.warning(f"Cluster API call failed: {exc}")

with st.expander("Raw responses"):
    st.json({"risk": risk_data})
