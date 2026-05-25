"""Safety scorecard — reads marts.v_safety_scorecard_dashboard.

KPI focus: overspeed rates, harsh-event rates, high-risk device counts, and
a composite 0-100 safety score that the insurance/risk team uses to track
fleet-wide MoM improvement.
"""

from __future__ import annotations

from io import BytesIO

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import RISK_COLORS, apply_layout, render_sidebar_filters

apply_layout(page_title="Safety scorecard")
filters = render_sidebar_filters()

st.title("Safety scorecard")
st.caption(
    "Normalised safety KPIs (per 1000 km), risk distribution, temporal patterns — "
    "sourced from `marts.v_safety_scorecard_dashboard`."
)

# ---- Load data ------------------------------------------------------------
query = f"""
    SELECT *
      FROM marts.v_safety_scorecard_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     ORDER BY year_month, tenant_id
"""
df = read_sql(query, params=filters.params())

if df.empty:
    st.warning("No safety data for the current filters.")
    st.stop()

# ---- Fleet aggregate per month --------------------------------------------
# Sum the additive columns (event counts, distance). Ratios get recomputed
# from totals so the fleet rate isn't a stale per-tenant blend.
ADDITIVE_COLS = [
    "active_devices", "total_distance_km", "total_trips", "total_overspeed",
    "total_harsh_events", "harsh_brake_count", "harsh_accel_count",
    "harsh_corner_count", "scored_devices", "high_risk_devices",
    "critical_risk_devices", "high_or_critical_devices",
]
present_add = [c for c in ADDITIVE_COLS if c in df.columns]
monthly = (
    df.groupby("year_month", as_index=False)[present_add]
    .sum(numeric_only=True)
    .sort_values("year_month")
)


def _safe_ratio(numer: pd.Series, denom: pd.Series, scale: float = 1.0) -> pd.Series:
    return (numer / denom.where(denom > 0)).fillna(0.0) * scale


if {"total_overspeed", "total_distance_km"} <= set(monthly.columns):
    monthly["overspeed_rate_per_1000km"] = _safe_ratio(
        monthly["total_overspeed"], monthly["total_distance_km"], scale=1000
    )
if {"total_harsh_events", "total_distance_km"} <= set(monthly.columns):
    monthly["harsh_events_per_1000km"] = _safe_ratio(
        monthly["total_harsh_events"], monthly["total_distance_km"], scale=1000
    )

# Composite safety score: re-derive from fleet totals so it matches the
# per-tenant SQL formula (100 - 2×overspeed_rate - 4×harsh_rate, clamped).
if {"overspeed_rate_per_1000km", "harsh_events_per_1000km"} <= set(monthly.columns):
    monthly["safety_score"] = (
        100
        - 2 * monthly["overspeed_rate_per_1000km"]
        - 4 * monthly["harsh_events_per_1000km"]
    ).clip(lower=0, upper=100)

if "high_or_critical_devices" in monthly.columns and "scored_devices" in monthly.columns:
    monthly["high_risk_device_pct"] = _safe_ratio(
        monthly["high_or_critical_devices"], monthly["scored_devices"], scale=100
    )

monthly = monthly.sort_values("year_month").reset_index(drop=True)
latest = monthly.iloc[-1]
prior = monthly.iloc[-2] if len(monthly) > 1 else None


def _delta(latest_val, prior_val) -> str | None:
    if prior_val is None or prior_val in (0, 0.0):
        return None
    diff = latest_val - prior_val
    sign = "-" if diff < 0 else "+"
    return f"{sign}{abs(diff):.2f}"


# ---- Row 1: 4 Safety KPIs -------------------------------------------------
st.markdown(
    "<style>div[data-testid='stMetricValue'] {font-size: 36px;}</style>",
    unsafe_allow_html=True,
)
c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Overspeed / 1000 km",
    f"{latest.get('overspeed_rate_per_1000km', 0):.2f}",
    delta=_delta(
        latest.get("overspeed_rate_per_1000km", 0),
        prior.get("overspeed_rate_per_1000km") if prior is not None else None,
    ),
    delta_color="inverse",  # fewer overspeed = better
)
c2.metric(
    "Harsh / 1000 km",
    f"{latest.get('harsh_events_per_1000km', 0):.2f}",
    delta=_delta(
        latest.get("harsh_events_per_1000km", 0),
        prior.get("harsh_events_per_1000km") if prior is not None else None,
    ),
    delta_color="inverse",
)
c3.metric(
    "High-risk devices",
    f"{int(latest.get('high_or_critical_devices', 0))}",
    delta=(
        f"{int(latest.get('high_or_critical_devices', 0)) - int(prior.get('high_or_critical_devices', 0)):+d}"
        if prior is not None
        else None
    ),
    delta_color="inverse",
)
c4.metric(
    "Safety score",
    f"{latest.get('safety_score', 0):.0f}",
    delta=_delta(
        latest.get("safety_score", 0),
        prior.get("safety_score") if prior is not None else None,
    ),  # higher safety_score = better → default green-on-positive is correct
)

st.caption(
    f"Latest: **{latest['year_month']}** · "
    f"{int(latest.get('active_devices', 0)):,} active devices · "
    f"{int(latest.get('total_distance_km', 0)):,} km driven"
)

# ---- Row 2: Safety trends -------------------------------------------------
st.subheader("Trends")
tcol1, tcol2, tcol3 = st.columns(3)

with tcol1:
    st.markdown("**Overspeed rate (6mo)**")
    if "overspeed_rate_per_1000km" in monthly.columns:
        fig = px.line(monthly, x="year_month", y="overspeed_rate_per_1000km", markers=True)
        fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
        st.plotly_chart(fig, use_container_width=True)

with tcol2:
    st.markdown("**Harsh events by type**")
    harsh_cols = [c for c in ("harsh_brake_count", "harsh_accel_count", "harsh_corner_count")
                  if c in monthly.columns]
    if harsh_cols:
        long_df = monthly.melt(
            id_vars=["year_month"],
            value_vars=harsh_cols,
            var_name="type",
            value_name="events",
        )
        fig = px.bar(long_df, x="year_month", y="events", color="type", barmode="stack")
        fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
        st.plotly_chart(fig, use_container_width=True)

with tcol3:
    st.markdown("**Risk distribution (latest)**")
    latest_rows = df[df["year_month"] == latest["year_month"]]
    pie_data = pd.DataFrame(
        {
            "category": ["high_or_critical", "moderate_or_low"],
            "devices": [
                int(latest_rows["high_or_critical_devices"].sum())
                if "high_or_critical_devices" in latest_rows.columns
                else 0,
                max(
                    int(latest_rows["scored_devices"].sum())
                    - int(latest_rows["high_or_critical_devices"].sum())
                    if {"scored_devices", "high_or_critical_devices"} <= set(latest_rows.columns)
                    else 0,
                    0,
                ),
            ],
        }
    )
    if pie_data["devices"].sum() > 0:
        fig = px.pie(
            pie_data,
            names="category",
            values="devices",
            color="category",
            color_discrete_map={
                "high_or_critical": RISK_COLORS["critical"],
                "moderate_or_low": RISK_COLORS["low"],
            },
            hole=0.4,
        )
        fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No scored devices yet.")

# ---- Row 3: Top 10 riskiest devices + overspeed-by-hour -------------------
st.subheader("Risk drill-downs")
rcol1, rcol2 = st.columns(2)

with rcol1:
    st.markdown("**Top 10 riskiest devices**")
    risky = read_sql(
        f"""
        SELECT tenant_id, device_id, latest_month, risk_score, risk_category,
               overspeed_3m, distance_3m
          FROM marts.v_device_risk_profile
         WHERE 1=1
           {filters.tenant_clause()}
           {filters.month_clause('latest_month')}
         ORDER BY risk_score DESC
         LIMIT 10
        """,
        params=filters.params(),
    )
    if risky.empty:
        st.info("No scored devices for this window.")
    else:
        st.dataframe(risky, use_container_width=True, hide_index=True)

with rcol2:
    st.markdown("**Overspeed by hour band**")
    hour_query = f"""
        SELECT
          EXTRACT(HOUR FROM begin_path_time)::INTEGER AS hour,
          COUNT(*) AS events
        FROM warehouse.fact_overspeed
        WHERE 1=1
          {filters.tenant_clause()}
          AND begin_path_time::DATE BETWEEN :start AND :end
        GROUP BY 1
        ORDER BY 1
    """
    try:
        by_hour = read_sql(hour_query, params=filters.params())
        if by_hour.empty:
            st.info("No overspeed events in window.")
        else:
            fig = px.bar(by_hour, x="hour", y="events")
            fig.update_xaxes(dtick=2)
            fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
            st.plotly_chart(fig, use_container_width=True)
    except Exception as exc:  # noqa: BLE001
        st.info(f"Hour breakdown unavailable: {exc}")

# ---- Row 4: Risk by tenant ------------------------------------------------
st.subheader("Risk distribution per tenant (latest month)")
latest_per_tenant = df[df["year_month"] == latest["year_month"]].copy()
risk_cols = [c for c in ("high_risk_devices", "critical_risk_devices", "scored_devices")
             if c in latest_per_tenant.columns]
if risk_cols and not latest_per_tenant.empty:
    plot_df = latest_per_tenant[["tenant_id"] + risk_cols].melt(
        id_vars="tenant_id", var_name="category", value_name="devices"
    )
    fig = px.bar(plot_df, x="tenant_id", y="devices", color="category", barmode="group")
    st.plotly_chart(fig, use_container_width=True)

# ---- Export ---------------------------------------------------------------
st.subheader("Export")
ecol1, ecol2 = st.columns(2)
with ecol1:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"safety_scorecard_{latest['year_month']}.csv",
        mime="text/csv",
    )
with ecol2:
    excel_buf = BytesIO()
    try:
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="raw", index=False)
            monthly.to_excel(writer, sheet_name="fleet_monthly", index=False)
        st.download_button(
            "Download Excel",
            data=excel_buf.getvalue(),
            file_name=f"safety_scorecard_{latest['year_month']}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except ImportError:
        st.caption("Install `openpyxl` for Excel export.")

# ---- Raw table ------------------------------------------------------------
st.subheader("Per-tenant detail")
st.dataframe(df, use_container_width=True, hide_index=True)
