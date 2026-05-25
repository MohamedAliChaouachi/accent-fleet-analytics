"""Executive overview — reads marts.v_executive_dashboard.

v2.0 addition: a 5-metric "fleet health" strip at the top sourced from
several views (efficiency, safety, alerts) so a director can read the
business pulse without scrolling.
"""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Executive overview")
filters = render_sidebar_filters()

st.title("Executive overview")
st.caption("Fleet-wide KPIs sourced from `marts.v_executive_dashboard`.")

# =============================================================================
# v2.0 KPI strip — top-of-page health pulse
# =============================================================================
# Each metric is read from the most appropriate view rather than recomputed
# locally; this keeps the page resilient to schema drift and means the
# Executive page is the canonical "fleet health" surface.
#
# Each lookup is wrapped in a try so a missing view (e.g. the safety mart
# hasn't been deployed yet) silently degrades to "—" instead of breaking
# the whole page.

st.markdown(
    "<style>div[data-testid='stMetricValue'] {font-size: 32px;}</style>",
    unsafe_allow_html=True,
)


def _safe_read(query: str, params: dict | None = None):
    try:
        return read_sql(query, params=params or {})
    except Exception:  # noqa: BLE001
        return None


k1, k2, k3, k4, k5 = st.columns(5)

# --- Fleet Health: latest safety_score from v_safety_scorecard_dashboard ----
health = _safe_read(
    f"""
    SELECT year_month, AVG(safety_score) AS safety_score
      FROM marts.v_safety_scorecard_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     GROUP BY year_month
     ORDER BY year_month DESC
     LIMIT 2
    """,
    params=filters.params(),
)
if health is not None and not health.empty:
    latest_h = float(health.iloc[0]["safety_score"] or 0)
    prior_h = float(health.iloc[1]["safety_score"] or 0) if len(health) > 1 else None
    delta = f"{latest_h - prior_h:+.1f}" if prior_h is not None else None
    k1.metric("Fleet health", f"{latest_h:.0f}%", delta=delta)
else:
    k1.metric("Fleet health", "—")

# --- Avg Risk Score: latest avg_risk_score across tenants -------------------
risk = _safe_read(
    f"""
    SELECT year_month, AVG(avg_risk_score) AS avg_risk
      FROM marts.v_safety_scorecard_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     GROUP BY year_month
     ORDER BY year_month DESC
     LIMIT 2
    """,
    params=filters.params(),
)
if risk is not None and not risk.empty:
    latest_r = float(risk.iloc[0]["avg_risk"] or 0)
    prior_r = float(risk.iloc[1]["avg_risk"] or 0) if len(risk) > 1 else None
    delta = f"{latest_r - prior_r:+.1f}" if prior_r is not None else None
    # Lower risk score = better → inverse colouring.
    k2.metric("Avg risk score", f"{latest_r:.0f}", delta=delta, delta_color="inverse")
else:
    k2.metric("Avg risk score", "—")

# --- Active Devices, Cost/KM — derived from the existing exec dashboard ----
# We re-query here (cached) so the strip is cheap and resilient even when
# the main df below is empty (e.g. on a fresh DB).
exec_summary = _safe_read(
    f"""
    SELECT year_month,
           SUM(active_devices)              AS active_devices,
           SUM(total_distance_km)           AS total_distance_km,
           SUM(total_operating_cost)        AS total_operating_cost,
           SUM(total_alerts)                AS total_alerts
      FROM marts.v_executive_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     GROUP BY year_month
     ORDER BY year_month DESC
     LIMIT 2
    """,
    params=filters.params(),
)
if exec_summary is not None and not exec_summary.empty:
    cur = exec_summary.iloc[0]
    prv = exec_summary.iloc[1] if len(exec_summary) > 1 else None

    # Active devices
    cur_d = int(cur["active_devices"] or 0)
    prv_d = int(prv["active_devices"]) if prv is not None else None
    k3.metric(
        "Active devices",
        f"{cur_d:,}",
        delta=f"{cur_d - prv_d:+d}" if prv_d is not None else None,
    )

    # Cost / km
    cur_cost = float(cur["total_operating_cost"] or 0)
    cur_dist = float(cur["total_distance_km"] or 0)
    cur_cpk = cur_cost / cur_dist if cur_dist > 0 else 0
    if prv is not None:
        prv_cost = float(prv["total_operating_cost"] or 0)
        prv_dist = float(prv["total_distance_km"] or 0)
        prv_cpk = prv_cost / prv_dist if prv_dist > 0 else 0
        cpk_delta = f"{(cur_cpk - prv_cpk) / prv_cpk * 100:+.1f}%" if prv_cpk > 0 else None
    else:
        cpk_delta = None
    k4.metric("Cost / km (DA)", f"{cur_cpk:.2f}", delta=cpk_delta, delta_color="inverse")
else:
    k3.metric("Active devices", "—")
    k4.metric("Cost / km (DA)", "—")

# --- Active alerts (from predictive alerts view; current snapshot) ----------
alerts = _safe_read(
    f"""
    SELECT COUNT(*) AS n
      FROM marts.v_predictive_alerts_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
    """,
    params=filters.params(),
)
if alerts is not None and not alerts.empty:
    n_alerts = int(alerts.iloc[0]["n"] or 0)
    # Inverse colour — more alerts is worse for the business.
    k5.metric("Active alerts", f"{n_alerts:,}", delta_color="inverse")
else:
    k5.metric("Active alerts", "—")

st.divider()

# Filtered SELECT. The view itself is small (≤12 months × tenants) so a
# WHERE clause is cheap; we still scope to the chosen window for tidiness.
#
# Note: the view is per-(tenant, year_month). When the sidebar leaves the
# tenant filter empty ("all tenants"), df has *N tenants × M months* rows
# and ANY single-row pick (e.g. df.iloc[-1]) is meaningless — it picks one
# arbitrary tenant's slice. We aggregate to one row per month below before
# computing KPIs or drawing trend charts. The user-visible symptom of not
# doing this is the classic "all tenants shows fewer devices than a single
# tenant" paradox: a single-tenant view trivially aggregates to that tenant,
# but the multi-tenant view would otherwise display whichever tenant ORDER
# BY put last in the latest month.
query = f"""
    SELECT *
      FROM marts.v_executive_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     ORDER BY year_month, tenant_id
"""
df = read_sql(query, params=filters.params())

if df.empty:
    st.warning(
        "No data for the current filters. Try widening the date range or "
        "clearing the tenant filter. If the table is empty, run "
        "`make seed` to populate marts."
    )
    st.stop()

# ----- Per-month fleet aggregate --------------------------------------------
# Columns that are additive across tenants (vehicle/device counts, trip
# counts, raw distances, raw cost totals, alert counts). cost_per_km is a
# ratio so it must be recomputed from totals — summing per-tenant ratios
# is nonsense. The MoM deltas and 3-month rolling averages from the view
# are also per-tenant; we drop them from the aggregate and recompute the
# delta on the aggregate timeline instead.
ADDITIVE_COLS = [
    "active_vehicles",
    "active_devices",
    "total_trips",
    "total_distance_km",
    "total_maintenance_cost",
    "total_fuel_cost",
    "total_operating_cost",
    "total_alerts",
    "panic_alerts",
    "total_overspeed",
    "total_harsh_events",
]
present_additive = [c for c in ADDITIVE_COLS if c in df.columns]
monthly = (
    df.groupby("year_month", as_index=False)[present_additive]
    .sum(numeric_only=True)
    .sort_values("year_month")
)
# Recompute cost_per_km from the aggregated totals so the ratio is correct
# even when summing across tenants of very different sizes.
if {"total_operating_cost", "total_distance_km"} <= set(monthly.columns):
    monthly["cost_per_km"] = (
        monthly["total_operating_cost"]
        / monthly["total_distance_km"].where(monthly["total_distance_km"] > 0)
    ).fillna(0.0)

latest = monthly.iloc[-1]


def _kpi(col, label: str, source_key: str, fmt: str = "{:,.0f}") -> None:
    """Render a metric only if the source column exists in the aggregate."""
    if source_key in monthly.columns:
        value = latest.get(source_key) or 0
        col.metric(label, fmt.format(value))
    else:
        col.metric(label, "—")


c1, c2, c3, c4 = st.columns(4)
_kpi(c1, "Active devices",   "active_devices")
_kpi(c2, "Total trips",      "total_trips")
_kpi(c3, "Distance (km)",    "total_distance_km")
_kpi(c4, "Cost / km",        "cost_per_km", fmt="{:,.2f}")

st.caption(
    f"KPIs above are the fleet total for **{latest['year_month']}**, "
    f"summed across {df[df['year_month'] == latest['year_month']]['tenant_id'].nunique()} "
    "tenant(s) in scope."
)

st.subheader("Trip volume over time")
if {"year_month", "total_trips"} <= set(monthly.columns):
    fig = px.line(monthly, x="year_month", y="total_trips", markers=True)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("`total_trips` not available in this view.")

st.subheader("Operating cost trend")
if {"year_month", "total_operating_cost"} <= set(monthly.columns):
    fig = px.bar(monthly, x="year_month", y="total_operating_cost")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Raw table (per tenant × month)")
st.dataframe(df, use_container_width=True, hide_index=True)
