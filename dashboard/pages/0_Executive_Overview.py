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
