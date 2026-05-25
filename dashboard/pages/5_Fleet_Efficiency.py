"""Fleet efficiency — reads marts.v_fleet_efficiency_dashboard.

KPI focus: cost-per-km, utilization, fuel efficiency, MoM trend. Tailored
for the operations team who watches Total Cost of Ownership and needs to
spot trends across a 6-month window at a glance.
"""

from __future__ import annotations

from io import BytesIO

import altair as alt
import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Fleet efficiency")
filters = render_sidebar_filters()

st.title("Fleet efficiency")
st.caption(
    "Cost-per-km, utilization, fuel efficiency — sourced from "
    "`marts.v_fleet_efficiency_dashboard`."
)

# ---- Load data ------------------------------------------------------------
# View is per-(tenant, month). We aggregate to fleet × month below.
query = f"""
    SELECT *
      FROM marts.v_fleet_efficiency_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     ORDER BY year_month, tenant_id
"""
df = read_sql(query, params=filters.params())

if df.empty:
    st.warning(
        "No efficiency data for the current filters. Widen the date range or "
        "clear the tenant filter."
    )
    st.stop()

# ---- Fleet aggregate per month --------------------------------------------
# Ratio columns (cost_per_km, fuel_litres_per_100km, ...) cannot be summed
# across tenants — they must be recomputed from totals. We sum the additive
# columns first, then derive the ratios.
ADDITIVE_COLS = [
    "active_devices", "active_vehicles", "total_trips", "total_distance_km",
    "total_driving_hours", "total_operating_cost", "total_fuel_cost",
    "total_maintenance_cost", "total_fuel_litres",
]
present_add = [c for c in ADDITIVE_COLS if c in df.columns]

monthly = (
    df.groupby("year_month", as_index=False)[present_add]
    .sum(numeric_only=True)
    .sort_values("year_month")
)


def _safe_ratio(numer: pd.Series, denom: pd.Series, scale: float = 1.0) -> pd.Series:
    return (numer / denom.where(denom > 0)).fillna(0.0) * scale


if {"total_operating_cost", "total_distance_km"} <= set(monthly.columns):
    monthly["cost_per_km"] = _safe_ratio(
        monthly["total_operating_cost"], monthly["total_distance_km"]
    )
if {"total_operating_cost", "total_trips"} <= set(monthly.columns):
    monthly["cost_per_trip"] = _safe_ratio(
        monthly["total_operating_cost"], monthly["total_trips"]
    )
if {"total_fuel_litres", "total_distance_km"} <= set(monthly.columns):
    monthly["fuel_litres_per_100km"] = _safe_ratio(
        monthly["total_fuel_litres"], monthly["total_distance_km"], scale=100
    )
if {"total_fuel_cost", "total_distance_km"} <= set(monthly.columns):
    monthly["fuel_cost_per_100km"] = _safe_ratio(
        monthly["total_fuel_cost"], monthly["total_distance_km"], scale=100
    )

# Idle / utilization can't be summed — take the device-weighted mean. (A
# quiet tenant would otherwise drag the mean down disproportionately.)
# Implemented as numerator/denominator on the original df so we don't
# need a closure-over-loop-variable lambda (ruff B023).
for ratio_col in ("utilization_rate_pct", "idle_time_pct"):
    if ratio_col not in df.columns:
        continue
    weighted = df.assign(
        _num=df[ratio_col] * df["active_devices"],
        _den=df["active_devices"],
    )
    grouped = weighted.groupby("year_month", as_index=False)[["_num", "_den"]].sum()
    grouped[ratio_col] = grouped["_num"] / grouped["_den"].where(grouped["_den"] > 0)
    grouped[ratio_col] = grouped[ratio_col].fillna(0.0)
    monthly = monthly.merge(
        grouped[["year_month", ratio_col]], on="year_month", how="left"
    )

monthly = monthly.sort_values("year_month").reset_index(drop=True)
latest = monthly.iloc[-1]
prior = monthly.iloc[-2] if len(monthly) > 1 else None


def _delta(latest_val, prior_val, *, lower_is_better: bool = False) -> str | None:
    """Format MoM delta as a percentage string. None when no prior row."""
    if prior_val is None or prior_val in (0, 0.0):
        return None
    pct = (latest_val - prior_val) / prior_val * 100
    sign = "-" if pct < 0 else "+"
    return f"{sign}{abs(pct):.1f}%"


# ---- Row 1: KPI cards -----------------------------------------------------
st.markdown(
    "<style>div[data-testid='stMetricValue'] {font-size: 36px;}</style>",
    unsafe_allow_html=True,
)
c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Cost / km (DA)",
    f"{latest.get('cost_per_km', 0):.2f}",
    delta=_delta(
        latest.get("cost_per_km", 0),
        prior.get("cost_per_km") if prior is not None else None,
    ),
    delta_color="inverse",  # lower cost = better
)
c2.metric(
    "Utilization %",
    f"{latest.get('utilization_rate_pct', 0):.1f}%",
    delta=_delta(
        latest.get("utilization_rate_pct", 0),
        prior.get("utilization_rate_pct") if prior is not None else None,
    ),
)
c3.metric(
    "Fuel L / 100km",
    f"{latest.get('fuel_litres_per_100km', 0):.1f}",
    delta=_delta(
        latest.get("fuel_litres_per_100km", 0),
        prior.get("fuel_litres_per_100km") if prior is not None else None,
    ),
    delta_color="inverse",
)
c4.metric(
    "Idle %",
    f"{latest.get('idle_time_pct', 0):.1f}%",
    delta=_delta(
        latest.get("idle_time_pct", 0),
        prior.get("idle_time_pct") if prior is not None else None,
    ),
    delta_color="inverse",
)

st.caption(
    f"Latest: **{latest['year_month']}** · "
    f"{int(latest.get('active_devices', 0)):,} active devices · "
    f"{int(latest.get('total_trips', 0)):,} trips"
)

# ---- Row 2: Trend charts --------------------------------------------------
st.subheader("Trends")
tcol1, tcol2 = st.columns(2)

with tcol1:
    st.markdown("**Cost per km (6mo)**")
    if "cost_per_km" in monthly.columns:
        fig = px.line(monthly, x="year_month", y="cost_per_km", markers=True)
        fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=300)
        st.plotly_chart(fig, use_container_width=True)

with tcol2:
    st.markdown("**Utilization trend**")
    if "utilization_rate_pct" in monthly.columns:
        fig = px.area(
            monthly,
            x="year_month",
            y="utilization_rate_pct",
        )
        fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=300)
        st.plotly_chart(fig, use_container_width=True)

# ---- Row 3: Top / bottom performers (per-tenant in the latest month) ------
st.subheader("Top & bottom performers")
latest_month = monthly.iloc[-1]["year_month"]
latest_rows = df[df["year_month"] == latest_month].copy()
if "cost_per_km" in latest_rows.columns and not latest_rows.empty:
    latest_rows = latest_rows.sort_values("cost_per_km")
    tcol1, tcol2 = st.columns(2)
    show_cols = [
        c
        for c in (
            "tenant_id", "active_devices", "total_trips", "total_distance_km",
            "cost_per_km", "fuel_litres_per_100km", "utilization_rate_pct",
        )
        if c in latest_rows.columns
    ]
    with tcol1:
        st.markdown(f"**Best efficiency · {latest_month}**")
        st.dataframe(
            latest_rows[show_cols].head(10).reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )
    with tcol2:
        st.markdown(f"**Worst efficiency · {latest_month}**")
        st.dataframe(
            latest_rows[show_cols].tail(10)[::-1].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

# ---- Row 4: Activity heatmap (day × hour) ---------------------------------
st.subheader("Fleet activity heatmap")
st.caption(
    "Trips per (day-of-week × hour-of-day) over the selected window. "
    "Useful for spotting unused capacity."
)
heat_query = f"""
    SELECT
      EXTRACT(DOW FROM begin_path_time)::INTEGER  AS dow,
      EXTRACT(HOUR FROM begin_path_time)::INTEGER AS hour,
      COUNT(*)                                    AS trips
    FROM warehouse.fact_trip
    WHERE 1=1
      {filters.tenant_clause()}
      {filters.date_clause('trip_date')}
    GROUP BY 1, 2
    ORDER BY 1, 2
"""
try:
    heat = read_sql(heat_query, params=filters.params())
    if heat.empty:
        st.info("No trip data in the selected window.")
    else:
        dow_labels = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed",
                      4: "Thu", 5: "Fri", 6: "Sat"}
        heat["day"] = heat["dow"].map(dow_labels)
        chart = (
            alt.Chart(heat)
            .mark_rect()
            .encode(
                x=alt.X("hour:O", title="Hour"),
                y=alt.Y(
                    "day:O",
                    sort=list(dow_labels.values()),
                    title="Day",
                ),
                color=alt.Color("trips:Q", scale=alt.Scale(scheme="viridis")),
                tooltip=["day", "hour", "trips"],
            )
            .properties(height=240)
        )
        st.altair_chart(chart, use_container_width=True)
except Exception as exc:  # noqa: BLE001
    st.info(f"Heatmap unavailable: {exc}")

# ---- Export ---------------------------------------------------------------
st.subheader("Export")
ecol1, ecol2 = st.columns(2)
with ecol1:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"fleet_efficiency_{latest_month}.csv",
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
            file_name=f"fleet_efficiency_{latest_month}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except ImportError:
        st.caption("Install `openpyxl` for Excel export.")

# ---- Raw table ------------------------------------------------------------
st.subheader("Per-tenant detail")
st.dataframe(df, use_container_width=True, hide_index=True)
