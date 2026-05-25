"""Tenant billing — reads marts.v_tenant_billing_dashboard.

KPI focus: per-tenant usage, storage, revenue estimate, and growth trends.
Used internally by Accent's finance / capacity-planning team — not exposed
to customer tenants.
"""

from __future__ import annotations

from io import BytesIO

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Tenant billing")
filters = render_sidebar_filters()

st.title("Tenant billing")
st.caption(
    "Per-tenant usage, storage, and revenue estimates from "
    "`marts.v_tenant_billing_dashboard`. Pricing tiers are configurable in "
    "the SQL view; see sql/39_v_tenant_billing_dashboard.sql."
)

# ---- Load data ------------------------------------------------------------
query = f"""
    SELECT *
      FROM marts.v_tenant_billing_dashboard
     WHERE 1=1
       {filters.tenant_clause()}
       {filters.month_clause('year_month')}
     ORDER BY year_month, tenant_id
"""
df = read_sql(query, params=filters.params())

if df.empty:
    st.warning("No billing data for the current filters.")
    st.stop()

# ---- Latest-month fleet roll-up -------------------------------------------
latest_month = df["year_month"].max()
latest_rows = df[df["year_month"] == latest_month].copy()
prior_month_rows = df[df["year_month"] == sorted(df["year_month"].unique())[-2]] \
    if df["year_month"].nunique() > 1 else None

# ---- Row 1: 4 summary cards -----------------------------------------------
st.markdown(
    "<style>div[data-testid='stMetricValue'] {font-size: 36px;}</style>",
    unsafe_allow_html=True,
)
c1, c2, c3, c4 = st.columns(4)

total_tenants = latest_rows["tenant_id"].nunique()
total_devices = int(latest_rows["active_devices"].sum())
total_revenue = float(latest_rows["estimated_revenue"].sum())
total_storage_gb = float(latest_rows["data_volume_gb"].sum())

# Compute deltas relative to prior month
def _int_delta(now: int, then: int | None) -> str | None:
    if then is None:
        return None
    return f"{now - then:+d}"


def _pct_delta(now: float, then: float | None) -> str | None:
    if then is None or then == 0:
        return None
    return f"{(now - then) / then * 100:+.1f}%"


prior_tenants = (
    prior_month_rows["tenant_id"].nunique() if prior_month_rows is not None else None
)
prior_devices = (
    int(prior_month_rows["active_devices"].sum())
    if prior_month_rows is not None else None
)
prior_revenue = (
    float(prior_month_rows["estimated_revenue"].sum())
    if prior_month_rows is not None else None
)
prior_storage = (
    float(prior_month_rows["data_volume_gb"].sum())
    if prior_month_rows is not None else None
)

c1.metric("Total tenants", f"{total_tenants:,}",
          delta=_int_delta(total_tenants, prior_tenants))
c2.metric("Active devices", f"{total_devices:,}",
          delta=_int_delta(total_devices, prior_devices))
c3.metric("Revenue estimate (DA)", f"{total_revenue:,.0f}",
          delta=_pct_delta(total_revenue, prior_revenue))
c4.metric("Storage used (GB)", f"{total_storage_gb:.2f}",
          delta=_pct_delta(total_storage_gb, prior_storage))

st.caption(
    f"Latest: **{latest_month}** · "
    f"pricing tiers: 1→100 DA (1-50), 2→80 DA (51-200), 3→60 DA (201+)"
)

# ---- Row 2: Per-tenant billing table --------------------------------------
st.subheader(f"Billing breakdown · {latest_month}")
billing_cols = [
    "tenant_id", "tenant_name", "active_devices", "active_vehicles",
    "total_trips_processed", "total_alerts_generated", "total_events",
    "data_volume_gb", "pricing_tier", "price_per_device", "estimated_revenue",
    "events_per_device", "devices_mom_growth_pct",
]
present_cols = [c for c in billing_cols if c in latest_rows.columns]
display = latest_rows[present_cols].sort_values("estimated_revenue", ascending=False)
st.dataframe(display, use_container_width=True, hide_index=True)

# ---- Row 3: Usage trends --------------------------------------------------
st.subheader("Usage trends")
tcol1, tcol2, tcol3 = st.columns(3)

# Aggregate over time across all tenants for the trend
agg_over_time = (
    df.groupby("year_month", as_index=False)
    .agg(
        active_devices=("active_devices", "sum"),
        data_volume_gb=("data_volume_gb", "sum"),
        estimated_revenue=("estimated_revenue", "sum"),
        total_trips_processed=("total_trips_processed", "sum"),
    )
    .sort_values("year_month")
)

with tcol1:
    st.markdown("**Active devices / month**")
    fig = px.bar(agg_over_time, x="year_month", y="active_devices")
    fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
    st.plotly_chart(fig, use_container_width=True)

with tcol2:
    st.markdown("**Storage growth (GB)**")
    fig = px.line(agg_over_time, x="year_month", y="data_volume_gb", markers=True)
    fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
    st.plotly_chart(fig, use_container_width=True)

with tcol3:
    st.markdown("**Revenue trend (DA)**")
    fig = px.area(agg_over_time, x="year_month", y="estimated_revenue")
    fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
    st.plotly_chart(fig, use_container_width=True)

# ---- Row 4: Per-tenant MoM growth comparison ------------------------------
st.subheader(f"Per-tenant MoM growth · {latest_month}")
growth_cols = ["devices_mom_growth_pct", "trips_mom_growth_pct",
               "alerts_mom_growth_pct"]
present_growth = [c for c in growth_cols if c in latest_rows.columns]
if present_growth:
    long_df = latest_rows[["tenant_id", *present_growth]].melt(
        id_vars="tenant_id", var_name="metric", value_name="growth_pct"
    )
    fig = px.bar(long_df, x="tenant_id", y="growth_pct", color="metric", barmode="group")
    fig.update_yaxes(title="MoM growth %")
    fig.add_hline(y=0, line_dash="dash", line_color="grey")
    st.plotly_chart(fig, use_container_width=True)

# ---- Pricing tier breakdown -----------------------------------------------
st.subheader("Pricing tier mix")
if "pricing_tier" in latest_rows.columns:
    tier_breakdown = (
        latest_rows.groupby("pricing_tier")
        .agg(tenants=("tenant_id", "nunique"),
             devices=("active_devices", "sum"),
             revenue=("estimated_revenue", "sum"))
        .reset_index()
    )
    tcol1, tcol2 = st.columns(2)
    with tcol1:
        st.dataframe(tier_breakdown, use_container_width=True, hide_index=True)
    with tcol2:
        fig = px.bar(
            tier_breakdown,
            x="pricing_tier",
            y="revenue",
            color="pricing_tier",
            title="Revenue contribution by tier",
        )
        fig.update_layout(margin={"l": 0, "r": 0, "t": 30, "b": 0}, height=280,
                          showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

# ---- Export ---------------------------------------------------------------
st.subheader("Export")
ecol1, ecol2 = st.columns(2)
with ecol1:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"billing_{latest_month}.csv",
        mime="text/csv",
    )
with ecol2:
    excel_buf = BytesIO()
    try:
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="raw", index=False)
            latest_rows.to_excel(writer, sheet_name=f"{latest_month}", index=False)
            agg_over_time.to_excel(writer, sheet_name="trends", index=False)
        st.download_button(
            "Download Excel",
            data=excel_buf.getvalue(),
            file_name=f"billing_{latest_month}.xlsx",
            mime=(
                "application/vnd.openxmlformats-"
                "officedocument.spreadsheetml.sheet"
            ),
        )
    except ImportError:
        st.caption("Install `openpyxl` for Excel export.")

# ---- Raw table ------------------------------------------------------------
st.subheader("All months · per-tenant detail")
st.dataframe(df, use_container_width=True, hide_index=True)
