"""Predictive alerts — reads marts.v_predictive_alerts_dashboard and
marts.v_real_time_alerts_stream.

Two-tab layout:
  - Active alerts   (proactive: high-risk prediction, maintenance due,
                     fuel anomaly, inactive, cluster migration, harsh spike)
  - Live stream     (last 24h notification + overspeed + harsh)

The page auto-refreshes every 60 seconds via st.cache_data TTL, not via
streamlit-autorefresh (avoids the extra dep).
"""

from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.lib.cache import read_sql
from dashboard.lib.theme import apply_layout, render_sidebar_filters

apply_layout(page_title="Predictive alerts")
filters = render_sidebar_filters()

st.title("Predictive alerts")
st.caption(
    "Proactive alerts from `marts.v_predictive_alerts_dashboard` plus the "
    "last-24h live stream from `marts.v_real_time_alerts_stream`. Refreshes "
    "every 5 minutes (per-query cache TTL)."
)

# ---- Severity → icon map (reused by both tabs) ----------------------------
SEVERITY_ICON = {
    "critical": "🔴",
    "high": "🟠",
    "medium": "🟡",
    "low": "🟢",
}
SEVERITY_ORDER = ["critical", "high", "medium", "low"]

# Manual refresh — clears the @st.cache_data layer so the next query hits
# the DB instead of returning the cached 5-minute-old result.
if st.button("🔄 Refresh now", help="Clear cache and re-query"):
    st.cache_data.clear()
    st.rerun()

tab1, tab2 = st.tabs(["Active alerts (proactive)", "Live stream (last 24h)"])

# ===========================================================================
# Tab 1: Predictive / proactive alerts
# ===========================================================================
with tab1:
    sev_filter = st.multiselect(
        "Severity filter",
        options=SEVERITY_ORDER,
        default=SEVERITY_ORDER,
        key="alerts_sev_filter",
    )
    type_filter = st.multiselect(
        "Alert type filter",
        options=[
            "high_risk_prediction", "maintenance_due", "fuel_anomaly",
            "inactive_device", "cluster_migration", "harsh_event_spike",
        ],
        default=[
            "high_risk_prediction", "maintenance_due", "fuel_anomaly",
            "inactive_device", "cluster_migration", "harsh_event_spike",
        ],
        key="alerts_type_filter",
    )

    alerts_query = f"""
        SELECT *
          FROM marts.v_predictive_alerts_dashboard
         WHERE 1=1
           {filters.tenant_clause()}
        ORDER BY detected_at DESC
    """
    alerts = read_sql(alerts_query, params=filters.params())

    if alerts.empty:
        st.success("No active alerts — system is healthy.")
        st.stop()

    # Apply UI-side filters
    if sev_filter:
        alerts = alerts[alerts["severity"].isin(sev_filter)]
    if type_filter:
        alerts = alerts[alerts["alert_type"].isin(type_filter)]

    # ---- Summary cards ----------------------------------------------------
    st.markdown(
        "<style>div[data-testid='stMetricValue'] {font-size: 36px;}</style>",
        unsafe_allow_html=True,
    )
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Active alerts", f"{len(alerts):,}")
    s2.metric(
        "High severity",
        f"{(alerts['severity'].isin(['high', 'critical'])).sum():,}",
    )
    # "Resolved today" is a future feature (needs an ack table). Until then,
    # surface "alerts detected in last 24h" as a useful proxy.
    last_24h = alerts[
        pd.to_datetime(alerts["detected_at"], utc=True)
        > datetime.now(UTC) - pd.Timedelta(hours=24)
    ]
    s3.metric("Detected last 24h", f"{len(last_24h):,}")
    s4.metric(
        "Unique devices",
        f"{alerts['device_id'].nunique():,}",
    )

    # ---- Alert table ------------------------------------------------------
    st.subheader("Active alerts")
    display = alerts.copy()
    display["icon"] = display["severity"].map(SEVERITY_ICON).fillna("⚪")
    display["detected_at"] = pd.to_datetime(display["detected_at"], utc=True).dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    cols_to_show = [
        c
        for c in (
            "icon", "tenant_id", "device_id", "alert_type", "severity",
            "detected_at", "alert_message", "assigned_to",
        )
        if c in display.columns
    ]
    st.dataframe(
        display[cols_to_show].head(200),
        use_container_width=True,
        hide_index=True,
    )
    if len(display) > 200:
        st.caption(f"Showing first 200 of {len(display)} alerts. Use filters to narrow.")

    # ---- Alert analytics --------------------------------------------------
    st.subheader("Alert analytics")
    acol1, acol2, acol3 = st.columns(3)

    with acol1:
        st.markdown("**By type**")
        by_type = alerts["alert_type"].value_counts().reset_index()
        by_type.columns = ["alert_type", "count"]
        fig = px.pie(by_type, names="alert_type", values="count", hole=0.4)
        fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
        st.plotly_chart(fig, use_container_width=True)

    with acol2:
        st.markdown("**By severity**")
        by_sev = alerts["severity"].value_counts().reset_index()
        by_sev.columns = ["severity", "count"]
        fig = px.bar(
            by_sev,
            x="severity",
            y="count",
            color="severity",
            color_discrete_map={
                "critical": "#e74c3c",
                "high":     "#e67e22",
                "medium":   "#f1c40f",
                "low":      "#2ecc71",
            },
        )
        fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
        st.plotly_chart(fig, use_container_width=True)

    with acol3:
        st.markdown("**Trend (30 days)**")
        trend = alerts.copy()
        trend["date"] = pd.to_datetime(trend["detected_at"], utc=True).dt.date
        cutoff = datetime.now(UTC).date() - pd.Timedelta(days=30).to_pytimedelta()
        trend = trend[trend["date"] >= cutoff]
        if not trend.empty:
            daily = trend.groupby("date").size().reset_index(name="alerts")
            fig = px.line(daily, x="date", y="alerts", markers=True)
            fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No alerts in last 30 days.")

    # ---- Export -----------------------------------------------------------
    st.subheader("Export")
    ecol1, ecol2 = st.columns(2)
    with ecol1:
        csv_bytes = alerts.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"alerts_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    with ecol2:
        excel_buf = BytesIO()
        try:
            with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
                alerts.to_excel(writer, sheet_name="alerts", index=False)
            st.download_button(
                "Download Excel",
                data=excel_buf.getvalue(),
                file_name=f"alerts_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime=(
                    "application/vnd.openxmlformats-"
                    "officedocument.spreadsheetml.sheet"
                ),
            )
        except ImportError:
            st.caption("Install `openpyxl` for Excel export.")

# ===========================================================================
# Tab 2: Live stream (last 24h)
# ===========================================================================
with tab2:
    st.caption(
        "Notifications, overspeed, harsh events, and silent devices from "
        "the last 24 hours — ordered by priority score (severity × recency × "
        "device-risk multiplier)."
    )
    stream_query = f"""
        SELECT *
          FROM marts.v_real_time_alerts_stream
         WHERE 1=1
           {filters.tenant_clause()}
         ORDER BY priority_score DESC, detected_at DESC
         LIMIT 500
    """
    stream = read_sql(stream_query, params=filters.params())

    if stream.empty:
        st.success("Nothing in the last 24 hours — quiet fleet.")
    else:
        # ---- Summary cards ----
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Events (last 24h)", f"{len(stream):,}")
        s2.metric(
            "Critical / high",
            f"{(stream['severity'].isin(['critical', 'high'])).sum():,}",
        )
        s3.metric(
            "Avg priority",
            f"{stream['priority_score'].astype(float).mean():.1f}",
        )
        s4.metric(
            "Unique devices",
            f"{stream['device_id'].nunique():,}",
        )

        # ---- Stream table ----
        st.subheader("Live events")
        display = stream.copy()
        display["icon"] = display["severity"].map(SEVERITY_ICON).fillna("⚪")
        display["detected_at"] = pd.to_datetime(
            display["detected_at"], utc=True
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
        show_cols = [
            c
            for c in (
                "icon", "priority_score", "detected_at", "tenant_id",
                "device_id", "alert_category", "severity", "alert_message",
                "device_risk_category",
            )
            if c in display.columns
        ]
        st.dataframe(
            display[show_cols].head(200),
            use_container_width=True,
            hide_index=True,
        )

        # ---- Breakdown ----
        bcol1, bcol2 = st.columns(2)
        with bcol1:
            st.markdown("**Events by category**")
            cat = stream["alert_category"].value_counts().reset_index()
            cat.columns = ["category", "count"]
            fig = px.bar(cat, x="category", y="count", color="category")
            fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280,
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        with bcol2:
            st.markdown("**Events by hour (last 24h)**")
            stream_hours = stream.copy()
            stream_hours["hour"] = pd.to_datetime(
                stream_hours["detected_at"], utc=True
            ).dt.hour
            by_hour = stream_hours.groupby("hour").size().reset_index(name="events")
            fig = px.line(by_hour, x="hour", y="events", markers=True)
            fig.update_xaxes(dtick=1)
            fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=280)
            st.plotly_chart(fig, use_container_width=True)
