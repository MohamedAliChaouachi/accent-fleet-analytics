"""Shared Streamlit layout helpers + global filter sidebar."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

import streamlit as st


# Color palette aligned with the executive dashboard SQL view tier labels.
RISK_COLORS = {
    "low": "#2ecc71",
    "moderate": "#f1c40f",
    "high": "#e67e22",
    "critical": "#e74c3c",
}


@dataclass
class Filters:
    """Resolved sidebar state. Empty `tenant_ids` means "all tenants"."""
    start: date
    end: date
    tenant_ids: list[int] = field(default_factory=list)

    # ---- helpers for SQL composition --------------------------------------

    @property
    def start_iso(self) -> str:
        return self.start.isoformat()

    @property
    def end_iso(self) -> str:
        return self.end.isoformat()

    @property
    def start_month(self) -> str:
        """YYYY-MM for use against CHAR(7) year_month columns."""
        return self.start.strftime("%Y-%m")

    @property
    def end_month(self) -> str:
        return self.end.strftime("%Y-%m")

    def tenant_clause(self, alias: str = "") -> str:
        """
        Return a SQL fragment like 'AND m.tenant_id = ANY(:tenant_ids)' or ''.

        Use together with `.params()` so the bind parameters always match.
        Pass `alias='m'` if you've aliased the table; defaults to bare column.
        """
        if not self.tenant_ids:
            return ""
        prefix = f"{alias}." if alias else ""
        return f"AND {prefix}tenant_id = ANY(CAST(:tenant_ids AS int[]))"

    def date_clause(self, column: str) -> str:
        """SQL fragment 'AND <column> BETWEEN :start AND :end'."""
        return f"AND {column} BETWEEN :start AND :end"

    def month_clause(self, column: str) -> str:
        """SQL fragment for a CHAR(7) year_month column."""
        return f"AND {column} BETWEEN :start_month AND :end_month"

    def params(self) -> dict:
        """All bind parameters together. Safe to pass to read_sql even when unused."""
        return {
            "start": self.start_iso,
            "end": self.end_iso,
            "start_month": self.start_month,
            "end_month": self.end_month,
            "tenant_ids": self.tenant_ids or None,
        }


def apply_layout(*, page_title: str, page_icon: str = "🚚") -> None:
    """Page-level config; safe to call at the top of every page."""
    st.set_page_config(
        page_title=page_title,
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )


def render_sidebar_filters() -> Filters:
    """
    Render the global filter sidebar and return a Filters object.

    State is persisted via st.session_state so the same selection survives
    page navigation. The returned object is also stashed under
    `st.session_state['filters']` for pages that don't capture the return.
    """
    with st.sidebar:
        st.header("Filters")

        # Date range — default last 90 days so monthly views have multiple
        # buckets out of the box.
        today = date.today()
        default_start = today - timedelta(days=90)
        date_range = st.date_input(
            "Date range",
            value=st.session_state.get("filter_date_range", (default_start, today)),
            key="filter_date_range",
        )
        # Streamlit returns a 1- or 2-tuple while the user is mid-edit.
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start, end = date_range
        else:
            start = end = today

        # Tenant filter — free-text comma-separated list. We avoid hitting
        # the DB for a tenant list to keep the sidebar render path stateless
        # (and resilient to DB outages).
        tenants_text = st.text_input(
            "Tenant ids (comma-separated, blank = all)",
            value=st.session_state.get("filter_tenants_text", ""),
            key="filter_tenants_text",
        )
        tenant_ids = [
            int(x.strip()) for x in tenants_text.split(",") if x.strip().isdigit()
        ]

        if tenant_ids:
            st.caption(f"Scope: {len(tenant_ids)} tenant(s), {start} → {end}")
        else:
            st.caption(f"Scope: all tenants, {start} → {end}")

        # Freshness footer — rendered in the sidebar so it survives any
        # st.stop() the page body might call when filters return no rows.
        # Imported lazily to avoid a theme<->cache import cycle.
        from dashboard.lib.cache import last_etl_run_at

        st.divider()
        ts = last_etl_run_at()
        if ts:
            st.caption(f"Last ETL refresh: **{ts}**")
        else:
            st.caption("Last ETL refresh: _no successful runs recorded_")

    filters = Filters(start=start, end=end, tenant_ids=tenant_ids)
    st.session_state["filters"] = filters
    return filters
