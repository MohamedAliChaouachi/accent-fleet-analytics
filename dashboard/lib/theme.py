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


def _ensure_dashboard_principal() -> None:
    """
    Stamp a "dashboard service" superadmin Principal into the request-scoped
    ContextVar that drives the RLS listener in
    src/accent_fleet/db/engine.py:_set_tenant_guc.

    Why this exists
    ---------------
    The dashboard reuses the same SQLAlchemy engine as the API and connects
    as `accent_app` (NOBYPASSRLS). Post-B7 + FORCE RLS (sql/55_force_rls.sql),
    a query fired without a Principal in scope hits the listener's "no
    principal" branch, no GUC / role-swap is emitted, and the RLS policies
    in sql/51_rls_policies.sql evaluate to NULL ⇒ zero rows. The dashboard
    is an internal ops console with no per-user login, so the natural fit
    is a single ambient "service superadmin" Principal — same pattern an
    authenticated superadmin user would produce, just stamped at page-init
    instead of by the API's auth middleware. The listener then emits
    `SET LOCAL ROLE accent_superadmin`, the transaction runs with
    BYPASSRLS via the membership granted in
    sql/54_grant_superadmin_membership.sql, and the role reverts at COMMIT.

    Idempotency
    -----------
    Streamlit re-runs the page script on every interaction. Whether the
    ContextVar survives between runs depends on threading; either way we
    only set when empty, so this is safe to call at the top of every page.

    Import-failure mode
    -------------------
    If `app.auth.principal` isn't importable (e.g. an older dashboard
    image built before this fix), we log loudly and return rather than
    crashing the page render. The listener will then soft-fail and the
    user will see the same empty-data symptom — but at least the cause
    appears in container logs.
    """
    try:
        from app.auth.principal import (
            Principal,
            current_principal,
            set_principal,
        )
    except ImportError:
        import logging

        logging.getLogger(__name__).error(
            "dashboard: app.auth.principal not importable; "
            "RLS will clamp every query — rebuild the dashboard image"
        )
        return

    if current_principal() is None:
        set_principal(
            Principal(
                user_id=0,
                tenant_id=None,
                role="superadmin",
                email="dashboard@service",
            )
        )


def apply_layout(*, page_title: str, page_icon: str = "🚚") -> None:
    """Page-level config; safe to call at the top of every page."""
    # Must run before any read_sql() so the engine's RLS listener picks up
    # a superadmin Principal and emits SET LOCAL ROLE accent_superadmin
    # for every transaction. See _ensure_dashboard_principal() for the
    # full rationale.
    _ensure_dashboard_principal()
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
