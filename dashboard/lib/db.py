"""Cached SQLAlchemy engine + helpers for the dashboard.

Reuses the same Settings object as the ETL/API so a single .env drives
the whole stack (including the Azure-hosted Postgres TLS settings).
"""

from __future__ import annotations

import streamlit as st
from sqlalchemy import Engine

from accent_fleet.db.engine import get_engine as _get_engine


@st.cache_resource
def get_engine() -> Engine:
    """One engine per Streamlit server process."""
    return _get_engine()
