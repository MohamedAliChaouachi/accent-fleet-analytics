"""Tiny query helpers cached with @st.cache_data so pages stay snappy."""

from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

from dashboard.lib.db import get_engine

DEFAULT_TTL = 300  # 5 minutes — matches the Prefect incremental cadence.


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def read_sql(query: str, params: dict | None = None) -> pd.DataFrame:
    """Cached SQL read. Use for SELECTs against marts/views."""
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params or {})


@st.cache_data(ttl=60, show_spinner=False)
def last_etl_run_at() -> str | None:
    """Timestamp of the most recent Prefect run. Used in the Home footer."""
    try:
        df = read_sql(
            """
            SELECT MAX(finished_at)::text AS ts
              FROM warehouse.etl_run_log
             WHERE status = 'success'
            """
        )
        ts = df.iloc[0]["ts"] if not df.empty else None
        return ts
    except Exception:  # noqa: BLE001
        return None
