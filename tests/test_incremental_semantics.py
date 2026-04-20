"""
Integration tests: incremental semantics + idempotency.

These require a live Postgres with the schemas bootstrapped. They are
gated by @pytest.mark.integration so `pytest -m "not integration"` skips
them in CI without a DB.

The key property under test: re-running the same window twice produces
identical output (no duplicates, no drift). This is THE core guarantee
of the streaming refactor.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from accent_fleet.db import get_engine
from accent_fleet.pipeline import incremental_flow


pytestmark = pytest.mark.integration


def _count_fact_trip() -> int:
    with get_engine().connect() as conn:
        return int(
            conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_trip")).scalar_one()
        )


def _checksum_fact_trip() -> str:
    """Simple content hash for equality comparison across runs."""
    with get_engine().connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT md5(string_agg(
                    tenant_id::text || '|' || device_id::text
                       || '|' || begin_path_time::text
                       || '|' || COALESCE(distance_km::text, 'null')
                       || '|' || COALESCE(max_speed_kmh::text, 'null'),
                    ',' ORDER BY tenant_id, device_id, begin_path_time
                )) AS cksum
                FROM warehouse.fact_trip
                """
            )
        ).first()
    return row.cksum if row and row.cksum else ""


def test_rerunning_same_window_is_idempotent():
    """
    Property: running the same incremental window twice must yield the same
    row count AND the same content hash. If this test fails, the UPSERT is
    broken or the watermark moved incorrectly.
    """
    end = datetime.utcnow() - timedelta(minutes=2)
    incremental_flow(window_end=end)
    n1 = _count_fact_trip()
    h1 = _checksum_fact_trip()

    incremental_flow(window_end=end)
    n2 = _count_fact_trip()
    h2 = _checksum_fact_trip()

    assert n1 == n2, f"Row count changed on re-run: {n1} -> {n2}"
    assert h1 == h2, "Content hash drifted on idempotent re-run"


def test_watermark_advances_monotonically():
    """The watermark for fact_trip must never go backward."""
    with get_engine().connect() as conn:
        before = conn.execute(
            text(
                """
                SELECT last_event_time FROM warehouse.etl_watermark
                WHERE table_name = 'fact_trip'
                """
            )
        ).scalar_one_or_none()

    incremental_flow()

    with get_engine().connect() as conn:
        after = conn.execute(
            text(
                """
                SELECT last_event_time FROM warehouse.etl_watermark
                WHERE table_name = 'fact_trip'
                """
            )
        ).scalar_one_or_none()

    if before is not None and after is not None:
        assert after >= before
