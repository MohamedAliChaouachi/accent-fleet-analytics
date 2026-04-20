"""
Unit tests for the WatermarkWindow dataclass and logical semantics.

Integration tests that actually hit Postgres live under test_incremental_semantics.py
and are marked @pytest.mark.integration.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from accent_fleet.db.watermark import WatermarkWindow


def test_window_is_empty_when_equal():
    t = datetime(2026, 4, 10)
    assert WatermarkWindow(start=t, end=t, table_name="fact_trip").is_empty


def test_window_is_empty_when_reversed():
    t0 = datetime(2026, 4, 10)
    t1 = datetime(2026, 4, 9)
    assert WatermarkWindow(start=t0, end=t1, table_name="fact_trip").is_empty


def test_window_non_empty():
    t0 = datetime(2026, 4, 10, 12, 0)
    t1 = t0 + timedelta(minutes=5)
    assert not WatermarkWindow(start=t0, end=t1, table_name="fact_trip").is_empty
