"""
Shared pytest fixtures.

These tests are UNIT tests — they do not require a live Postgres. Integration
tests (marked `@pytest.mark.integration`) can be skipped with:

    pytest -m "not integration"
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest


# ---------------------------------------------------------------------------
@pytest.fixture
def path_sample_df() -> pl.DataFrame:
    """
    A small sample of path-like rows covering every cleaning rule edge case.

    Layout matches BatchStagingSource's path projection so the rule engine
    can operate on it unmodified.
    """
    return pl.DataFrame(
        [
            # Valid row
            {
                "tenant_id": 235, "device_id": 425239,
                "begin_path_time": datetime(2026, 4, 10, 12, 0, 0),
                "path_duration": 360, "distance_driven": 3.5, "max_speed": 72,
                "fuel_used": 0.8,
            },
            # C1: pre-2019 epoch error
            {
                "tenant_id": 235, "device_id": 425239,
                "begin_path_time": datetime(1970, 1, 1),
                "path_duration": 300, "distance_driven": 2.0, "max_speed": 50,
                "fuel_used": 0.0,
            },
            # C2: negative duration
            {
                "tenant_id": 235, "device_id": 425239,
                "begin_path_time": datetime(2026, 4, 10, 12, 5, 0),
                "path_duration": -5, "distance_driven": 2.0, "max_speed": 60,
                "fuel_used": 0.0,
            },
            # C3: zero distance
            {
                "tenant_id": 235, "device_id": 425239,
                "begin_path_time": datetime(2026, 4, 10, 12, 10, 0),
                "path_duration": 120, "distance_driven": 0.0, "max_speed": 10,
                "fuel_used": 0.0,
            },
            # C4: overflow fuel
            {
                "tenant_id": 235, "device_id": 425239,
                "begin_path_time": datetime(2026, 4, 10, 12, 15, 0),
                "path_duration": 300, "distance_driven": 5.0, "max_speed": 70,
                "fuel_used": 2_147_000_000.0,
            },
            # C5: clamp speed
            {
                "tenant_id": 235, "device_id": 425239,
                "begin_path_time": datetime(2026, 4, 10, 12, 20, 0),
                "path_duration": 600, "distance_driven": 20.0, "max_speed": 220,
                "fuel_used": 1.5,
            },
        ]
    )
