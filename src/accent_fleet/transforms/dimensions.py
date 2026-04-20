"""
Dimension loaders.

Dimensions are small (5 tenants, 638 vehicles, 549 devices, 294 drivers)
and rarely change, so we full-refresh them via the idempotent SQL files.
No watermark needed.
"""

from __future__ import annotations

import structlog

from accent_fleet.db import run_sql_file, transaction

log = structlog.get_logger()


DIMENSION_FILES = [
    "01_dim_tenant.sql",
    "02_dim_vehicle.sql",
    "03_dim_device.sql",
    "04_dim_driver.sql",
    "05_dim_date_hour.sql",
]


def refresh_all_dimensions() -> None:
    """Run every dimension loader in order. Idempotent."""
    with transaction() as conn:
        for filename in DIMENSION_FILES:
            log.info("dim_refresh.start", file=filename)
            run_sql_file(conn, filename)
            log.info("dim_refresh.done", file=filename)
