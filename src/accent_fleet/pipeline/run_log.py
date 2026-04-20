"""
Run-log helpers.

Small wrapper around warehouse.etl_run_log that gives the pipeline a
clean "begin_run / end_run" API without leaking SQL everywhere.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from accent_fleet.config import config_hash
from accent_fleet.db import get_engine


def begin_run(
    *,
    mode: str,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> int:
    """Insert a 'running' row into etl_run_log and return its run_id."""
    sql = text(
        """
        INSERT INTO warehouse.etl_run_log
          (mode, status, window_start, window_end, config_hash)
        VALUES (:mode, 'running', :ws, :we, :ch)
        RETURNING run_id
        """
    )
    with get_engine().begin() as conn:
        return int(
            conn.execute(
                sql,
                {
                    "mode": mode,
                    "ws": window_start,
                    "we": window_end,
                    "ch": config_hash(),
                },
            ).scalar_one()
        )


def end_run(
    run_id: int,
    *,
    status: str = "success",
    rows_read: int = 0,
    rows_loaded: int = 0,
    rows_rejected: int = 0,
    rejected_by_rule: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    """Mark a run as finished with its final counters."""
    sql = text(
        """
        UPDATE warehouse.etl_run_log
        SET finished_at      = NOW(),
            status           = :status,
            rows_read        = :rr,
            rows_loaded      = :rl,
            rows_rejected    = :rej,
            rejected_by_rule = :by_rule,
            error_message    = :err
        WHERE run_id = :run_id
        """
    )
    with get_engine().begin() as conn:
        conn.execute(
            sql,
            {
                "run_id": run_id,
                "status": status,
                "rr": rows_read,
                "rl": rows_loaded,
                "rej": rows_rejected,
                "by_rule": json.dumps(rejected_by_rule or {}),
                "err": error_message,
            },
        )
