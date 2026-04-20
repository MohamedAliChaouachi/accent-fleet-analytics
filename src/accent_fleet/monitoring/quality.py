"""
Quality and freshness monitoring.

Two responsibilities:
  1. Run sql/99_validation_suite.sql and summarise pass/fail.
  2. Check freshness — is warehouse.fact_trip behind staging.path?

Designed to be called by the pipeline flow at the end of every run.
Failures are logged but do NOT abort the pipeline — monitoring is
observational, not a gate. Gates belong in data contracts, not monitors.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import text

from accent_fleet.config import load_pipeline_config
from accent_fleet.db import get_engine
from accent_fleet.db.sql_loader import load_sql, split_sql_statements

log = structlog.get_logger()


@dataclass
class QualityReport:
    """One run of the validation suite."""

    checks: list[dict[str, Any]] = field(default_factory=list)
    freshness: dict[str, Any] = field(default_factory=dict)

    @property
    def all_passed(self) -> bool:
        return all(c.get("passed", False) for c in self.checks)

    @property
    def failed_checks(self) -> list[str]:
        return [c["check_name"] for c in self.checks if not c.get("passed", False)]


# ---------------------------------------------------------------------------
def run_validation_suite() -> QualityReport:
    """
    Execute 99_validation_suite.sql and collect one row per check.

    The SQL file has 8 SELECT statements separated by semicolons. We split
    and run each, collecting a dict per check.
    """
    sql_text = load_sql("99_validation_suite.sql")
    statements = [
        s for s in split_sql_statements(sql_text)
        if s.strip().upper().startswith("SELECT")
    ]

    report = QualityReport()
    engine = get_engine()
    with engine.connect() as conn:
        for stmt in statements:
            try:
                row = conn.execute(text(stmt)).mappings().first()
                if row:
                    report.checks.append(dict(row))
            except Exception as exc:  # noqa: BLE001
                log.error("validation.check_failed", sql=stmt[:80], error=str(exc))
                report.checks.append({
                    "check_name": "ERROR",
                    "passed": False,
                    "error": str(exc),
                })
    report.freshness = check_freshness()
    return report


# ---------------------------------------------------------------------------
def check_freshness() -> dict[str, Any]:
    """
    How far behind staging is the warehouse?

    Returns a dict with per-fact lag in seconds and a passed/failed flag
    against the thresholds in config/pipeline.yaml.
    """
    cfg = load_pipeline_config()["monitoring"]["freshness"]

    queries = {
        "fact_trip": (
            "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(begin_path_time::timestamptz))) AS lag "
            "FROM warehouse.fact_trip"
        ),
        "fact_stop": (
            "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(stop_start::timestamptz))) AS lag "
            "FROM warehouse.fact_stop"
        ),
    }

    out: dict[str, Any] = {}
    engine = get_engine()
    with engine.connect() as conn:
        for fact, sql in queries.items():
            row = conn.execute(text(sql)).first()
            lag_sec = float(row.lag) if row and row.lag is not None else None
            threshold_key = f"{fact}_max_lag_minutes"
            threshold_sec = cfg.get(threshold_key, 60) * 60
            out[fact] = {
                "lag_seconds": lag_sec,
                "threshold_seconds": threshold_sec,
                "passed": (lag_sec is not None and lag_sec <= threshold_sec),
            }
    return out


# ---------------------------------------------------------------------------
def recent_rejection_summary(hours: int = 24) -> list[dict[str, Any]]:
    """
    Rejection counts by rule over the last N hours. Used by the dashboard
    notebook and for weekly review of quarantine health.
    """
    sql = """
        SELECT rule_id, COUNT(*) AS rejected
        FROM warehouse.quarantine_rejected
        WHERE rejected_at >= :since
        GROUP BY rule_id
        ORDER BY rejected DESC
    """
    since = datetime.utcnow() - timedelta(hours=hours)
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), {"since": since}).mappings().all()
        return [dict(r) for r in rows]
