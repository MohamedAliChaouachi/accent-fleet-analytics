"""
Incremental fact loader.

The hot path of the refactor. One function that:
  1. Reads the watermark for the target fact.
  2. Computes the (start, end) event-time window with overlap.
  3. Runs the fact's SQL file (10_fact_trip_incremental, etc.) bound
     with :window_start, :window_end, :etl_run_id.
  4. Advances the watermark in the same transaction.

Everything lives in one DB transaction so a crash halfway leaves no
partial state — the next run will repeat the same window, and the
UPSERT on natural keys makes that a no-op for already-landed rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import structlog

from accent_fleet.config import load_pipeline_config, settings
from accent_fleet.db import WatermarkStore, run_sql_file, transaction

log = structlog.get_logger()


# SQL file per fact target
FACT_SQL: dict[str, str] = {
    "fact_trip":                  "10_fact_trip_incremental.sql",
    "fact_overspeed":             "11_fact_overspeed_incremental.sql",
    "fact_stop":                  "12_fact_stop_incremental.sql",
    "fact_speed_notification":    "13_fact_speed_notification_incr.sql",
    "fact_daily_activity":        "14_fact_daily_activity_incr.sql",
    # Archive-derived facts (Project 1: Driver Behavior Scoring)
    "fact_harsh_event":           "15_fact_harsh_event_incremental.sql",
    "fact_telemetry_daily":       "16_fact_telemetry_daily_incr.sql",
    # BI-coverage facts (Project 2: Fleet BI Dashboard)
    "fact_notification":          "17_fact_notification_incr.sql",
    "fact_maintenance":           "18_fact_maintenance_incr.sql",
    "fact_maintenance_line":      "19_fact_maintenance_line_incr.sql",
    "fact_fueling":               "24_fact_fueling_incr.sql",
}


def _extra_params_for_fact(fact_name: str, cfg: dict) -> dict:
    """
    Per-fact extra named parameters bound to the SQL file. Defaults come
    from `config/pipeline.yaml` under `archive_thresholds:` and
    `archive_telemetry:` keys (see those YAMLs for documentation).
    """
    if fact_name == "fact_harsh_event":
        thr = cfg.get("archive_thresholds", {})
        return {
            "thresh_brake":   int(thr.get("brake", 40)),
            "thresh_accel":   int(thr.get("accel", 40)),
            "thresh_corner":  int(thr.get("corner", 40)),
            "thresh_high":    int(thr.get("high", 60)),
            "thresh_extreme": int(thr.get("extreme", 80)),
        }
    if fact_name == "fact_telemetry_daily":
        tel = cfg.get("archive_telemetry", {})
        return {
            "ping_seconds":       int(tel.get("ping_seconds", 30)),
            "rpm_high_threshold": int(tel.get("rpm_high_threshold", 3000)),
        }
    return {}


@dataclass
class FactLoadResult:
    """What a single fact load returns to the caller."""

    fact_name: str
    rows_loaded: int
    window_start: datetime
    window_end: datetime
    new_max_event_time: datetime | None


def load_fact_incremental(
    fact_name: str,
    *,
    run_id: int,
    window_end: datetime | None = None,
) -> FactLoadResult:
    """
    Run the incremental loader for one fact.

    Parameters
    ----------
    fact_name : str
        One of FACT_SQL keys (e.g. "fact_trip").
    run_id : int
        The warehouse.etl_run_log.run_id this load belongs to.
    window_end : datetime | None
        Upper bound of the window (exclusive). Defaults to utcnow().
    """
    if fact_name not in FACT_SQL:
        raise ValueError(f"Unknown fact: {fact_name}")

    sql_file = FACT_SQL[fact_name]
    s = settings()
    cfg = load_pipeline_config()
    overlap = s.pipeline_overlap_minutes
    max_age_cap = datetime.fromisoformat(
        cfg["window"]["min_event_time"].replace("Z", "+00:00")
    ).replace(tzinfo=None)

    with transaction() as conn:
        ws = WatermarkStore(conn)
        window = ws.get_window(
            fact_name,
            overlap_minutes=overlap,
            now=window_end,
            max_age_cap=max_age_cap,
        )

        if window.is_empty:
            log.info("fact_load.skip_empty_window", fact=fact_name,
                     start=window.start, end=window.end)
            return FactLoadResult(fact_name, 0, window.start, window.end, None)

        log.info("fact_load.start", fact=fact_name,
                 start=window.start, end=window.end, run_id=run_id)

        params = {
            "window_start": window.start,
            "window_end": window.end,
            "etl_run_id": run_id,
        }
        params.update(_extra_params_for_fact(fact_name, cfg))

        result = run_sql_file(
            conn,
            sql_file,
            params=params,
        )
        rows = result.rowcount or 0

        # Advance watermark to window.end — even if 0 rows landed, we
        # still want the watermark to move so we don't rescan the same
        # empty window forever.
        ws.advance(
            fact_name,
            new_event_time=window.end,
            run_id=run_id,
            rows_loaded=rows,
        )

        log.info("fact_load.done", fact=fact_name, rows=rows,
                 new_watermark=window.end)

        return FactLoadResult(
            fact_name=fact_name,
            rows_loaded=rows,
            window_start=window.start,
            window_end=window.end,
            new_max_event_time=window.end,
        )


def touched_dates_from_windows(
    results: list[FactLoadResult],
) -> list[str]:
    """
    Day-grain version of touched_months_from_windows. Returns ISO date
    strings ('YYYY-MM-DD') for every day overlapped by any non-empty
    fact-load window. Used to drive marts.mart_fleet_daily recompute.
    """
    from datetime import timedelta

    dates: set[str] = set()
    for r in results:
        if r.new_max_event_time is None or r.rows_loaded == 0:
            continue
        cursor = r.window_start.replace(hour=0, minute=0, second=0, microsecond=0).date()
        end = r.window_end.date()
        while cursor <= end:
            dates.add(cursor.isoformat())
            cursor = cursor + timedelta(days=1)
    return sorted(dates)


def touched_months_from_windows(
    results: list[FactLoadResult],
) -> list[str]:
    """
    Derive the set of (year-month) strings that the facts in this run
    touched. The mart loader recomputes only these months.
    """
    months: set[str] = set()
    for r in results:
        if r.new_max_event_time is None or r.rows_loaded == 0:
            continue
        cursor = r.window_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = r.window_end
        while cursor <= end:
            months.add(cursor.strftime("%Y-%m"))
            # Advance one month
            if cursor.month == 12:
                cursor = cursor.replace(year=cursor.year + 1, month=1)
            else:
                cursor = cursor.replace(month=cursor.month + 1)
    return sorted(months)
