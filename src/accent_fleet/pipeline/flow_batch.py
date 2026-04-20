"""
Batch pipeline flows.

Three Prefect flows:

  - bootstrap_flow   : one-time DDL + static dimensions.
  - backfill_flow    : one-time historical load of staging -> warehouse -> marts.
  - incremental_flow : every 5 min, process the new event-time window only.

All three share the same task vocabulary (refresh_dimensions,
load_fact_incremental, recompute_mart). Only the windowing differs.

Why Prefect? Two reasons:
  1. Task-level retries (flaky DB connection shouldn't fail the whole flow).
  2. Scheduling + observability come free — we just register the flow with
     a 5-min interval and Prefect handles triggering and UI.

The flows can also be run as plain functions (see scripts/run_batch.py),
so nothing forces you to adopt Prefect.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import structlog
from prefect import flow, task
from sqlalchemy import text

from accent_fleet.config import load_pipeline_config
from accent_fleet.db import get_engine, run_sql_file, transaction
from accent_fleet.db.sql_loader import split_sql_statements, load_sql
from accent_fleet.monitoring import run_validation_suite
from accent_fleet.pipeline.run_log import begin_run, end_run
from accent_fleet.transforms import refresh_all_dimensions
from accent_fleet.transforms.facts import (
    FACT_SQL,
    FactLoadResult,
    load_fact_incremental,
    touched_months_from_windows,
)

log = structlog.get_logger()


# =============================================================================
# Shared tasks
# =============================================================================

@task(retries=2, retry_delay_seconds=15)
def task_bootstrap_schemas() -> None:
    """Create schemas + state tables. Idempotent."""
    sql = load_sql("00_schemas_and_state.sql")
    with transaction() as conn:
        for stmt in split_sql_statements(sql):
            conn.execute(text(stmt))
    log.info("bootstrap.schemas_ready")


@task(retries=2, retry_delay_seconds=15)
def task_refresh_dimensions() -> None:
    refresh_all_dimensions()


@task(retries=2, retry_delay_seconds=15)
def task_load_fact(fact_name: str, run_id: int,
                   window_end: datetime | None = None) -> FactLoadResult:
    return load_fact_incremental(fact_name, run_id=run_id, window_end=window_end)


@task(retries=1)
def task_recompute_mart(touched_months: list[str], run_id: int) -> int:
    """Recompute mart rows for the given year-months only."""
    if not touched_months:
        log.info("mart.skip_no_touched_months")
        return 0
    with transaction() as conn:
        result = run_sql_file(
            conn,
            "20_mart_device_monthly_behavior.sql",
            params={"touched_months": touched_months, "etl_run_id": run_id},
        )
        rows = result.rowcount or 0
        log.info("mart.recomputed", months=touched_months, rows=rows)
        return rows


@task(retries=1)
def task_ensure_views() -> None:
    """(Re)create the marts views. Idempotent."""
    with transaction() as conn:
        for f in ("21_v_device_risk_profile.sql",
                  "22_v_ml_features.sql",
                  "23_v_fleet_risk_dashboard.sql"):
            run_sql_file(conn, f)


@task
def task_run_validation(run_id: int) -> None:   # noqa: ARG001
    report = run_validation_suite()
    if report.all_passed:
        log.info("validation.all_passed", n=len(report.checks))
    else:
        log.warning("validation.failures", failed=report.failed_checks)


# =============================================================================
# Flow: bootstrap
# =============================================================================

@flow(name="accent-bootstrap")
def bootstrap_flow() -> None:
    """One-time bootstrap. Safe to re-run."""
    run_id = begin_run(mode="bootstrap")
    try:
        task_bootstrap_schemas()
        task_refresh_dimensions()
        task_ensure_views()
        end_run(run_id, status="success")
    except Exception as exc:
        end_run(run_id, status="failed", error_message=str(exc))
        raise


# =============================================================================
# Flow: incremental
# =============================================================================

@flow(name="accent-incremental")
def incremental_flow(window_end: datetime | None = None) -> None:
    """
    Normal operating mode. Processes everything that arrived since the
    last watermark (minus overlap) up to `window_end`.
    """
    window_end = window_end or datetime.utcnow()
    run_id = begin_run(mode="incremental", window_end=window_end)

    try:
        # 1. Refresh dims (cheap; handles new devices/vehicles)
        task_refresh_dimensions()

        # 2. Load facts in priority order (defined in pipeline.yaml)
        cfg = load_pipeline_config()
        fact_results: list[FactLoadResult] = []
        for src in sorted(cfg["sources"], key=lambda s: s["priority"]):
            fact_name = src["target_fact"]
            if fact_name not in FACT_SQL:
                continue
            fact_results.append(task_load_fact(fact_name, run_id, window_end))

        # 3. Recompute mart only for months that received new fact rows
        touched = touched_months_from_windows(fact_results)
        task_recompute_mart(touched, run_id)

        # 4. Views are views — they don't need refreshing, but we keep
        #    the task in case we switch to materialised views later.

        # 5. Validation
        task_run_validation(run_id)

        total_loaded = sum(r.rows_loaded for r in fact_results)
        end_run(run_id, status="success", rows_loaded=total_loaded)
        log.info("incremental.done", rows_loaded=total_loaded,
                 facts=len(fact_results), touched_months=touched)
    except Exception as exc:
        end_run(run_id, status="failed", error_message=str(exc))
        raise


# =============================================================================
# Flow: backfill
# =============================================================================

@flow(name="accent-backfill")
def backfill_flow(chunk_days: int | None = None) -> None:
    """
    One-shot historical load.

    We slice [min_event_time, now) into chunks of `chunk_days` days and
    run `incremental_flow` for each chunk. Because the incremental flow is
    watermark-driven, this is equivalent to "advance the watermark chunk
    by chunk". Crashes are resumable: re-running backfill picks up where
    the watermark left off.
    """
    cfg = load_pipeline_config()
    chunk_days = chunk_days or cfg["window"]["backfill_chunk_days"]
    min_time = datetime.fromisoformat(
        cfg["window"]["min_event_time"].replace("Z", "+00:00")
    ).replace(tzinfo=None)

    # Find "now" or the latest staging event-time, whichever is smaller
    with get_engine().connect() as conn:
        latest = conn.execute(
            text("SELECT MAX(begin_path_time) FROM staging.path")
        ).scalar_one_or_none()
    end = latest or datetime.utcnow()

    log.info("backfill.plan", start=min_time, end=end, chunk_days=chunk_days)

    # Seed the watermark at min_time so the first chunk's window starts there.
    # (The incremental flow's max_age_cap handles the "never loaded" case.)
    cursor = min_time
    while cursor < end:
        next_cursor = min(cursor + timedelta(days=chunk_days), end)
        log.info("backfill.chunk", start=cursor, end=next_cursor)
        incremental_flow(window_end=next_cursor)
        cursor = next_cursor

    log.info("backfill.done")
