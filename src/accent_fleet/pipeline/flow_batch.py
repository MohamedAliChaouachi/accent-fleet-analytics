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
from accent_fleet.db.sql_loader import load_sql, split_sql_statements
from accent_fleet.monitoring import run_validation_suite
from accent_fleet.pipeline.run_log import begin_run, end_run
from accent_fleet.transforms import refresh_all_dimensions
from accent_fleet.transforms.facts import (
    FACT_SQL,
    FactLoadResult,
    load_fact_incremental,
    touched_dates_from_windows,
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
def task_recompute_telemetry_mart(touched_months: list[str], run_id: int) -> int:
    """
    Recompute marts.mart_device_monthly_telemetry for given year-months.

    This is the archive-side companion to mart_device_monthly_behavior. It
    aggregates fact_harsh_event + fact_telemetry_daily into the per-month
    grain expected by the ML feature view (sql/26).
    """
    if not touched_months:
        log.info("telemetry_mart.skip_no_touched_months")
        return 0
    with transaction() as conn:
        result = run_sql_file(
            conn,
            "25_mart_device_monthly_telemetry.sql",
            params={"touched_months": touched_months, "etl_run_id": run_id},
        )
        rows = result.rowcount or 0
        log.info("telemetry_mart.recomputed", months=touched_months, rows=rows)
        return rows


@task(retries=1)
def task_recompute_fleet_daily(touched_dates: list[str], run_id: int) -> int:
    """Recompute marts.mart_fleet_daily for given ISO dates only."""
    if not touched_dates:
        log.info("fleet_daily.skip_no_touched_dates")
        return 0
    with transaction() as conn:
        result = run_sql_file(
            conn,
            "30_mart_fleet_daily.sql",
            params={"touched_dates": touched_dates, "etl_run_id": run_id},
        )
        rows = result.rowcount or 0
        log.info("fleet_daily.recomputed", dates=len(touched_dates), rows=rows)
        return rows


@task(retries=1)
def task_recompute_vehicle_monthly(touched_months: list[str], run_id: int) -> int:
    """Recompute marts.mart_vehicle_monthly for given year-months only."""
    if not touched_months:
        log.info("vehicle_monthly.skip_no_touched_months")
        return 0
    with transaction() as conn:
        result = run_sql_file(
            conn,
            "31_mart_vehicle_monthly.sql",
            params={"touched_months": touched_months, "etl_run_id": run_id},
        )
        rows = result.rowcount or 0
        log.info("vehicle_monthly.recomputed", months=touched_months, rows=rows)
        return rows


@task(retries=1)
def task_recompute_tenant_summary(touched_months: list[str], run_id: int) -> int:
    """Recompute marts.mart_tenant_monthly_summary for given year-months."""
    if not touched_months:
        log.info("tenant_summary.skip_no_touched_months")
        return 0
    with transaction() as conn:
        result = run_sql_file(
            conn,
            "32_mart_tenant_monthly_summary.sql",
            params={"touched_months": touched_months, "etl_run_id": run_id},
        )
        rows = result.rowcount or 0
        log.info("tenant_summary.recomputed", months=touched_months, rows=rows)
        return rows


@task(retries=1)
def task_refresh_device_risk_profile(touched_months: list[str], run_id: int) -> int:
    """
    Refresh marts.fact_device_risk_profile from marts.v_device_risk_profile.

    This is the materialized snapshot the /devices/{id}/profile endpoint reads
    from. Querying the view directly costs ~500ms p95 because its ROW_NUMBER()
    window can't be pushed below the WHERE clause; the snapshotted fact table
    has a per-device index → microseconds.

    Gated on touched_months so it only runs when device-month behavior actually
    changed. Safe to skip otherwise: the snapshot is a deterministic function
    of the source view, so if no source rows moved, the snapshot is still
    correct.
    """
    if not touched_months:
        log.info("device_risk_profile.skip_no_touched_months")
        return 0
    with transaction() as conn:
        conn.execute(text("CALL marts.refresh_fact_device_risk_profile()"))
        # The procedure does TRUNCATE + INSERT; rowcount is not meaningful
        # across both, so report the post-state cardinality instead.
        rows = conn.execute(
            text("SELECT COUNT(*) FROM marts.fact_device_risk_profile")
        ).scalar_one()
        log.info("device_risk_profile.refreshed", rows=rows, months=touched_months)
        return rows


@task(retries=1)
def task_detect_drift(touched_months: list[str], run_id: int) -> int:   # noqa: ARG001
    """
    Compute per-feature PSI between the scored months and a recent reference
    window. Updates the Prometheus `accent_ml_feature_drift_score` gauge and
    logs a warning when any feature crosses the alert threshold.

    Returns the count of features that drifted (0 = healthy). Never raises
    — drift is informational, not fatal: a transient anomaly shouldn't
    cancel the rest of the flow.

    Lazy import of the drift module mirrors task_score_latest_partition:
    keeps numpy/pandas off the bootstrap import path for environments
    that haven't installed the ML extras.
    """
    if not touched_months:
        log.info("drift.skip_no_touched_months")
        return 0
    try:
        from accent_fleet.ml.drift import detect_drift_for_months
        from accent_fleet.observability import ml_feature_drift_score
    except ImportError as exc:
        log.warning("drift.skipped_import_error", error=str(exc))
        return 0

    try:
        report = detect_drift_for_months(touched_months)
    except Exception as exc:  # noqa: BLE001 — drift should never fail the flow
        log.warning("drift.compute_failed", error=str(exc))
        return 0

    # Publish per-feature PSI to Prometheus so alerts can fire from the
    # metrics path even if the log line goes to a black hole.
    for f in report.features:
        ml_feature_drift_score.labels(feature=f.feature).set(f.psi)

    if report.any_drifted:
        log.warning(
            "drift.alert",
            drifted_features=report.drifted_features,
            threshold=report.threshold,
            n_current_rows=report.n_current_rows,
            n_reference_rows=report.n_reference_rows,
        )
    else:
        log.info(
            "drift.healthy",
            n_features=len(report.features),
            n_current_rows=report.n_current_rows,
            n_reference_rows=report.n_reference_rows,
        )
    return len(report.drifted_features)


@task(retries=1)
def task_score_latest_partition(touched_months: list[str], run_id: int) -> int:
    """
    Run the clustering model over the touched months and write
    fact_device_cluster_assignment rows. Never fails the flow: a missing
    model is logged and skipped so the rest of the pipeline keeps running.

    Imported lazily because batch_scoring pulls in sklearn/mlflow — keep
    those out of the bootstrap path for environments that haven't installed
    the ML extras yet.
    """
    if not touched_months:
        log.info("cluster_scoring.skip_no_touched_months")
        return 0
    from accent_fleet.ml.batch_scoring import score_partitions

    result = score_partitions(touched_months, run_id)
    if result.skipped_reason:
        log.warning(
            "cluster_scoring.skipped",
            reason=result.skipped_reason,
            months=touched_months,
        )
    else:
        log.info(
            "cluster_scoring.done",
            rows=result.rows_scored,
            months=touched_months,
            model_version=result.model_version,
        )
    return result.rows_scored


@task(retries=1)
def task_ensure_views() -> None:
    """(Re)create the marts views. Idempotent."""
    with transaction() as conn:
        for f in ("21_v_device_risk_profile.sql",
                  "22_v_ml_features.sql",
                  "23_v_fleet_risk_dashboard.sql",
                  "26_v_ml_features_full.sql",
                  # BI dashboard views
                  "33_v_executive_dashboard.sql",
                  "34_v_operational_dashboard.sql",
                  "35_v_maintenance_dashboard.sql"):
            run_sql_file(conn, f)


@task(retries=1)
def task_ensure_mart_structure() -> None:
    """Create mart table/index DDL needed by views without recomputing data."""
    def _first_sql_token(statement: str) -> str:
        for line in statement.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            return stripped.upper()
        return ""

    with transaction() as conn:
        for sql_file in (
            "20_mart_device_monthly_behavior.sql",
            "25_mart_device_monthly_telemetry.sql",
            # ML output table (DDL only — populated by batch_scoring.py)
            "27_fact_device_cluster_assignment.sql",
            # BI marts (DDL only — recompute happens later)
            "30_mart_fleet_daily.sql",
            "31_mart_vehicle_monthly.sql",
            "32_mart_tenant_monthly_summary.sql",
        ):
            sql = load_sql(sql_file)
            for stmt in split_sql_statements(sql):
                head = _first_sql_token(stmt)
                if head.startswith("WITH") or head.startswith("INSERT INTO"):
                    break
                conn.exec_driver_sql(stmt)

        # 41_fact_device_risk_profile.sql is pure DDL (table + indexes +
        # CREATE OR REPLACE PROCEDURE) — no INSERT tail to guard against,
        # so apply it whole. The procedure body lives in a $$...$$ block
        # which split_sql_statements respects.
        sql = load_sql("41_fact_device_risk_profile.sql")
        for stmt in split_sql_statements(sql):
            conn.exec_driver_sql(stmt)
    log.info("mart.structure_ready")


@task
def task_run_validation(run_id: int) -> None:   # noqa: ARG001
    report = run_validation_suite()
    if report.all_passed:
        log.info("validation.all_passed", n=len(report.checks))
    else:
        log.warning("validation.failures", failed=report.failed_checks)


def _months_ago_yyyy_mm(now: datetime, months: int) -> str:
    """
    Return the YYYY-MM cutoff that's `months` calendar months before `now`.

    Computed via integer month arithmetic to avoid pulling python-dateutil
    for a single subtraction. Always produces a string sortable against
    CHAR(7) year_month columns.
    """
    idx = now.year * 12 + (now.month - 1) - months
    year, month0 = divmod(idx, 12)
    return f"{year:04d}-{month0 + 1:02d}"


@task(retries=1)
def task_apply_retention(run_id: int) -> None:   # noqa: ARG001
    """
    Prune unbounded operational tables per config/pipeline.yaml > retention.

    Disabled by default-friendly fallback: if the section is missing or
    `enabled: false`, this task is a no-op so existing deployments behave
    exactly as before this commit landed.

    Why a separate task (vs. a cron job): keeping it inline means the
    retention window is enforced on every successful flow, which is the
    same cadence that fills these tables. No drift, no missed days.
    """
    cfg = load_pipeline_config()
    ret = cfg.get("retention") or {}
    if not ret.get("enabled", False):
        log.info("retention.skip_disabled")
        return

    etl_days = int(ret.get("etl_run_log_days", 90))
    quarantine_days = int(ret.get("quarantine_days", 30))
    cluster_months = int(ret.get("cluster_assignment_months", 12))
    cutoff_month = _months_ago_yyyy_mm(datetime.utcnow(), cluster_months)

    with transaction() as conn:
        run_sql_file(
            conn,
            "40_retention.sql",
            params={
                "etl_run_log_retention_days": etl_days,
                "quarantine_retention_days": quarantine_days,
                "cluster_assignment_cutoff_month": cutoff_month,
            },
        )
    log.info(
        "retention.applied",
        etl_run_log_days=etl_days,
        quarantine_days=quarantine_days,
        cluster_assignment_cutoff_month=cutoff_month,
    )


# =============================================================================
# Flow: bootstrap
# =============================================================================

@flow(name="accent-bootstrap")
def bootstrap_flow() -> None:
    """One-time bootstrap. Safe to re-run."""
    task_bootstrap_schemas()
    run_id = begin_run(mode="bootstrap")
    try:
        task_refresh_dimensions()
        task_ensure_mart_structure()
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

        # 3. Recompute marts only for periods that received new fact rows.
        #
        #    ML marts (month-grain, device-grain):
        #      - mart_device_monthly_behavior  (trip-side)
        #      - mart_device_monthly_telemetry (archive-side)
        #
        #    BI marts:
        #      - mart_fleet_daily              (day-grain, tenant-grain)
        #      - mart_vehicle_monthly          (month-grain, vehicle-grain)
        #      - mart_tenant_monthly_summary   (month-grain, tenant-grain;
        #                                       depends on the two above)
        touched = touched_months_from_windows(fact_results)
        touched_dates = touched_dates_from_windows(fact_results)
        task_recompute_mart(touched, run_id)
        task_recompute_telemetry_mart(touched, run_id)
        task_recompute_fleet_daily(touched_dates, run_id)
        task_recompute_vehicle_monthly(touched, run_id)
        # tenant_summary rolls up the two BI marts above — must run last.
        task_recompute_tenant_summary(touched, run_id)

        # 3b. Refresh the materialized risk-profile fact AFTER
        #     mart_device_monthly_behavior — it reads from that mart through
        #     v_device_risk_profile. Cheap (TRUNCATE + INSERT of a small,
        #     gate-filtered snapshot) so we re-do it whenever any month moved.
        task_refresh_device_risk_profile(touched, run_id)

        # 4. Score touched partitions against the clustering model and
        #    persist cluster assignments. Runs AFTER all marts because the
        #    scorer reads from marts.v_ml_features_full. If no model is
        #    registered yet this is a no-op (logs and continues).
        task_score_latest_partition(touched, run_id)

        # 4b. Drift check — compares the just-scored months against a
        #     6-month reference window. Informational only: a warning is
        #     logged and a Prometheus gauge updated, but the flow continues
        #     regardless of the result. Drift triggering retraining is a
        #     human decision in v0.7.
        task_detect_drift(touched, run_id)

        # 5. Views are views — they don't need refreshing, but we keep
        #    the task in case we switch to materialised views later.

        # 6. Validation
        task_run_validation(run_id)

        # 7. Bounded retention — prune old etl_run_log / quarantine /
        #    cluster_assignment rows per config/pipeline.yaml. Runs LAST
        #    so even if it fails, the run that produced this batch's data
        #    is already logged as success in step 5.
        task_apply_retention(run_id)

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
    chunk_days = int(chunk_days or cfg["window"]["backfill_chunk_days"])
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
