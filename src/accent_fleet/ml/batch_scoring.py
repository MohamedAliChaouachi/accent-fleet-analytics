"""
Batch scoring: write per-(tenant, device, year_month) model outputs into
the marts fact tables for a set of touched months.

Two parallel scoring paths share the same shape:

  - ``score_partitions``        — KMeans cluster assignments
                                  → marts.fact_device_cluster_assignment
  - ``score_risk_partitions``   — Isolation Forest risk scores (per tenant)
                                  → marts.fact_device_risk_score

Both are called from the Prefect incremental flow after marts are refreshed.
The dashboard reads pre-computed scores from the fact tables rather than
calling the API per row — keeping page loads fast.

Contract for both:
  - Idempotent: rerunning on the same months replaces those rows.
  - Atomic per-flow-run: delete-then-insert in one transaction so readers
    never see a half-populated partition.
  - No-op (and logs the reason) if no model is available — the flow
    continues even when MLflow / disk artifacts are missing.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sqlalchemy import text

from accent_fleet.db.engine import get_engine, transaction
from accent_fleet.ml.inference import (
    ClusterPredictor,
    RiskPredictor,
    TenantModelMissingError,
)

logger = logging.getLogger("accent_fleet.ml.batch_scoring")


# Singletons — we want to load each model once per process even if the
# scoring entry points are called from multiple Prefect tasks. Both
# predictors are thread-safe.
_CLUSTER_PREDICTOR: ClusterPredictor | None = None
_RISK_PREDICTOR: RiskPredictor | None = None


# Lazily construct (and cache) the process-wide cluster predictor singleton.
def _get_cluster_predictor() -> ClusterPredictor:
    global _CLUSTER_PREDICTOR
    if _CLUSTER_PREDICTOR is None:
        _CLUSTER_PREDICTOR = ClusterPredictor()
    return _CLUSTER_PREDICTOR


# Lazily construct (and cache) the process-wide risk predictor singleton.
def _get_risk_predictor() -> RiskPredictor:
    global _RISK_PREDICTOR
    if _RISK_PREDICTOR is None:
        _RISK_PREDICTOR = RiskPredictor()
    return _RISK_PREDICTOR


@dataclass
class ScoreResult:
    """Outcome of a batch scoring pass — shared between cluster + risk."""
    rows_scored: int
    months: list[str]
    model_version: str
    model_source: str
    skipped_reason: str | None = None
    # Risk-side only: which tenants we have no model for, mapped to the
    # number of rows that were therefore skipped (NOT written to the fact).
    skipped_tenant_rows: dict[int, int] | None = None


# ---------------------------------------------------------------------------
# Shared feature-loader. The risk + cluster paths read the SAME view because
# both train on it; keeping the loader in one place means a column rename
# only needs to be reflected in one query.
# ---------------------------------------------------------------------------
def _load_features(
    months: list[str],
    feature_order: list[str],
    *,
    include_tenant: bool = False,
) -> pd.DataFrame:
    """
    Pull rows from marts.v_ml_features_full for the given months.

    Only the columns we need (ids + features the model was trained on) are
    selected, which keeps the in-memory frame small even on a wide view.
    ``include_tenant`` decides whether the resulting frame contains
    tenant_id — the risk path groups by it; cluster path doesn't need it.
    """
    # Always carry the id columns plus the model's feature columns.
    base_cols = ["tenant_id", "device_id", "year_month", *feature_order]
    if not months:
        return pd.DataFrame(columns=base_cols)

    # Build a parameterised query over only the touched months.
    select_list = ", ".join(base_cols)
    sql = text(
        f"""
        SELECT {select_list}
          FROM marts.v_ml_features_full
         WHERE year_month = ANY(CAST(:months AS text[]))
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params={"months": months})
    logger.info("loaded %d rows for scoring (%d months)", len(df), len(months))
    return df


# ---------------------------------------------------------------------------
# Cluster-assignment fact upsert (unchanged from prior version).
# ---------------------------------------------------------------------------
def _upsert_cluster_rows(rows: Iterable[dict], months: list[str], run_id: int) -> int:
    """
    Replace-then-insert pattern: delete every row for the touched months,
    then insert the freshly-scored rows. Wrapped in a single transaction
    so a partial failure leaves the prior state intact.

    Why delete + insert instead of ON CONFLICT? Three reasons:
      1. Rows whose (tenant, device) disappeared from a month should also
         disappear from the assignment table. ON CONFLICT can't express that.
      2. After a model swap, every row's cluster_id needs to be invalidated
         atomically — delete-then-insert guarantees the dashboard never sees
         a partition with mixed-version cluster ids.
      3. The fact is small (one row per active device-month) so the cost
         of the delete is negligible.
    """
    rows = list(rows)
    with transaction() as conn:
        # Delete the existing partition rows so the insert is a clean replace.
        conn.execute(
            text(
                """
                DELETE FROM marts.fact_device_cluster_assignment
                 WHERE year_month = ANY(CAST(:months AS text[]))
                """
            ),
            {"months": months},
        )
        if not rows:
            return 0
        # Insert the freshly-scored cluster assignments.
        conn.execute(
            text(
                """
                INSERT INTO marts.fact_device_cluster_assignment (
                  tenant_id, device_id, year_month,
                  cluster_id, distance_to_centroid,
                  model_version, model_source, _etl_run_id
                ) VALUES (
                  :tenant_id, :device_id, :year_month,
                  :cluster_id, :distance_to_centroid,
                  :model_version, :model_source, :etl_run_id
                )
                """
            ),
            rows,
        )
    return len(rows)


# ---------------------------------------------------------------------------
def score_partitions(touched_months: list[str], run_id: int) -> ScoreResult:
    """
    Score every device-month in `touched_months` and upsert into
    marts.fact_device_cluster_assignment.

    Returns a ScoreResult describing what happened. Never raises on a missing
    model — instead returns skipped_reason set, so the pipeline keeps going.
    """
    # Nothing to score — short-circuit before touching the DB.
    if not touched_months:
        return ScoreResult(
            rows_scored=0,
            months=[],
            model_version="n/a",
            model_source="n/a",
            skipped_reason="no_touched_months",
        )

    # Load the model; on absence, skip the batch instead of failing the flow.
    predictor = _get_cluster_predictor()
    try:
        predictor.ensure_loaded()
    except RuntimeError as exc:
        logger.warning("batch scoring skipped: %s", exc)
        return ScoreResult(
            rows_scored=0,
            months=list(touched_months),
            model_version="unloaded",
            model_source="none",
            skipped_reason=f"no_model: {exc}",
        )

    # Load feature rows for the touched months in the model's feature order.
    feature_order = predictor.feature_order
    df = _load_features(touched_months, feature_order)
    if df.empty:
        # Still need to clear stale rows for these months — done in upsert.
        _upsert_cluster_rows([], touched_months, run_id)
        return ScoreResult(
            rows_scored=0,
            months=list(touched_months),
            model_version=predictor.model_version,
            model_source=predictor.source,
            skipped_reason="no_feature_rows",
        )

    # Vectorised predict: one transform/predict over the entire frame is
    # ~1000x faster than calling predictor.predict() per row.
    X = df[feature_order].fillna(0).to_numpy(dtype=float)
    scaled = predictor._scaler.transform(X)              # noqa: SLF001
    labels = predictor._kmeans.predict(scaled)           # noqa: SLF001
    all_distances = predictor._kmeans.transform(scaled)  # noqa: SLF001
    # Pick out the distance to the assigned centroid for each row.
    chosen_distances = all_distances[np.arange(len(labels)), labels]

    model_version = predictor.model_version
    model_source = predictor.source

    # Materialise one fact row per device-month from the vectorised outputs.
    rows = [
        {
            "tenant_id": int(r.tenant_id),
            "device_id": int(r.device_id),
            "year_month": str(r.year_month),
            "cluster_id": int(labels[i]),
            "distance_to_centroid": float(chosen_distances[i]),
            "model_version": model_version,
            "model_source": model_source,
            "etl_run_id": run_id,
        }
        for i, r in enumerate(df.itertuples(index=False))
    ]
    written = _upsert_cluster_rows(rows, touched_months, run_id)
    logger.info(
        "scored %d device-months across %d partitions (model=%s)",
        written, len(touched_months), model_version,
    )
    return ScoreResult(
        rows_scored=written,
        months=list(touched_months),
        model_version=model_version,
        model_source=model_source,
    )


# ---------------------------------------------------------------------------
# Risk-score fact upsert
# ---------------------------------------------------------------------------
def _upsert_risk_rows(rows: Iterable[dict], months: list[str], run_id: int) -> int:
    """
    Same replace-then-insert semantics as ``_upsert_cluster_rows`` but for
    ``marts.fact_device_risk_score``. See that docstring for the rationale.

    The risk variant also clears rows for tenants that the new model can't
    score anymore (because the tenant fell below ``min_rows_per_tenant``
    in this training window) — the row count returned reflects the
    INSERT side only, not the delete.
    """
    rows = list(rows)
    with transaction() as conn:
        # Delete the existing partition rows so the insert is a clean replace.
        conn.execute(
            text(
                """
                DELETE FROM marts.fact_device_risk_score
                 WHERE year_month = ANY(CAST(:months AS text[]))
                """
            ),
            {"months": months},
        )
        if not rows:
            return 0
        # Insert the freshly-scored risk rows.
        conn.execute(
            text(
                """
                INSERT INTO marts.fact_device_risk_score (
                  tenant_id, device_id, year_month,
                  risk_score, risk_category,
                  model_version, model_source, _etl_run_id
                ) VALUES (
                  :tenant_id, :device_id, :year_month,
                  :risk_score, :risk_category,
                  :model_version, :model_source, :etl_run_id
                )
                """
            ),
            rows,
        )
    return len(rows)


# ---------------------------------------------------------------------------
def score_risk_partitions(touched_months: list[str], run_id: int) -> ScoreResult:
    """
    Score every device-month in ``touched_months`` with the per-tenant
    Isolation Forest and upsert into ``marts.fact_device_risk_score``.

    Per-tenant model semantics: rows for a tenant that has no model in the
    loaded artifact are dropped silently (counted in ``skipped_tenant_rows``)
    rather than aborting the batch. The mart for that tenant simply won't
    have current rows until the next retrain includes them — same posture
    as the API's 503 behaviour.

    Returns a ScoreResult describing what happened. Never raises on a
    missing or unloadable risk artifact; the flow keeps moving and surfaces
    the reason in logs / metrics.
    """
    # Nothing to score — short-circuit before touching the DB.
    if not touched_months:
        return ScoreResult(
            rows_scored=0,
            months=[],
            model_version="n/a",
            model_source="n/a",
            skipped_reason="no_touched_months",
        )

    # Load the model; on absence, skip the batch instead of failing the flow.
    predictor = _get_risk_predictor()
    try:
        predictor.ensure_loaded()
    except RuntimeError as exc:
        logger.warning("risk batch scoring skipped: %s", exc)
        return ScoreResult(
            rows_scored=0,
            months=list(touched_months),
            model_version="unloaded",
            model_source="none",
            skipped_reason=f"no_model: {exc}",
        )

    # Load feature rows (with tenant_id, since we score per tenant).
    feature_order = predictor.feature_order
    df = _load_features(touched_months, feature_order, include_tenant=True)
    if df.empty:
        _upsert_risk_rows([], touched_months, run_id)
        return ScoreResult(
            rows_scored=0,
            months=list(touched_months),
            model_version=predictor.model_version,
            model_source=predictor.source,
            skipped_reason="no_feature_rows",
            skipped_tenant_rows={},
        )

    model_version = predictor.model_version
    model_source = predictor.source

    rows: list[dict] = []
    skipped_tenant_rows: dict[int, int] = {}

    # Group by tenant_id so each per-tenant Isolation Forest gets one
    # vectorised forward pass. We process the groups in deterministic
    # sorted order so logs / metrics are stable across runs.
    for tenant_id, sub in df.groupby("tenant_id", sort=True):
        tid = int(tenant_id)
        try:
            scores, labels = predictor.predict_batch(
                tenant_id=tid,
                features_df=sub[list(feature_order)],
            )
        except TenantModelMissingError:
            skipped_tenant_rows[tid] = len(sub)
            logger.info(
                "risk scoring: skipping tenant %d (%d rows) — no per-tenant model",
                tid, len(sub),
            )
            continue

        rows.extend(
            {
                "tenant_id": tid,
                "device_id": int(r.device_id),
                "year_month": str(r.year_month),
                "risk_score": float(scores[i]),
                "risk_category": str(labels[i]),
                "model_version": model_version,
                "model_source": model_source,
                "etl_run_id": run_id,
            }
            for i, r in enumerate(sub.itertuples(index=False))
        )

    # Replace the touched partitions with the scored rows.
    written = _upsert_risk_rows(rows, touched_months, run_id)
    logger.info(
        "risk scored %d device-months across %d partitions (model=%s, "
        "skipped_tenants=%s)",
        written, len(touched_months), model_version, skipped_tenant_rows,
    )
    return ScoreResult(
        rows_scored=written,
        months=list(touched_months),
        model_version=model_version,
        model_source=model_source,
        skipped_tenant_rows=skipped_tenant_rows,
    )
