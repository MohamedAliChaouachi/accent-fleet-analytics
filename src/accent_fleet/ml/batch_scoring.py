"""
Batch scoring: write cluster assignments for every (tenant, device, year_month)
in a set of touched months into marts.fact_device_cluster_assignment.

Called from the Prefect incremental flow after marts are refreshed. The
dashboard reads pre-computed cluster_ids from this fact table rather than
calling the API per row — keeping page loads fast.

Contract:
  - score_partitions(touched_months, run_id) -> ScoreResult
  - Idempotent: rerunning on the same months replaces those rows.
  - No-op (and logs the reason) if no clustering model is available.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from sqlalchemy import text

from accent_fleet.db.engine import get_engine, transaction
from accent_fleet.ml.inference import ClusterPredictor

logger = logging.getLogger("accent_fleet.ml.batch_scoring")


# Singleton — we want to load the model once per process even if score_partitions
# is called from multiple tasks. The ClusterPredictor itself is thread-safe.
_PREDICTOR: ClusterPredictor | None = None


def _get_predictor() -> ClusterPredictor:
    global _PREDICTOR
    if _PREDICTOR is None:
        _PREDICTOR = ClusterPredictor()
    return _PREDICTOR


@dataclass
class ScoreResult:
    rows_scored: int
    months: list[str]
    model_version: str
    model_source: str
    skipped_reason: str | None = None


# ---------------------------------------------------------------------------
def _load_features(months: list[str], feature_order: list[str]) -> pd.DataFrame:
    """
    Pull rows from marts.v_ml_features_full for the given months.

    Only the columns we need (ids + features the model was trained on) are
    selected, which keeps the in-memory frame small even on a wide view.
    """
    if not months:
        return pd.DataFrame(columns=["tenant_id", "device_id", "year_month", *feature_order])

    cols = ["tenant_id", "device_id", "year_month", *feature_order]
    select_list = ", ".join(cols)
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
def _upsert_rows(rows: Iterable[dict], months: list[str], run_id: int) -> int:
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
    if not touched_months:
        return ScoreResult(
            rows_scored=0,
            months=[],
            model_version="n/a",
            model_source="n/a",
            skipped_reason="no_touched_months",
        )

    predictor = _get_predictor()
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

    feature_order = predictor.feature_order
    df = _load_features(touched_months, feature_order)
    if df.empty:
        # Still need to clear stale rows for these months — done in upsert.
        _upsert_rows([], touched_months, run_id)
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
    written = _upsert_rows(rows, touched_months, run_id)
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
