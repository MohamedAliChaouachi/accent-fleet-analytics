"""
Train + persist + register the device-behavior clustering model.

Lifted from notebooks/04_modeling/01_device_behavior_clustering.ipynb but
restructured for reproducibility:
  - random_state pinned everywhere (k-selection, KMeans, PCA)
  - one model fitted across all tenants (not per-tenant) for the v1 API;
    per-tenant variants can be added later as separate registered models
  - artifacts written both to MLflow (preferred) and models/clustering/
    on disk (fallback for environments without an MLflow server)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from accent_fleet.config import PROJECT_ROOT, settings
from accent_fleet.db.engine import get_engine

logger = logging.getLogger("accent_fleet.ml.train_clustering")

# ---------------------------------------------------------------------------
# Feature set — keep in sync with the notebook + config/feature_definitions.yaml.
# These are the columns we feed the KMeans; missing values are filled with 0
# (matches the SQL view's COALESCE semantics).
# ---------------------------------------------------------------------------
FEATURES: tuple[str, ...] = (
    "overspeed_per_100km",
    "avg_speed_over_limit",
    "high_speed_trip_ratio",
    "speed_alert_per_100km",
    "harsh_brake_per_100km",
    "harsh_accel_per_100km",
    "harsh_corner_per_100km",
    "monthly_idle_ratio",
    "high_rpm_minutes_per_day",
    "night_trip_ratio",
    "rush_hour_trip_ratio",
    "stddev_trip_distance",
    "short_trip_ratio",
)

ID_COLS: tuple[str, ...] = ("tenant_id", "device_id", "year_month")

RANDOM_STATE = 42
DEFAULT_K_RANGE = range(3, 7)
MIN_ROWS = 50
ARTIFACT_DIR = PROJECT_ROOT / "models" / "clustering"


@dataclass
class TrainResult:
    k: int
    silhouette: float
    n_rows: int
    feature_order: list[str]
    cluster_sizes: dict[int, int] = field(default_factory=dict)
    training_window: str = ""


# ---------------------------------------------------------------------------
def load_training_frame(month_from: str = "2025-01") -> pd.DataFrame:
    """Pull device-month rows with enough activity to be worth clustering."""
    sql = text(
        """
        SELECT *
          FROM marts.v_ml_features_full
         WHERE year_month >= :month_from
           AND total_distance_km >= 100
           AND total_ignition_on_minutes > 0
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params={"month_from": month_from})
    logger.info("loaded %d rows for clustering (window >= %s)", len(df), month_from)
    return df


# ---------------------------------------------------------------------------
def fit_clustering(
    df: pd.DataFrame,
    k_range: range = DEFAULT_K_RANGE,
) -> tuple[KMeans, StandardScaler, TrainResult]:
    """Pick k by silhouette over the configured range; return fitted pipeline."""
    if len(df) < MIN_ROWS:
        raise ValueError(f"need at least {MIN_ROWS} rows, got {len(df)}")

    X = df[list(FEATURES)].fillna(0).to_numpy(dtype=float)
    scaler = StandardScaler().fit(X)
    Xs = scaler.transform(X)

    best_km: KMeans | None = None
    best_sil = -1.0
    best_k = -1
    for k in k_range:
        if k >= len(Xs):
            continue
        km = KMeans(n_clusters=k, n_init=10, random_state=RANDOM_STATE).fit(Xs)
        sil = silhouette_score(Xs, km.labels_)
        logger.info("k=%d silhouette=%.4f", k, sil)
        if sil > best_sil:
            best_sil = sil
            best_km = km
            best_k = k

    if best_km is None:
        raise RuntimeError("KMeans selection produced no model")

    labels, counts = np.unique(best_km.labels_, return_counts=True)
    sizes = {int(lab): int(c) for lab, c in zip(labels, counts)}

    result = TrainResult(
        k=best_k,
        silhouette=float(best_sil),
        n_rows=len(df),
        feature_order=list(FEATURES),
        cluster_sizes=sizes,
    )
    return best_km, scaler, result


# ---------------------------------------------------------------------------
def save_local(kmeans: KMeans, scaler: StandardScaler, result: TrainResult) -> None:
    """Write joblib + metadata under models/clustering/."""
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(kmeans, ARTIFACT_DIR / "kmeans_v1.joblib")
    joblib.dump(scaler, ARTIFACT_DIR / "scaler_v1.joblib")
    metadata: dict[str, Any] = {
        "version": "v1",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "k": result.k,
        "silhouette": result.silhouette,
        "n_rows": result.n_rows,
        "feature_order": result.feature_order,
        "cluster_sizes": result.cluster_sizes,
        "random_state": RANDOM_STATE,
        "training_window": result.training_window,
    }
    (ARTIFACT_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2))
    logger.info("wrote artifacts to %s", ARTIFACT_DIR)


# ---------------------------------------------------------------------------
def log_to_mlflow(
    kmeans: KMeans,
    scaler: StandardScaler,
    result: TrainResult,
    promote: bool = True,
) -> str | None:
    """Log run + register model. Returns the new model version, or None on error."""
    try:
        import mlflow
        import mlflow.sklearn
        from mlflow.tracking import MlflowClient
    except ImportError:
        logger.warning("mlflow not installed; skipping registry log")
        return None

    s = settings()
    mlflow.set_tracking_uri(s.mlflow_tracking_uri)
    mlflow.set_experiment(s.mlflow_experiment_name)

    artifact = {
        "kmeans": kmeans,
        "scaler": scaler,
        "feature_order": result.feature_order,
    }

    with mlflow.start_run() as run:
        mlflow.log_params({
            "k": result.k,
            "random_state": RANDOM_STATE,
            "n_features": len(result.feature_order),
            "n_rows": result.n_rows,
        })
        mlflow.log_metric("silhouette", result.silhouette)
        for cid, size in result.cluster_sizes.items():
            mlflow.log_metric(f"cluster_{cid}_size", size)
        mlflow.log_dict(
            {"feature_order": result.feature_order, "cluster_sizes": result.cluster_sizes},
            "metadata.json",
        )
        mlflow.sklearn.log_model(
            artifact,  # type: ignore[arg-type]
            artifact_path="model",
            registered_model_name=s.mlflow_model_name,
        )
        run_id = run.info.run_id

    # Promote to the configured stage so the API picks it up on next reload.
    if promote:
        client = MlflowClient()
        versions = client.search_model_versions(f"name='{s.mlflow_model_name}'")
        latest = max(versions, key=lambda v: int(v.version))
        client.transition_model_version_stage(
            name=s.mlflow_model_name,
            version=latest.version,
            stage=s.mlflow_model_stage,
            archive_existing_versions=True,
        )
        logger.info(
            "registered %s v%s -> %s (run=%s)",
            s.mlflow_model_name, latest.version, s.mlflow_model_stage, run_id,
        )
        return latest.version

    return None


# ---------------------------------------------------------------------------
def run(month_from: str = "2025-01", promote: bool = True) -> TrainResult:
    """End-to-end training. Used by scripts/train_clustering.py and tests."""
    logging.basicConfig(
        level=os.environ.get("PIPELINE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    df = load_training_frame(month_from=month_from)
    kmeans, scaler, result = fit_clustering(df)
    result.training_window = f">= {month_from}"
    save_local(kmeans, scaler, result)
    log_to_mlflow(kmeans, scaler, result, promote=promote)
    return result
