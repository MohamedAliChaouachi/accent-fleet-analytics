"""
Inference layer for the FastAPI service.

Two scorers are exposed:

  - RiskScorer (re-exported from features/risk_score.py) — pure-Python
    composite scorer that mirrors sql/21_v_device_risk_profile.sql.
    Wrapped here with a `score_with_components()` helper so the API can
    return per-factor contributions.

  - ClusterPredictor — loads the persisted KMeans + StandardScaler from
    MLflow (preferred) or from `models/clustering/` on disk (fallback).
    Loaded lazily so the API can boot even before a model is registered.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

from accent_fleet.config import PROJECT_ROOT, config_hash, settings
from accent_fleet.features.risk_score import RiskScorer, load_risk_scorer

logger = logging.getLogger("accent_fleet.ml.inference")


# ---------------------------------------------------------------------------
# Risk scoring helpers
# ---------------------------------------------------------------------------
def get_risk_scorer() -> RiskScorer:
    """Return the singleton RiskScorer loaded from feature_definitions.yaml."""
    return load_risk_scorer()


def score_with_components(
    scorer: RiskScorer, features: dict[str, Any]
) -> tuple[float, dict[str, float]]:
    """
    Compute the composite score AND the per-factor weighted contributions.

    Useful for the API/dashboard: the total alone hides which factor pushed
    a device into the critical band.
    """
    components: dict[str, float] = {}
    total = 0.0
    for f in scorer.factors:
        raw = scorer._raw_value(f, features)          # noqa: SLF001 — intentional reuse
        normalized = scorer._normalize(f, raw)        # noqa: SLF001
        contribution = f.weight * normalized
        components[f.name] = round(contribution * 100, 2)
        total += contribution
    return round(total * 100, 1), components


# ---------------------------------------------------------------------------
# Clustering predictor
# ---------------------------------------------------------------------------
DEFAULT_MODEL_DIR = PROJECT_ROOT / "models" / "clustering"


class ClusterPredictor:
    """
    Loads a fitted KMeans + StandardScaler and predicts cluster ids.

    Loading strategy (in order):
      1. MLflow registry — models:/<name>/<stage>
      2. Local joblib files under models/clustering/

    The first .predict() call triggers loading. If both sources fail the
    predictor stays in 'unloaded' state and raises on use, so /health stays
    green and the API can be deployed before the first training run.
    """

    def __init__(self) -> None:
        self._kmeans: Any = None
        self._scaler: Any = None
        self._feature_order: list[str] | None = None
        self._model_version: str = "unloaded"
        self._source: str = "none"
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    @property
    def is_loaded(self) -> bool:
        return self._kmeans is not None and self._scaler is not None

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def source(self) -> str:
        return self._source

    @property
    def feature_order(self) -> list[str]:
        if self._feature_order is None:
            raise RuntimeError("ClusterPredictor not loaded — no feature order.")
        return list(self._feature_order)

    # ------------------------------------------------------------------
    def ensure_loaded(self) -> None:
        """Idempotent load. Tries MLflow first, then local disk."""
        if self.is_loaded:
            return
        with self._lock:
            if self.is_loaded:
                return
            if self._try_load_mlflow():
                return
            if self._try_load_local():
                return
            raise RuntimeError(
                "No clustering model available. Train one with "
                "`python scripts/train_clustering.py` or register a model "
                f"in MLflow at {settings().mlflow_tracking_uri}."
            )

    # ------------------------------------------------------------------
    def reload(self) -> dict[str, str]:
        """
        Force a fresh load from MLflow (or local disk). Use after promoting a
        new model version so the running API picks it up without a restart.
        """
        with self._lock:
            self._kmeans = None
            self._scaler = None
            self._feature_order = None
            self._model_version = "unloaded"
            self._source = "none"
        self.ensure_loaded()
        return {"model_version": self._model_version, "source": self._source}

    # ------------------------------------------------------------------
    def _try_load_mlflow(self) -> bool:
        """Load latest Production-stage clustering pipeline from MLflow."""
        s = settings()
        try:
            import mlflow
            from mlflow.tracking import MlflowClient
        except ImportError:
            logger.warning("mlflow not installed; skipping registry load")
            return False

        try:
            mlflow.set_tracking_uri(s.mlflow_tracking_uri)
            client = MlflowClient()
            uri = f"models:/{s.mlflow_model_name}/{s.mlflow_model_stage}"
            logger.info("loading clustering model from %s", uri)
            artifact = mlflow.sklearn.load_model(uri)

            # We log a dict {'kmeans': ..., 'scaler': ..., 'feature_order': [...]}
            # See ml/train_clustering.py for the contract.
            self._kmeans = artifact["kmeans"]
            self._scaler = artifact["scaler"]
            self._feature_order = list(artifact["feature_order"])

            # Resolve the concrete version number for diagnostics.
            versions = client.get_latest_versions(s.mlflow_model_name, [s.mlflow_model_stage])
            self._model_version = versions[0].version if versions else "unknown"
            self._source = f"mlflow:{uri}"
            logger.info("loaded clustering model version=%s", self._model_version)
            return True
        except Exception as exc:  # noqa: BLE001 — any failure falls back to disk
            logger.warning("mlflow load failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    def _try_load_local(self) -> bool:
        """Fallback: load joblib artifacts from models/clustering/."""
        try:
            import joblib
        except ImportError:
            logger.warning("joblib not installed; cannot load local model")
            return False

        kmeans_path = DEFAULT_MODEL_DIR / "kmeans_v1.joblib"
        scaler_path = DEFAULT_MODEL_DIR / "scaler_v1.joblib"
        meta_path = DEFAULT_MODEL_DIR / "metadata.json"

        if not (kmeans_path.exists() and scaler_path.exists() and meta_path.exists()):
            logger.info("local clustering model not found at %s", DEFAULT_MODEL_DIR)
            return False

        try:
            self._kmeans = joblib.load(kmeans_path)
            self._scaler = joblib.load(scaler_path)
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            self._feature_order = list(meta["feature_order"])
            self._model_version = f"local:{meta.get('version', 'v1')}"
            self._source = f"file:{DEFAULT_MODEL_DIR}"
            logger.info("loaded local clustering model %s", self._model_version)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("local clustering load failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    def predict(self, features: dict[str, Any]) -> tuple[int, float]:
        """
        Return (cluster_id, distance_to_centroid) for a single feature dict.

        Missing features default to 0 — same convention as the SQL view.
        """
        self.ensure_loaded()

        # Numpy is a transitive dep of scikit-learn — safe to import here.
        import numpy as np

        feature_order = self.feature_order
        vec = np.asarray(
            [[float(features.get(name) or 0.0) for name in feature_order]],
            dtype=float,
        )
        scaled = self._scaler.transform(vec)
        cluster_id = int(self._kmeans.predict(scaled)[0])
        # KMeans.transform returns distances to all centroids.
        distances = self._kmeans.transform(scaled)[0]
        distance = float(distances[cluster_id])
        return cluster_id, distance


# ---------------------------------------------------------------------------
# Diagnostics — used by /health and admin reload
# ---------------------------------------------------------------------------
def runtime_info() -> dict[str, str]:
    """Return identifiers useful for tracing a score back to its config."""
    return {
        "risk_score_config_hash": config_hash(),
    }
