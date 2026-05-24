"""
Inference layer for the FastAPI service.

Two model wrappers are exposed:

  - RiskPredictor — loads the bundled per-tenant Isolation Forest artifact
    from MLflow (preferred) or from ``models/risk_score/`` on disk
    (fallback). Scoring is per-tenant: given a feature vector AND a
    tenant_id, return the rescaled 0–100 anomaly score, its risk-category
    band, the model version, and per-feature z-score components.

  - ClusterPredictor — loads the persisted KMeans + StandardScaler from
    MLflow (preferred) or from ``models/clustering/`` on disk (fallback).
    Loaded lazily so the API can boot even before a model is registered.

Both predictors are loaded lazily so a fresh stack can boot before any
model has been trained; the first call that needs them triggers loading
and raises a clear error if nothing is available.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from typing import Any

import numpy as np

from accent_fleet.config import (
    PROJECT_ROOT,
    config_hash,
    load_feature_definitions,
    settings,
)

logger = logging.getLogger("accent_fleet.ml.inference")


# ---------------------------------------------------------------------------
# Common artifact-discovery paths
# ---------------------------------------------------------------------------
CLUSTERING_MODEL_DIR = PROJECT_ROOT / "models" / "clustering"
RISK_MODEL_DIR = PROJECT_ROOT / "models" / "risk_score"
RISK_ARTIFACT_FILENAME = "risk_v1.joblib"
RISK_METADATA_FILENAME = "metadata.json"

# Same epsilon used at fit time — kept in sync with train_risk.RESCALE_EPSILON.
_RESCALE_EPSILON = 1.0e-9


# ---------------------------------------------------------------------------
# Risk scoring
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RiskPrediction:
    """A single device-month risk-score prediction."""
    score: float
    category: str
    components: dict[str, float]
    model_version: str
    model_source: str


class RiskPredictor:
    """
    Loads the bundled per-tenant risk-score artifact and serves predictions.

    Loading strategy (in order):
      1. MLflow registry — ``models:/<risk_model_name>/<stage>``
      2. Local joblib file under ``models/risk_score/``

    The first ``.predict()`` call triggers loading. If both sources fail
    the predictor stays in 'unloaded' state and raises on use, so /health
    stays green and the API can be deployed before the first training run.

    Per-tenant semantics:
      Every tenant in the cohort has its own (scaler, IsolationForest,
      raw_min, raw_max, thresholds). A request for an unknown tenant is
      raised as ``TenantModelMissing`` — the API translates that to a 503
      because the feature itself is unavailable, not malformed.
    """

    def __init__(self) -> None:
        self._artifact: dict[str, Any] | None = None
        self._feature_order: list[str] | None = None
        self._model_version: str = "unloaded"
        self._source: str = "none"
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    @property
    def is_loaded(self) -> bool:
        return self._artifact is not None

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def source(self) -> str:
        return self._source

    @property
    def feature_order(self) -> list[str]:
        if self._feature_order is None:
            raise RuntimeError("RiskPredictor not loaded — no feature order.")
        return list(self._feature_order)

    @property
    def tenants(self) -> list[int]:
        """Tenants for which a per-tenant model is available."""
        if not self.is_loaded:
            return []
        return sorted(int(t) for t in (self._artifact or {}).get("tenants", {}))

    def has_tenant(self, tenant_id: int) -> bool:
        if not self.is_loaded:
            return False
        return int(tenant_id) in (self._artifact or {}).get("tenants", {})

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
                "No risk-score model available. Train one with "
                "`python scripts/train_risk_score.py` or register a model "
                f"in MLflow at {settings().mlflow_tracking_uri}."
            )

    # ------------------------------------------------------------------
    def reload(self) -> dict[str, str]:
        """
        Force a fresh load from MLflow (or local disk). Use after promoting
        a new model version so the running API picks it up without a restart.
        """
        with self._lock:
            self._artifact = None
            self._feature_order = None
            self._model_version = "unloaded"
            self._source = "none"
        self.ensure_loaded()
        return {"model_version": self._model_version, "source": self._source}

    # ------------------------------------------------------------------
    def _risk_model_name(self) -> str:
        cfg = (load_feature_definitions().get("risk_score_model") or {})
        return str(cfg.get("mlflow_model_name", "device-risk-score"))

    def _risk_model_stage(self) -> str:
        cfg = (load_feature_definitions().get("risk_score_model") or {})
        return str(cfg.get("mlflow_model_stage", settings().mlflow_model_stage))

    # ------------------------------------------------------------------
    def _try_load_mlflow(self) -> bool:
        """Load the latest Production-stage risk artifact from MLflow."""
        s = settings()
        try:
            import mlflow
            from mlflow.tracking import MlflowClient
        except ImportError:
            logger.warning("mlflow not installed; skipping risk registry load")
            return False

        try:
            mlflow.set_tracking_uri(s.mlflow_tracking_uri)
            client = MlflowClient()
            name = self._risk_model_name()
            stage = self._risk_model_stage()
            uri = f"models:/{name}/{stage}"
            logger.info("loading risk model from %s", uri)
            artifact = mlflow.sklearn.load_model(uri)

            self._artifact = artifact
            self._feature_order = list(artifact["feature_order"])

            versions = client.get_latest_versions(name, [stage])
            self._model_version = versions[0].version if versions else "unknown"
            self._source = f"mlflow:{uri}"
            logger.info(
                "loaded risk model version=%s (tenants=%s)",
                self._model_version,
                sorted(int(t) for t in artifact.get("tenants", {})),
            )
            return True
        except Exception as exc:  # noqa: BLE001 — any failure falls back to disk
            logger.warning("mlflow load failed for risk model: %s", exc)
            return False

    # ------------------------------------------------------------------
    def _try_load_local(self) -> bool:
        """Fallback: load the bundled joblib artifact from models/risk_score/."""
        try:
            import joblib
        except ImportError:
            logger.warning("joblib not installed; cannot load local risk model")
            return False

        artifact_path = RISK_MODEL_DIR / RISK_ARTIFACT_FILENAME
        meta_path = RISK_MODEL_DIR / RISK_METADATA_FILENAME

        if not (artifact_path.exists() and meta_path.exists()):
            logger.info("local risk model not found at %s", RISK_MODEL_DIR)
            return False

        try:
            self._artifact = joblib.load(artifact_path)
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            self._feature_order = list(meta["feature_order"])
            self._model_version = f"local:{meta.get('version', 'v1')}"
            self._source = f"file:{RISK_MODEL_DIR}"
            logger.info("loaded local risk model %s", self._model_version)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("local risk load failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    def _tenant_entry(self, tenant_id: int) -> dict[str, Any]:
        """Return the per-tenant artifact entry or raise TenantModelMissing."""
        self.ensure_loaded()
        tenants = (self._artifact or {}).get("tenants", {})
        entry = tenants.get(int(tenant_id))
        if entry is None:
            raise TenantModelMissing(
                f"no risk-score model for tenant_id={tenant_id}. "
                f"Trained tenants: {sorted(int(t) for t in tenants)}."
            )
        return entry

    # ------------------------------------------------------------------
    def predict(
        self,
        tenant_id: int,
        features: dict[str, Any],
    ) -> RiskPrediction:
        """
        Return the rescaled 0–100 risk score for a single feature vector.

        Missing feature keys default to 0 — same convention as the SQL view
        and the clustering predictor.
        """
        entry = self._tenant_entry(tenant_id)
        feature_order = self.feature_order

        vec = np.asarray(
            [float(features.get(name) or 0.0) for name in feature_order],
            dtype=float,
        ).reshape(1, -1)

        scaled = entry["scaler"].transform(vec)
        raw = -entry["model"].decision_function(scaled)
        score = _rescale_one(raw[0], entry["raw_min"], entry["raw_max"])
        category = _categorize_one(score, entry["thresholds"])

        # Components are per-feature z-scores from the per-tenant scaler.
        # The scaler has already standardised them above so we just expose
        # the row's scaled values keyed by feature name — they're the
        # post-standardisation z-scores by definition.
        components = {
            name: round(float(scaled[0, i]), 4)
            for i, name in enumerate(feature_order)
        }

        return RiskPrediction(
            score=round(float(score), 1),
            category=category,
            components=components,
            model_version=self._model_version,
            model_source=self._source,
        )

    # ------------------------------------------------------------------
    def predict_batch(
        self,
        tenant_id: int,
        features_df: "Any",  # pandas.DataFrame; typed as Any to keep import light
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Vectorised batch predict for the offline scoring path
        (``batch_scoring.score_risk_partitions``).

        Returns (scores_0_100, category_labels_object_dtype).
        """
        entry = self._tenant_entry(tenant_id)
        feature_order = self.feature_order

        X = features_df.reindex(columns=feature_order).fillna(0).to_numpy(dtype=float)
        if X.size == 0:
            return np.zeros((0,)), np.zeros((0,), dtype=object)

        scaled = entry["scaler"].transform(X)
        raw = -entry["model"].decision_function(scaled)
        scores = _rescale_array(raw, entry["raw_min"], entry["raw_max"])
        labels = _categorize_array(scores, entry["thresholds"])
        return scores, labels


class TenantModelMissing(LookupError):
    """Raised by RiskPredictor when no per-tenant model is fitted for tenant_id."""


# ---------------------------------------------------------------------------
# Pure helpers — mirror train_risk's math so prediction == training-time scoring
# ---------------------------------------------------------------------------
def _rescale_one(raw: float, raw_min: float, raw_max: float) -> float:
    span = max(raw_max - raw_min, _RESCALE_EPSILON)
    scaled = (raw - raw_min) / span
    if scaled < 0.0:
        scaled = 0.0
    elif scaled > 1.0:
        scaled = 1.0
    return scaled * 100.0


def _rescale_array(
    raw: np.ndarray, raw_min: float, raw_max: float
) -> np.ndarray:
    span = max(raw_max - raw_min, _RESCALE_EPSILON)
    scaled = (raw - raw_min) / span
    return np.clip(scaled, 0.0, 1.0) * 100.0


def _categorize_one(score: float, thresholds: dict[str, float]) -> str:
    if score >= thresholds["critical"]:
        return "critical"
    if score >= thresholds["high"]:
        return "high"
    if score >= thresholds["moderate"]:
        return "moderate"
    return "low"


def _categorize_array(
    scores: np.ndarray, thresholds: dict[str, float]
) -> np.ndarray:
    out = np.full(scores.shape, "low", dtype=object)
    out[scores >= thresholds["moderate"]] = "moderate"
    out[scores >= thresholds["high"]] = "high"
    out[scores >= thresholds["critical"]] = "critical"
    return out


# ---------------------------------------------------------------------------
# Clustering predictor (unchanged from prior version)
# ---------------------------------------------------------------------------
class ClusterPredictor:
    """
    Loads a fitted KMeans + StandardScaler and predicts cluster ids.

    Loading strategy (in order):
      1. MLflow registry — ``models:/<name>/<stage>``
      2. Local joblib files under ``models/clustering/``

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

        kmeans_path = CLUSTERING_MODEL_DIR / "kmeans_v1.joblib"
        scaler_path = CLUSTERING_MODEL_DIR / "scaler_v1.joblib"
        meta_path = CLUSTERING_MODEL_DIR / "metadata.json"

        if not (kmeans_path.exists() and scaler_path.exists() and meta_path.exists()):
            logger.info("local clustering model not found at %s", CLUSTERING_MODEL_DIR)
            return False

        try:
            self._kmeans = joblib.load(kmeans_path)
            self._scaler = joblib.load(scaler_path)
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            self._feature_order = list(meta["feature_order"])
            self._model_version = f"local:{meta.get('version', 'v1')}"
            self._source = f"file:{CLUSTERING_MODEL_DIR}"
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
