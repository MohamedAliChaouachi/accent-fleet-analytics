"""
Train + persist + register the device-risk-score model.

The risk score is an UNSUPERVISED anomaly score, not a hand-tuned formula.
For each tenant in the configured cohort we fit an Isolation Forest on the
13 driver-behavior features from ``marts.v_ml_features_full``, capture the
fit-time min/max of the raw anomaly score and the per-tenant quantile-based
risk-band thresholds, and bundle every per-tenant artifact into a single
MLflow model version so rollback is atomic across the cohort.

Lifted from ``notebooks/04_modeling/02_anomaly_risk_score.ipynb`` but
restructured for reproducibility and matching the layout of
``train_clustering.py``:
  - ``random_state=42`` pinned everywhere
  - one fitted artifact per (tenant_id) packaged as a dict in a single
    MLflow registry version under ``device-risk-score``
  - artifacts written both to MLflow (preferred) and to
    ``models/risk_score/`` on disk (fallback for environments without
    a reachable MLflow server)

Bundled artifact contract (the dict logged to MLflow and persisted to
``models/risk_score/risk_v1.joblib``):

    {
      "tenants": {
        <tenant_id:int>: {
          "scaler": StandardScaler,
          "model":  IsolationForest,
          "raw_min": float,          # min of raw = -decision_function(X_train)
          "raw_max": float,
          "thresholds": {            # boundaries on the 0-100 rescaled score
            "moderate": float,       # = p50
            "high":     float,       # = p80
            "critical": float,       # = p95
          },
          "feature_means": list[float],   # for component z-scores at inference
          "feature_stds":  list[float],
          "n_rows": int,
          "score_share": {           # category mix at fit time, for the gate
            "low": float, "moderate": float, "high": float, "critical": float,
          },
        },
        ...
      },
      "feature_order": list[str],
      "config": dict,                # risk_score_model block from YAML
    }
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from accent_fleet.config import (
    PROJECT_ROOT,
    load_feature_definitions,
    load_pipeline_config,
    settings,
)
from accent_fleet.db.engine import get_engine

logger = logging.getLogger("accent_fleet.ml.train_risk")

# ---------------------------------------------------------------------------
# Feature set — MUST stay in sync with config/feature_definitions.yaml
# (risk_score_model.features) and with marts.v_ml_features_full.
# Treated as a tuple here so it's hashable and importable as a constant by
# other modules (drift, batch scoring) without going through the YAML path.
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
ARTIFACT_DIR = PROJECT_ROOT / "models" / "risk_score"
ARTIFACT_FILENAME = "risk_v1.joblib"
METADATA_FILENAME = "metadata.json"

# Score-rescaling epsilon — matches the YAML default. Kept here too so the
# inference code path can import it without round-tripping through YAML.
RESCALE_EPSILON = 1.0e-9


# ---------------------------------------------------------------------------
# Result dataclasses — used by the Prefect retrain task and tests.
# ---------------------------------------------------------------------------
@dataclass
class TenantTrainStats:
    """Per-tenant training metrics — drives the promotion gate."""
    tenant_id: int
    n_rows: int
    raw_min: float
    raw_max: float
    threshold_moderate: float
    threshold_high: float
    threshold_critical: float
    share_low: float
    share_moderate: float
    share_high: float
    share_critical: float


@dataclass
class TrainResult:
    """Top-level outcome of a risk-model training run."""
    feature_order: list[str]
    tenants: list[int] = field(default_factory=list)
    rows_by_tenant: dict[int, int] = field(default_factory=dict)
    skipped_tenants: dict[int, str] = field(default_factory=dict)
    tenant_stats: dict[int, TenantTrainStats] = field(default_factory=dict)
    overall_share: dict[str, float] = field(default_factory=dict)
    training_window: str = ""
    n_rows: int = 0

    @property
    def n_tenants(self) -> int:
        return len(self.tenant_stats)


# ---------------------------------------------------------------------------
# YAML accessors
# ---------------------------------------------------------------------------
def _risk_model_config() -> dict[str, Any]:
    cfg = load_feature_definitions().get("risk_score_model") or {}
    if not cfg:
        raise RuntimeError(
            "feature_definitions.yaml is missing the risk_score_model block. "
            "See the file header for the expected structure."
        )
    return cfg


def _hyperparams() -> dict[str, Any]:
    return dict(_risk_model_config().get("hyperparameters") or {})


def _min_rows_per_tenant() -> int:
    return int(_risk_model_config().get("min_rows_per_tenant", 50))


def _quantiles() -> tuple[float, float, float]:
    """Return (low->moderate, moderate->high, high->critical) quantiles."""
    q = _risk_model_config().get("thresholds", {}).get("quantiles", {}) or {}
    return (
        float(q.get("low_to_moderate", 0.50)),
        float(q.get("moderate_to_high", 0.80)),
        float(q.get("high_to_critical", 0.95)),
    )


def _mlflow_model_name() -> str:
    return str(_risk_model_config().get("mlflow_model_name", "device-risk-score"))


def _mlflow_experiment_name() -> str:
    return str(_risk_model_config().get("mlflow_experiment_name", "device-risk-score"))


def _mlflow_model_stage() -> str:
    return str(_risk_model_config().get("mlflow_model_stage", "Production"))


# ---------------------------------------------------------------------------
# Tenant cohort validation — same shape as train_clustering._validate_tenant_coverage
# but reusable independently.
# ---------------------------------------------------------------------------
def _configured_tenants(key: str) -> tuple[int, ...]:
    cfg = load_pipeline_config().get("modeling") or {}
    return tuple(int(t) for t in cfg.get(key, []) or [])


def _rows_by_tenant(df: pd.DataFrame) -> dict[int, int]:
    if df.empty or "tenant_id" not in df:
        return {}
    counts = df["tenant_id"].value_counts().sort_index()
    return {int(tenant_id): int(n) for tenant_id, n in counts.items()}


def _validate_tenant_coverage(df: pd.DataFrame) -> None:
    """
    Loud-fail if a required modeling tenant is missing from the training
    frame. Soft-warn for merely expected tenants. The set of tenants that
    pass min_rows is checked separately downstream — coverage here means
    "did the marts produce ANY rows for this tenant".
    """
    rows_by_tenant = _rows_by_tenant(df)
    present = set(rows_by_tenant)
    expected = set(_configured_tenants("expected_tenants"))
    required = set(_configured_tenants("required_tenants"))

    logger.info("risk training tenant coverage: %s", rows_by_tenant)

    missing_expected = sorted(expected - present)
    if missing_expected:
        logger.warning("expected modeling tenant(s) absent after filters: %s", missing_expected)

    missing_required = sorted(required - present)
    if missing_required:
        raise ValueError(
            "risk training frame is missing required tenant(s) "
            f"{missing_required}. Tenant 7486 depends on telemetry-to-trip "
            "reconstruction; run scripts/reconstruct_telemetry_trips.py "
            "--tenant-id 7486 --from-month 2025-01, then rebuild the marts."
        )


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_training_frame(month_from: str = "2025-01") -> pd.DataFrame:
    """
    Pull device-month rows with enough activity to be worth scoring.

    Same filter as ``train_clustering.load_training_frame`` so the two
    models train on a comparable population. Tenants with fewer than
    ``min_rows_per_tenant`` rows are *kept* in the frame here — the
    fitter decides per tenant whether to skip; we still want to log the
    counts for visibility.
    """
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
    logger.info("loaded %d rows for risk training (window >= %s)", len(df), month_from)
    _validate_tenant_coverage(df)
    return df


# ---------------------------------------------------------------------------
# Core math — split out so unit tests can drive them with synthetic frames
# without touching the database.
# ---------------------------------------------------------------------------
def rescale_raw_to_0_100(
    raw: np.ndarray,
    raw_min: float,
    raw_max: float,
    *,
    eps: float = RESCALE_EPSILON,
) -> np.ndarray:
    """
    Linearly rescale raw anomaly scores to [0, 100] using fit-time bounds.

    Values outside [raw_min, raw_max] are clipped — see D2 in the design
    notes. Returning the same array shape regardless of bound geometry
    keeps the caller code branch-free.
    """
    span = max(raw_max - raw_min, eps)
    scaled = (raw - raw_min) / span
    scaled = np.clip(scaled, 0.0, 1.0)
    return scaled * 100.0


def quantile_thresholds(
    scores_0_100: np.ndarray,
    quantiles: tuple[float, float, float],
) -> tuple[float, float, float]:
    """Per-tenant thresholds on the rescaled score, in 0-100 space."""
    q_lm, q_mh, q_hc = quantiles
    if scores_0_100.size == 0:
        return (0.0, 0.0, 0.0)
    return (
        float(np.quantile(scores_0_100, q_lm)),
        float(np.quantile(scores_0_100, q_mh)),
        float(np.quantile(scores_0_100, q_hc)),
    )


def categorize_scores(
    scores_0_100: np.ndarray,
    thresholds: tuple[float, float, float],
) -> np.ndarray:
    """
    Vectorised band assignment matching the YAML semantics:
        low      = [0,         moderate)
        moderate = [moderate,  high)
        high     = [high,      critical)
        critical = [critical,  100]
    Returns a numpy array of strings — small categorical, so dtype=object
    is fine here.
    """
    t_m, t_h, t_c = thresholds
    out = np.full(scores_0_100.shape, "low", dtype=object)
    out[scores_0_100 >= t_m] = "moderate"
    out[scores_0_100 >= t_h] = "high"
    out[scores_0_100 >= t_c] = "critical"
    return out


def _share(labels: np.ndarray) -> dict[str, float]:
    """Category mix as fractions in [0, 1] — used by the promotion gate."""
    if labels.size == 0:
        return {"low": 0.0, "moderate": 0.0, "high": 0.0, "critical": 0.0}
    out: dict[str, float] = {}
    n = float(labels.size)
    for cat in ("low", "moderate", "high", "critical"):
        out[cat] = float((labels == cat).sum()) / n
    return out


# ---------------------------------------------------------------------------
# Per-tenant fit
# ---------------------------------------------------------------------------
def fit_one_tenant(
    sub: pd.DataFrame,
    *,
    hyperparams: dict[str, Any] | None = None,
    quantiles: tuple[float, float, float] | None = None,
    min_rows: int | None = None,
) -> dict[str, Any] | None:
    """
    Fit a per-tenant Isolation Forest and return the bundled artifact entry.

    Returns ``None`` when the tenant has fewer than ``min_rows`` rows — the
    caller treats this as a skip and logs it in ``TrainResult.skipped_tenants``.
    """
    rows = len(sub)
    threshold = min_rows if min_rows is not None else _min_rows_per_tenant()
    if rows < threshold:
        return None

    hp = dict(hyperparams or _hyperparams())
    quants = quantiles or _quantiles()

    X = sub[list(FEATURES)].fillna(0).to_numpy(dtype=float)
    scaler = StandardScaler().fit(X)
    Xs = scaler.transform(X)

    iso = IsolationForest(
        n_estimators=int(hp.get("n_estimators", 200)),
        contamination=hp.get("contamination", "auto"),
        random_state=int(hp.get("random_state", RANDOM_STATE)),
        n_jobs=int(hp.get("n_jobs", -1)),
    ).fit(Xs)

    raw = -iso.decision_function(Xs)  # higher = more anomalous
    raw_min = float(raw.min())
    raw_max = float(raw.max())

    scores = rescale_raw_to_0_100(raw, raw_min, raw_max)
    t_m, t_h, t_c = quantile_thresholds(scores, quants)
    labels = categorize_scores(scores, (t_m, t_h, t_c))

    # feature_means / feature_stds come from the fitted scaler — store them
    # explicitly so the inference path doesn't have to know that
    # StandardScaler exposes them as `mean_` / `scale_`. (Component z-scores
    # at inference are just (x - mean) / std per feature.)
    return {
        "scaler": scaler,
        "model": iso,
        "raw_min": raw_min,
        "raw_max": raw_max,
        "thresholds": {
            "moderate": t_m,
            "high":     t_h,
            "critical": t_c,
        },
        "feature_means": [float(v) for v in scaler.mean_.tolist()],
        "feature_stds":  [float(v) for v in scaler.scale_.tolist()],
        "n_rows": rows,
        "score_share": _share(labels),
    }


# ---------------------------------------------------------------------------
# Top-level fit across all tenants
# ---------------------------------------------------------------------------
def fit_risk_model(df: pd.DataFrame) -> tuple[dict[str, Any], TrainResult]:
    """
    Fit one Isolation Forest per tenant_id present in `df`, returning the
    bundled artifact plus the aggregated TrainResult used by the gate.

    Tenants with fewer than ``min_rows_per_tenant`` rows are skipped and
    listed in ``result.skipped_tenants`` — the API will return 503 for
    devices in those tenants until a future training run sees more data.
    """
    if df.empty:
        raise ValueError("training frame is empty — nothing to fit")

    if "tenant_id" not in df.columns:
        raise ValueError("training frame must include a tenant_id column")

    hp = _hyperparams()
    quants = _quantiles()
    threshold = _min_rows_per_tenant()

    artifact: dict[str, Any] = {
        "tenants": {},
        "feature_order": list(FEATURES),
        "config": {
            "algorithm": _risk_model_config().get("algorithm"),
            "hyperparameters": hp,
            "quantiles": {
                "low_to_moderate": quants[0],
                "moderate_to_high": quants[1],
                "high_to_critical": quants[2],
            },
            "rescale_epsilon": RESCALE_EPSILON,
            "min_rows_per_tenant": threshold,
        },
    }
    result = TrainResult(
        feature_order=list(FEATURES),
        rows_by_tenant=_rows_by_tenant(df),
        n_rows=len(df),
    )
    overall_counts = {"low": 0, "moderate": 0, "high": 0, "critical": 0}
    overall_n = 0

    for tenant_id, sub in df.groupby("tenant_id", sort=True):
        tid = int(tenant_id)
        entry = fit_one_tenant(
            sub,
            hyperparams=hp,
            quantiles=quants,
            min_rows=threshold,
        )
        if entry is None:
            reason = f"below_min_rows ({len(sub)} < {threshold})"
            result.skipped_tenants[tid] = reason
            logger.info("skipped tenant %d: %s", tid, reason)
            continue

        artifact["tenants"][tid] = entry
        n_rows = entry["n_rows"]
        share = entry["score_share"]
        result.tenants.append(tid)
        result.tenant_stats[tid] = TenantTrainStats(
            tenant_id=tid,
            n_rows=n_rows,
            raw_min=entry["raw_min"],
            raw_max=entry["raw_max"],
            threshold_moderate=entry["thresholds"]["moderate"],
            threshold_high=entry["thresholds"]["high"],
            threshold_critical=entry["thresholds"]["critical"],
            share_low=share["low"],
            share_moderate=share["moderate"],
            share_high=share["high"],
            share_critical=share["critical"],
        )
        for cat, frac in share.items():
            overall_counts[cat] += int(round(frac * n_rows))
        overall_n += n_rows

    if not result.tenants:
        raise ValueError(
            "no tenant had >= min_rows_per_tenant rows; refusing to register "
            "an empty risk model."
        )

    if overall_n > 0:
        result.overall_share = {k: v / overall_n for k, v in overall_counts.items()}
    return artifact, result


# ---------------------------------------------------------------------------
# Persistence — local joblib (fallback) and MLflow registry (preferred)
# ---------------------------------------------------------------------------
def save_local(artifact: dict[str, Any], result: TrainResult) -> None:
    """Write the bundled artifact + metadata under models/risk_score/."""
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, ARTIFACT_DIR / ARTIFACT_FILENAME)
    metadata: dict[str, Any] = {
        "version": "v1",
        "model_name": _mlflow_model_name(),
        "trained_at": datetime.now(UTC).isoformat(),
        "feature_order": result.feature_order,
        "tenants": result.tenants,
        "rows_by_tenant": result.rows_by_tenant,
        "skipped_tenants": result.skipped_tenants,
        "tenant_stats": {
            str(tid): {
                "n_rows": st.n_rows,
                "raw_min": st.raw_min,
                "raw_max": st.raw_max,
                "thresholds": {
                    "moderate": st.threshold_moderate,
                    "high": st.threshold_high,
                    "critical": st.threshold_critical,
                },
                "score_share": {
                    "low": st.share_low,
                    "moderate": st.share_moderate,
                    "high": st.share_high,
                    "critical": st.share_critical,
                },
            }
            for tid, st in result.tenant_stats.items()
        },
        "overall_share": result.overall_share,
        "n_rows": result.n_rows,
        "random_state": RANDOM_STATE,
        "training_window": result.training_window,
        "config": artifact.get("config", {}),
    }
    (ARTIFACT_DIR / METADATA_FILENAME).write_text(json.dumps(metadata, indent=2))
    logger.info("wrote risk artifacts to %s", ARTIFACT_DIR)


def log_to_mlflow(
    artifact: dict[str, Any],
    result: TrainResult,
    promote: bool = True,
) -> str | None:
    """
    Log + register a bundled risk artifact. Returns the new model version
    string. Returns ``None`` only on infrastructure failure (mlflow missing
    or server unreachable). Returns the version even when ``promote=False``
    so callers (see ml/promotion.py) can gate the transition.
    """
    try:
        import mlflow
        import mlflow.sklearn
        from mlflow.tracking import MlflowClient
    except ImportError:
        logger.warning("mlflow not installed; skipping risk-model registry log")
        return None

    s = settings()
    model_name = _mlflow_model_name()
    experiment_name = _mlflow_experiment_name()
    stage = _mlflow_model_stage()

    mlflow.set_tracking_uri(s.mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run() as run:
        mlflow.log_params({
            "algorithm": artifact["config"].get("algorithm", "isolation_forest"),
            "n_estimators": artifact["config"]["hyperparameters"].get("n_estimators"),
            "contamination": artifact["config"]["hyperparameters"].get("contamination"),
            "random_state": artifact["config"]["hyperparameters"].get("random_state"),
            "n_features": len(result.feature_order),
            "n_tenants": result.n_tenants,
            "n_rows": result.n_rows,
            "min_rows_per_tenant": artifact["config"].get("min_rows_per_tenant"),
        })
        # Cohort-level metrics — these are what the promotion gate compares.
        for cat, frac in result.overall_share.items():
            mlflow.log_metric(f"overall_share_{cat}", frac)
        # Per-tenant metrics for diagnostics. Tags rather than param keys
        # because tags accept the dotted/numeric tenant id pattern.
        for tid, st in result.tenant_stats.items():
            mlflow.log_metric(f"tenant_{tid}_n_rows", st.n_rows)
            mlflow.log_metric(f"tenant_{tid}_share_critical", st.share_critical)
            mlflow.log_metric(f"tenant_{tid}_share_high", st.share_high)
            mlflow.log_metric(f"tenant_{tid}_threshold_critical", st.threshold_critical)
            mlflow.log_metric(f"tenant_{tid}_threshold_high", st.threshold_high)
        mlflow.log_dict(
            {
                "feature_order": result.feature_order,
                "tenants": result.tenants,
                "skipped_tenants": result.skipped_tenants,
                "overall_share": result.overall_share,
                "rows_by_tenant": result.rows_by_tenant,
            },
            "metadata.json",
        )
        # mlflow.sklearn understands an arbitrary picklable Python object;
        # the bundled dict of (scaler, model, raw_min/max, thresholds) is
        # picklable because everything inside is.
        mlflow.sklearn.log_model(
            artifact,  # type: ignore[arg-type]
            artifact_path="model",
            registered_model_name=model_name,
        )
        run_id = run.info.run_id

    client = MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    latest = max(versions, key=lambda v: int(v.version))

    if promote:
        client.transition_model_version_stage(
            name=model_name,
            version=latest.version,
            stage=stage,
            archive_existing_versions=True,
        )
        logger.info(
            "registered %s v%s -> %s (run=%s)",
            model_name, latest.version, stage, run_id,
        )
    else:
        logger.info(
            "registered %s v%s (no promotion) (run=%s)",
            model_name, latest.version, run_id,
        )
    return latest.version


# ---------------------------------------------------------------------------
# End-to-end entrypoint — used by scripts/train_risk_score.py and tests.
# ---------------------------------------------------------------------------
def run(month_from: str = "2025-01", promote: bool = True) -> TrainResult:
    """End-to-end training + registration."""
    logging.basicConfig(
        level=os.environ.get("PIPELINE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    df = load_training_frame(month_from=month_from)
    artifact, result = fit_risk_model(df)
    result.training_window = f">= {month_from}"
    save_local(artifact, result)
    log_to_mlflow(artifact, result, promote=promote)
    return result
