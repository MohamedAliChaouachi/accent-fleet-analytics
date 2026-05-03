"""
fig_feature_importance.py
==========================
Render the surrogate feature importance for K-Means and Isolation Forest.

KMeans importance      = standard deviation of the per-cluster centroids
                          on the standardized features.
Isolation Forest imp.  = frequency at which each feature is used as a
                          split across the trees, normalized to [0, 1].

When the trained joblib artefacts are present in artifacts/ the script
loads them and reports the live importances. Otherwise it falls back to
the representative values reported in Chapter 5 so that the figure can
still be regenerated for the report on a workstation that does not carry
the model artefacts.
"""

from __future__ import annotations
from pathlib import Path
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

OUT = Path(__file__).resolve().parents[3] / "figures" / "feature_importance.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)
ACCENT = "#1F4E79"; SECOND = "#C00000"

ARTIFACTS = Path(os.environ.get("AFA_ARTIFACTS_DIR", "artifacts"))
KMEANS_MODEL = ARTIFACTS / "kmeans_tenant_1787.joblib"
IFOREST_MODEL = ARTIFACTS / "iforest_tenant_1787.joblib"

FEATURE_NAMES = [
    "overspeed_per_100km", "speed_alert_per_100km",
    "harsh_brake_per_100km", "harsh_accel_per_100km", "harsh_corner_per_100km",
    "high_speed_trip_ratio", "p95_max_speed",
    "monthly_idle_ratio", "high_rpm_minutes_per_day",
    "night_trip_ratio", "rush_hour_trip_ratio",
    "stddev_trip_distance", "short_trip_ratio",
]


# ----- live importances (when the joblib models are available) ------------

def kmeans_importance(model) -> np.ndarray:
    return model.cluster_centers_.std(axis=0)


def iforest_importance(model) -> np.ndarray:
    counts = np.zeros(len(FEATURE_NAMES))
    for tree in model.estimators_:
        for f in tree.tree_.feature:
            if f >= 0:
                counts[f] += 1
    total = counts.sum()
    return counts / total if total > 0 else counts


def _load_live() -> tuple[np.ndarray, np.ndarray] | None:
    if not (KMEANS_MODEL.exists() and IFOREST_MODEL.exists()):
        return None
    try:
        import joblib  # noqa: WPS433
    except ImportError:
        return None
    try:
        km_model = joblib.load(KMEANS_MODEL)
        iso_model = joblib.load(IFOREST_MODEL)
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] could not load model artefacts ({exc.__class__.__name__}); using fallback values")
        return None
    return kmeans_importance(km_model), iforest_importance(iso_model)


# ----- fallback (Chapter 5 representative values) -------------------------

def _fallback_importances() -> tuple[np.ndarray, np.ndarray]:
    """Surrogate importances cited in Chapter 5 for tenant 1787."""
    # Order matches FEATURE_NAMES.
    kmeans = np.array([
        0.92,  # overspeed_per_100km
        0.78,  # speed_alert_per_100km
        0.95,  # harsh_brake_per_100km
        0.88,  # harsh_accel_per_100km
        0.71,  # harsh_corner_per_100km
        0.65,  # high_speed_trip_ratio
        0.83,  # p95_max_speed
        0.42,  # monthly_idle_ratio
        0.55,  # high_rpm_minutes_per_day
        0.34,  # night_trip_ratio
        0.31,  # rush_hour_trip_ratio
        0.48,  # stddev_trip_distance
        0.27,  # short_trip_ratio
    ])
    iforest = np.array([
        0.135, 0.078,
        0.142, 0.121, 0.068,
        0.064, 0.116,
        0.038, 0.052,
        0.029, 0.025,
        0.083, 0.049,
    ])
    iforest = iforest / iforest.sum()
    return kmeans, iforest


# ----- rendering ----------------------------------------------------------

def plot(km: np.ndarray, iso: np.ndarray) -> None:
    df = pd.DataFrame({"feature": FEATURE_NAMES, "kmeans": km / km.sum(), "iforest": iso})
    df = df.sort_values("kmeans", ascending=True)
    y = np.arange(len(df))
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(y - 0.20, df["kmeans"],  height=0.4, color=ACCENT, label="K-Means")
    ax.barh(y + 0.20, df["iforest"], height=0.4, color=SECOND, label="Isolation Forest")
    ax.set_yticks(y); ax.set_yticklabels(df["feature"])
    ax.set_xlabel("normalized importance")
    ax.set_title("Surrogate feature importance for the unsupervised models", color=ACCENT)
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(OUT, bbox_inches="tight", dpi=300)
    print(f"wrote {OUT}")


def main() -> None:
    live = _load_live()
    if live is None:
        print(f"[info] artefacts not found in {ARTIFACTS.resolve()}; using fallback values")
        km, iso = _fallback_importances()
    else:
        km, iso = live
    plot(km, iso)


if __name__ == "__main__":
    main()
