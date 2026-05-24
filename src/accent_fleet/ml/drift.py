"""
Feature drift detection (CRISP-DM §2.5, first chunk).

We compute the Population Stability Index (PSI) per feature between two
windows of `marts.v_ml_features_full`:

  - **reference**: the months immediately preceding the freshly-scored batch
    (default: 6 months, ending one month before the earliest touched month).
    Re-deriving the reference window every run instead of pinning it to
    the training window means a model that was trained 18 months ago is
    still measured against a moving recent baseline — drift here means
    "the population shifted vs. recent history", which is what the ops
    team cares about for stale-features-vs-still-current-model alerts.

  - **current**: the touched months that the batch flow just (re)scored.

PSI per feature is:

      PSI = Σ_i (p_current[i] - p_reference[i]) * ln(p_current[i] / p_reference[i])

…over `n_bins` quantile bins derived from the reference. Industry rule of
thumb (Karakoulas 2004; used by SAS, FICO and most banking model
monitoring):

      PSI < 0.10  → stable
      PSI < 0.25  → moderate change
      PSI ≥ 0.25  → significant drift (default alert threshold here)

Why PSI and not KL divergence directly:
  PSI is the symmetric KL (KL(P||Q) + KL(Q||P)) restricted to a histogram
  of P. It's bounded behaviour-wise (no infinite spikes from zero bins
  when we add the standard epsilon floor), more interpretable across
  features with different scales, and the threshold table above is
  widely understood. The Prometheus gauge is still named
  `*_drift_score` so we can swap implementations later without
  touching dashboards.

The Prefect task that wraps this never raises — drift is informational.
A flow that drift-checks itself shouldn't fail because the model is
suddenly stale; it should keep loading data and flag the problem so
humans can decide whether to retrain.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import text

from accent_fleet.db.engine import get_engine
from accent_fleet.ml.train_clustering import FEATURES as DEFAULT_FEATURES

log = structlog.get_logger("accent_fleet.ml.drift")


# Calibration constants. Keep these module-level so they're trivially
# patchable from tests and not hidden in a settings dict.
PSI_ALERT_THRESHOLD = 0.25
PSI_N_BINS = 10
PSI_EPSILON = 1e-6  # floor for empty bins to keep log() finite
DEFAULT_REFERENCE_WINDOW_MONTHS = 6


@dataclass
class FeatureDrift:
    feature: str
    psi: float
    drifted: bool


@dataclass
class DriftReport:
    reference_months: list[str]
    current_months: list[str]
    n_reference_rows: int
    n_current_rows: int
    threshold: float
    features: list[FeatureDrift] = field(default_factory=list)

    @property
    def drifted_features(self) -> list[str]:
        return [f.feature for f in self.features if f.drifted]

    @property
    def any_drifted(self) -> bool:
        return any(f.drifted for f in self.features)


# ---------------------------------------------------------------------------
# Math: PSI between two 1-D samples.
# ---------------------------------------------------------------------------
def compute_psi(
    reference: np.ndarray,
    current: np.ndarray,
    n_bins: int = PSI_N_BINS,
) -> float:
    """
    PSI between `reference` and `current` after binning into `n_bins`
    quantile bins of the reference distribution.

    Returns 0.0 when either side is empty (we don't have evidence of drift),
    not NaN — keeps downstream comparisons trivial.
    """
    ref = np.asarray(reference, dtype=float)
    cur = np.asarray(current, dtype=float)
    ref = ref[~np.isnan(ref)]
    cur = cur[~np.isnan(cur)]
    if ref.size == 0 or cur.size == 0:
        return 0.0

    # Quantile bin edges from reference. Deduplicate to handle features
    # with many tied values (e.g. mostly-zero counts) — np.quantile can
    # return repeated boundaries, which would create zero-width bins and
    # poison np.histogram.
    quantiles = np.linspace(0, 1, n_bins + 1)
    edges = np.unique(np.quantile(ref, quantiles))
    if edges.size < 3:
        # Reference is effectively a single value → PSI is undefined.
        # Treat as "no signal", matches the empty-input contract above.
        return 0.0
    # Expand the outer edges by a tiny amount so values exactly at the
    # min/max land in the outermost bins instead of being clipped out.
    edges[0] = -np.inf
    edges[-1] = np.inf

    ref_counts, _ = np.histogram(ref, bins=edges)
    cur_counts, _ = np.histogram(cur, bins=edges)

    ref_pct = ref_counts / ref.size
    cur_pct = cur_counts / cur.size

    # Floor empty bins to avoid log(0). The same epsilon on both sides
    # keeps the symmetric property of PSI.
    ref_pct = np.where(ref_pct == 0, PSI_EPSILON, ref_pct)
    cur_pct = np.where(cur_pct == 0, PSI_EPSILON, cur_pct)

    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


# ---------------------------------------------------------------------------
# Window computation.
# ---------------------------------------------------------------------------
def _months_before(yyyy_mm: str, n: int) -> list[str]:
    """
    Return the `n` calendar months strictly preceding `yyyy_mm`, in
    ascending order. Pure integer arithmetic — no python-dateutil.
    """
    year, month = (int(x) for x in yyyy_mm.split("-"))
    idx_end = year * 12 + (month - 1)  # exclusive upper bound
    out = []
    for k in range(n, 0, -1):
        idx = idx_end - k
        y, m0 = divmod(idx, 12)
        out.append(f"{y:04d}-{m0 + 1:02d}")
    return out


def derive_reference_window(
    current_months: list[str],
    n_months: int = DEFAULT_REFERENCE_WINDOW_MONTHS,
) -> list[str]:
    """
    The N months immediately before the earliest scored month.

    We anchor to min(current_months) so a backfill that re-scores 2024
    measures drift against 2023-H2, not against "the past 6 months from
    today" — which would be meaningless on a historical replay.
    """
    if not current_months:
        return []
    anchor = min(current_months)
    return _months_before(anchor, n_months)


# ---------------------------------------------------------------------------
# Feature loading.
# ---------------------------------------------------------------------------
def _load_feature_frame(months: list[str], features: list[str]) -> pd.DataFrame:
    """
    Pull just the columns we need for drift comparison. Same source view
    as batch_scoring (`v_ml_features_full`) so reference and current go
    through the identical COALESCE / filter pipeline — drift then reflects
    real distribution shift, not view-vs-view artefact.
    """
    if not months:
        return pd.DataFrame(columns=features)
    cols = ", ".join(features)
    sql = text(
        f"""
        SELECT {cols}
          FROM marts.v_ml_features_full
         WHERE year_month = ANY(CAST(:months AS text[]))
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"months": months})


# ---------------------------------------------------------------------------
# Public entry point — pure function over two DataFrames so tests don't
# need a database. The DB-backed driver lives in `detect_drift_for_months`.
# ---------------------------------------------------------------------------
def compare_frames(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    features: list[str],
    *,
    threshold: float = PSI_ALERT_THRESHOLD,
    n_bins: int = PSI_N_BINS,
    reference_months: list[str] | None = None,
    current_months: list[str] | None = None,
) -> DriftReport:
    """Compute the drift report from two already-loaded frames."""
    report = DriftReport(
        reference_months=reference_months or [],
        current_months=current_months or [],
        n_reference_rows=len(reference),
        n_current_rows=len(current),
        threshold=threshold,
    )
    for feat in features:
        if feat not in reference.columns or feat not in current.columns:
            log.warning("drift.feature_missing_from_view", feature=feat)
            continue
        psi = compute_psi(
            reference[feat].to_numpy(dtype=float, na_value=np.nan),
            current[feat].to_numpy(dtype=float, na_value=np.nan),
            n_bins=n_bins,
        )
        report.features.append(
            FeatureDrift(feature=feat, psi=psi, drifted=psi >= threshold)
        )
    return report


def compute_score_drift(
    current_months: list[str],
    *,
    reference_window_months: int = DEFAULT_REFERENCE_WINDOW_MONTHS,
    n_bins: int = PSI_N_BINS,
) -> float | None:
    """
    PSI of the IF risk-score distribution between two windows of
    ``marts.fact_device_risk_score``.

    Returns the score-level PSI as a single float, or None when either
    side has no rows (a fresh stack, or current_months is empty). Used by
    the risk-model promotion gate — see ``ml.promotion.retrain_risk_with_gate``.

    Reference window = the N months immediately before the earliest current
    month, same anchoring rule as ``detect_drift_for_months`` so a backfill
    is measured against its own neighbourhood.
    """
    if not current_months:
        return None

    reference_months = derive_reference_window(current_months, reference_window_months)
    if not reference_months:
        return None

    sql = text(
        """
        SELECT year_month, risk_score
          FROM marts.fact_device_risk_score
         WHERE year_month = ANY(CAST(:months AS text[]))
        """
    )
    with get_engine().connect() as conn:
        ref_df = pd.read_sql(sql, conn, params={"months": reference_months})
        cur_df = pd.read_sql(sql, conn, params={"months": list(current_months)})

    if ref_df.empty or cur_df.empty:
        log.info(
            "drift.score.empty_window",
            reference_rows=len(ref_df),
            current_rows=len(cur_df),
        )
        return None

    psi = compute_psi(
        ref_df["risk_score"].to_numpy(dtype=float, na_value=np.nan),
        cur_df["risk_score"].to_numpy(dtype=float, na_value=np.nan),
        n_bins=n_bins,
    )
    log.info(
        "drift.score.computed",
        psi=psi,
        n_reference_rows=len(ref_df),
        n_current_rows=len(cur_df),
        reference_months=reference_months,
        current_months=list(current_months),
    )
    return float(psi)


def detect_drift_for_months(
    current_months: list[str],
    *,
    features: list[str] | None = None,
    reference_window_months: int = DEFAULT_REFERENCE_WINDOW_MONTHS,
    threshold: float = PSI_ALERT_THRESHOLD,
) -> DriftReport:
    """
    DB-backed entry point. Loads reference + current feature frames and
    returns a DriftReport. Safe to call when no rows exist — returns a
    report with empty features list rather than raising.
    """
    if not current_months:
        return DriftReport(
            reference_months=[],
            current_months=[],
            n_reference_rows=0,
            n_current_rows=0,
            threshold=threshold,
        )
    feats = list(features or DEFAULT_FEATURES)
    reference_months = derive_reference_window(current_months, reference_window_months)
    ref_df = _load_feature_frame(reference_months, feats)
    cur_df = _load_feature_frame(list(current_months), feats)
    started = datetime.utcnow()
    report = compare_frames(
        ref_df,
        cur_df,
        feats,
        threshold=threshold,
        reference_months=reference_months,
        current_months=list(current_months),
    )
    log.info(
        "drift.computed",
        elapsed_ms=int((datetime.utcnow() - started).total_seconds() * 1000),
        n_features=len(report.features),
        n_drifted=len(report.drifted_features),
        n_reference_rows=report.n_reference_rows,
        n_current_rows=report.n_current_rows,
    )
    return report
