"""
Risk score — Python reference implementation.

MUST stay numerically identical to sql/21_v_device_risk_profile.sql.
The unit test tests/test_feature_computation.py pins this invariant by
computing the score both ways on a sample and asserting equality.

Why mirror in Python if the SQL view exists? Because Phase 6 (deployment)
will want to score *online*, outside the database — e.g., scoring a driver
in-flight from a single trip's features. The online scorer reuses the
coefficients loaded from YAML so a weight tweak affects both paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from accent_fleet.config import load_feature_definitions


@dataclass(frozen=True)
class Factor:
    """One weighted factor of the composite score."""

    name: str
    weight: float
    feature: str | None = None      # driver feature name
    formula: str | None = None      # or a derived formula
    normalize_type: str = "linear"
    normalize_max: float | None = None


@dataclass(frozen=True)
class RiskCategory:
    """A risk-category band in the [0, 100] scale."""

    name: str
    min_score: int
    max_score: int

    def contains(self, score: float) -> bool:
        return self.min_score <= score <= self.max_score


class RiskScorer:
    """Compute the 0–100 composite risk score for a device-month row."""

    def __init__(
        self,
        factors: list[Factor],
        categories: list[RiskCategory],
    ) -> None:
        self.factors = factors
        self.categories = categories

    # ------------------------------------------------------------------
    def _normalize(self, factor: Factor, raw: float) -> float:
        """Bring `raw` into [0, 1] per the factor's normalize spec."""
        if factor.normalize_type == "identity":
            return max(0.0, min(1.0, raw))
        if factor.normalize_type == "linear":
            if not factor.normalize_max or factor.normalize_max == 0:
                return 0.0
            return max(0.0, min(1.0, raw / factor.normalize_max))
        raise ValueError(f"Unknown normalize type: {factor.normalize_type}")

    # ------------------------------------------------------------------
    def score(self, features: dict[str, Any]) -> float:
        """
        Compute the score [0, 100] for a single device-month feature vector.

        `features` must contain (at least) the feature names referenced by
        the factors. Missing keys are treated as 0.
        """
        total = 0.0
        for f in self.factors:
            raw = self._raw_value(f, features)
            total += f.weight * self._normalize(f, raw)
        return round(total * 100, 1)

    # ------------------------------------------------------------------
    def _raw_value(self, factor: Factor, features: dict[str, Any]) -> float:
        """Pull or compute the raw factor input from the features dict."""
        if factor.feature:
            return float(features.get(factor.feature) or 0.0)

        # The only formula today is (high+extreme)/total for severe share.
        if factor.formula and "overspeed_severity_high" in factor.formula:
            total = float(features.get("overspeed_count") or 0.0)
            if total == 0:
                return 0.0
            high = float(features.get("overspeed_severity_high") or 0.0)
            extreme = float(features.get("overspeed_severity_extreme") or 0.0)
            return (high + extreme) / total
        return 0.0

    # ------------------------------------------------------------------
    def categorize(self, score: float) -> str:
        for cat in self.categories:
            if cat.contains(score):
                return cat.name
        return "unknown"


# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_risk_scorer() -> RiskScorer:
    """Build the scorer from YAML config."""
    cfg = load_feature_definitions()
    rs_cfg = cfg["risk_score"]

    factors: list[Factor] = []
    for f in rs_cfg["factors"]:
        norm = f.get("normalize", {})
        factors.append(
            Factor(
                name=f["name"],
                weight=float(f["weight"]),
                feature=f.get("feature"),
                formula=f.get("formula"),
                normalize_type=norm.get("type", "linear"),
                normalize_max=norm.get("max"),
            )
        )

    categories = [
        RiskCategory(name=c["name"], min_score=int(c["min"]), max_score=int(c["max"]))
        for c in rs_cfg["categories"]
    ]
    return RiskScorer(factors=factors, categories=categories)
