"""
Feature registry.

Until v0.6 this package also exported a deterministic RiskScorer that read
a weighted-formula block from ``config/feature_definitions.yaml``. That
scorer is now retired — the risk score is produced by the per-tenant
Isolation Forest in ``accent_fleet.ml.inference.RiskPredictor`` and the
relevant configuration lives under ``risk_score_model`` in the same YAML.
"""

from accent_fleet.features.registry import FeatureRegistry, load_feature_registry

__all__ = ["FeatureRegistry", "load_feature_registry"]
