"""
Feature registry.

A typed wrapper around config/feature_definitions.yaml so downstream code
doesn't index into raw dicts. The registry is the single catalogue of:

  - feature name
  - feature group
  - source (fact table or derived)
  - aggregation or formula
  - importance tag

Phase 4 modelling code should iterate via `FeatureRegistry.all_features()`
rather than hardcoding column names.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from accent_fleet.config import load_feature_definitions


@dataclass(frozen=True)
class FeatureSpec:
    """One feature as defined in YAML."""

    name: str
    group: str
    source: str
    aggregation: str | None = None
    column: str | None = None
    filter: str | None = None
    formula: str | None = None
    importance: str = "normal"
    quantile: float | None = None


class FeatureRegistry:
    """Read-only catalogue of the 35 features for Project P1."""

    def __init__(self, specs: list[FeatureSpec]) -> None:
        self._by_name: dict[str, FeatureSpec] = {s.name: s for s in specs}
        self._by_group: dict[str, list[FeatureSpec]] = {}
        for s in specs:
            self._by_group.setdefault(s.group, []).append(s)

    # ------------------------------------------------------------------
    def get(self, name: str) -> FeatureSpec:
        return self._by_name[name]

    def all_features(self) -> list[FeatureSpec]:
        return list(self._by_name.values())

    def group(self, name: str) -> list[FeatureSpec]:
        return self._by_group.get(name, [])

    def groups(self) -> list[str]:
        return list(self._by_group.keys())

    def names(self) -> list[str]:
        """Column order matching v_ml_features_driver_behavior (ex-identity)."""
        return [s.name for s in self._by_name.values()]

    def __len__(self) -> int:
        return len(self._by_name)

    def __contains__(self, name: str) -> bool:
        return name in self._by_name


# ---------------------------------------------------------------------------
def _parse_group(group_cfg: dict[str, Any]) -> list[FeatureSpec]:
    out: list[FeatureSpec] = []
    group_name = group_cfg["name"]
    for f in group_cfg["features"]:
        out.append(
            FeatureSpec(
                name=f["name"],
                group=group_name,
                source=f["source"],
                aggregation=f.get("aggregation"),
                column=f.get("column"),
                filter=f.get("filter"),
                formula=f.get("formula"),
                importance=f.get("importance", "normal"),
                quantile=f.get("quantile"),
            )
        )
    return out


@lru_cache(maxsize=1)
def load_feature_registry() -> FeatureRegistry:
    """Build the registry from YAML. Cached — safe to call repeatedly."""
    cfg = load_feature_definitions()
    specs: list[FeatureSpec] = []
    for group in cfg["feature_groups"]:
        specs.extend(_parse_group(group))
    return FeatureRegistry(specs)
