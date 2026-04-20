"""
Cleaning rule engine.

Reads config/cleaning_rules.yaml and applies rules in two modes:

1. **SQL mode** — rules are rendered into WHERE clauses and injected into
   incremental fact loaders. This is what the SQL files under /sql already
   do inline; the engine exposes the rule metadata for logging and testing.

2. **Polars mode** — for streaming, each message is validated against the
   same rules using a Polars expression. Same source-of-truth YAML, two
   runtimes, no drift.

Each rule has one of three actions:

- `reject`   — filter the row out, optionally write to quarantine.
- `clamp`    — cap a numeric value at a threshold.
- `nullify`  — set a column to NULL when outside bounds (keeps the row).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import polars as pl

from accent_fleet.config import load_cleaning_rules


@dataclass
class RuleResult:
    """Counters returned after applying the engine to a DataFrame."""

    total_in: int = 0
    total_out: int = 0
    rejected_by_rule: dict[str, int] = field(default_factory=dict)
    clamped_by_rule: dict[str, int] = field(default_factory=dict)
    nullified_by_rule: dict[str, int] = field(default_factory=dict)

    @property
    def total_rejected(self) -> int:
        return sum(self.rejected_by_rule.values())


@dataclass
class CleaningRule:
    """One rule from cleaning_rules.yaml."""

    id: str
    name: str
    severity: str
    targets: list[str]
    condition: str                         # SQL fragment, {time_column} is templated
    rationale: str
    action: str                            # reject | clamp | nullify
    enabled: bool = True
    time_column: dict[str, str] | None = None
    clamp_column: str | None = None
    clamp_max: float | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "CleaningRule":
        return cls(
            id=d["id"],
            name=d["name"],
            severity=d["severity"],
            targets=d["targets"],
            condition=d.get("condition", "TRUE"),
            rationale=d.get("rationale", ""),
            action=d["action"],
            enabled=d.get("enabled", True),
            time_column=d.get("time_column"),
            clamp_column=d.get("clamp_column"),
            clamp_max=d.get("clamp_max"),
        )


class RuleEngine:
    """Apply cleaning rules to a Polars DataFrame, keyed on source table."""

    def __init__(self, rules: list[CleaningRule]) -> None:
        self.rules = rules

    # ------------------------------------------------------------------
    def rules_for(self, table: str) -> list[CleaningRule]:
        return [r for r in self.rules if r.enabled and table in r.targets]

    # ------------------------------------------------------------------
    def apply(self, df: pl.DataFrame, *, table: str) -> tuple[pl.DataFrame, RuleResult]:
        """
        Run every enabled rule that targets `table` against `df`.

        Returns (cleaned_df, result_counters).
        """
        result = RuleResult(total_in=len(df))
        out = df

        for rule in self.rules_for(table):
            if rule.action == "reject":
                before = len(out)
                out = self._apply_reject(out, rule, table)
                removed = before - len(out)
                if removed:
                    result.rejected_by_rule[rule.id] = removed
            elif rule.action == "clamp":
                clamped = self._apply_clamp(out, rule)
                if clamped > 0:
                    result.clamped_by_rule[rule.id] = clamped
                out = self._clamp_column(out, rule)
            elif rule.action == "nullify":
                nulled = self._apply_nullify_count(out, rule)
                if nulled > 0:
                    result.nullified_by_rule[rule.id] = nulled
                out = self._nullify_column(out, rule)

        result.total_out = len(out)
        return out, result

    # ------------------------------------------------------------------
    # Rule-specific Polars implementations.
    # Each one mirrors the SQL condition in cleaning_rules.yaml.
    # ------------------------------------------------------------------

    def _apply_reject(
        self, df: pl.DataFrame, rule: CleaningRule, table: str
    ) -> pl.DataFrame:
        """Reject rows that do NOT satisfy the rule's VALID condition."""
        match rule.id:
            case "C1":
                col = rule.time_column[table] if rule.time_column else "event_time"
                return df.filter(pl.col(col) >= datetime(2019, 10, 1))
            case "C2":
                return df.filter(pl.col("path_duration") > 0)
            case "C3":
                return df.filter(pl.col("distance_driven") > 0)
            case "C6":
                return df.filter(
                    (pl.col("stop_duration") > 0) & (pl.col("stop_duration") < 31_536_000)
                )
            case "C7":
                return df.filter(pl.col("vehicule_id").is_not_null())
            case _:
                return df

    def _apply_clamp(self, df: pl.DataFrame, rule: CleaningRule) -> int:
        col = rule.clamp_column or "max_speed"
        if col not in df.columns or rule.clamp_max is None:
            return 0
        return int(df.filter(pl.col(col) > rule.clamp_max).height)

    def _clamp_column(self, df: pl.DataFrame, rule: CleaningRule) -> pl.DataFrame:
        col = rule.clamp_column or "max_speed"
        if col not in df.columns or rule.clamp_max is None:
            return df
        return df.with_columns(
            pl.when(pl.col(col) > rule.clamp_max)
              .then(pl.lit(rule.clamp_max))
              .otherwise(pl.col(col))
              .alias(col)
        )

    def _apply_nullify_count(self, df: pl.DataFrame, rule: CleaningRule) -> int:
        """C4: count rows whose fuel_used is outside [0, 500]."""
        if rule.id != "C4" or "fuel_used" not in df.columns:
            return 0
        return int(
            df.filter((pl.col("fuel_used") < 0) | (pl.col("fuel_used") > 500)).height
        )

    def _nullify_column(self, df: pl.DataFrame, rule: CleaningRule) -> pl.DataFrame:
        if rule.id != "C4" or "fuel_used" not in df.columns:
            return df
        return df.with_columns(
            pl.when((pl.col("fuel_used") < 0) | (pl.col("fuel_used") > 500))
              .then(None)
              .otherwise(pl.col("fuel_used"))
              .alias("fuel_used")
        )


# ---------------------------------------------------------------------------
def load_rule_engine() -> RuleEngine:
    """Build a RuleEngine from the YAML catalog."""
    cfg = load_cleaning_rules()
    rules = [CleaningRule.from_dict(r) for r in cfg["rules"]]
    return RuleEngine(rules)
