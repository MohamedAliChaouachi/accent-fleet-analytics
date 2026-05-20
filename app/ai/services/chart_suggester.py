"""
Chart-type heuristic.

Pure-function, deterministic. We deliberately don't ask the LLM what
chart to use — the LLM is unreliable about it ("pie chart!" for a
12-month time series is a real failure mode) and a 30-line rules engine
gets the call right ~95% of the time on the shapes we actually return.

Rules, in order. The first rule that matches wins.

  1. ≤ 1 row OR ≥ 5 columns → table (key/value answer or wide detail).
  2. A column reading as time (date / 'YYYY-MM' year_month / fleet_date)
     AND a numeric column → line.
  3. Exactly two columns, one categorical + one numeric, where the
     categorical column reads like a proportion target (risk_category,
     vehicle_class, cluster_id) AND ≤ 8 distinct categories AND the
     numeric column name suggests a share (`pct`, `ratio`, `share`,
     `device_count`) → pie.
  4. Two-or-three columns with at least one numeric column → bar.
  5. Default → table.
"""

from __future__ import annotations

from typing import Any, Literal

ChartType = Literal["line", "bar", "pie", "table"]


# Column-name fragments that signal "this is the time axis". Order
# doesn't matter; we just test membership.
_TIME_HINTS: tuple[str, ...] = (
    "year_month",
    "fleet_date",
    "date",
    "month",
    "day",
    "ts",
    "timestamp",
)

_PROPORTION_HINTS: tuple[str, ...] = (
    "pct",
    "share",
    "ratio",
    "device_count",
    "devices",
)

_PIE_FRIENDLY_CATEGORIES: tuple[str, ...] = (
    "risk_category",
    "vehicle_class",
    "vehicle_mark",
    "cluster_id",
    "category",
)


def suggest(columns: list[str], rows: list[dict[str, Any]]) -> ChartType:
    """Pick a chart type from the result shape. Never raises."""
    if not rows or not columns:
        return "table"
    if len(rows) == 1 or len(columns) >= 5:
        return "table"

    sample = _first_non_null_row(rows)
    numeric_cols = [c for c in columns if _is_numeric(sample.get(c))]
    time_cols = [c for c in columns if any(h in c.lower() for h in _TIME_HINTS)]

    # Rule 2: time series → line
    if time_cols and numeric_cols and len(rows) >= 3:
        return "line"

    # Rule 3: low-cardinality categorical + share → pie
    if len(columns) == 2 and numeric_cols:
        cat_candidates = [c for c in columns if c not in numeric_cols]
        if cat_candidates:
            cat_col = cat_candidates[0]
            distinct = {r.get(cat_col) for r in rows}
            if (
                cat_col.lower() in _PIE_FRIENDLY_CATEGORIES
                and len(distinct) <= 8
                and any(h in numeric_cols[0].lower() for h in _PROPORTION_HINTS)
            ):
                return "pie"

    # Rule 4: categorical + numeric → bar
    if numeric_cols and len(columns) <= 3 and len(rows) <= 50:
        return "bar"

    return "table"


def _first_non_null_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a representative row for type-sniffing.

    Picking row 0 fails when its first value is None for an otherwise
    numeric column. We scan a small prefix and merge non-null entries
    so the type guess is robust to leading nulls.
    """
    merged: dict[str, Any] = {}
    for r in rows[:5]:
        for k, v in r.items():
            if k not in merged or merged[k] is None:
                merged[k] = v
    return merged or rows[0]


def _is_numeric(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):  # bool is a subclass of int; exclude
        return False
    return isinstance(v, (int, float))
