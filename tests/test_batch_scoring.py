"""
Tests for accent_fleet.ml.batch_scoring and the matching SQL DDL.

These are unit tests — no live Postgres required. We:

  1. Statically parse `sql/27_fact_device_cluster_assignment.sql` to assert
     it contains the expected table + indexes (catches a typo in DDL).
  2. Monkeypatch the engine + predictor inside batch_scoring so we can
     verify the upsert path: delete-then-insert, idempotency on rerun,
     and the no-model branch.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pytest

from accent_fleet.config import SQL_DIR
from accent_fleet.ml import batch_scoring


# ---------------------------------------------------------------------------
# 1. DDL static checks
# ---------------------------------------------------------------------------
def test_cluster_assignment_ddl_present() -> None:
    """27_*.sql must declare the table + primary key + the three indexes."""
    sql = (SQL_DIR / "27_fact_device_cluster_assignment.sql").read_text(encoding="utf-8")

    assert "marts.fact_device_cluster_assignment" in sql
    assert "PRIMARY KEY (tenant_id, device_id, year_month)" in sql

    # All three indexes the dashboard + validation rely on.
    assert "idx_fact_cluster_assignment_cluster" in sql
    assert "idx_fact_cluster_assignment_tenant_month" in sql
    assert "idx_fact_cluster_assignment_run" in sql

    # Provenance columns the validation suite (V9/V10) checks.
    for col in ("cluster_id", "distance_to_centroid", "model_version", "_etl_run_id"):
        assert col in sql, f"DDL is missing {col}"


def test_validation_suite_has_cluster_checks() -> None:
    """V9 + V10 must exist so a stale assignment table fails CI."""
    sql = (SQL_DIR / "99_validation_suite.sql").read_text(encoding="utf-8")
    assert "V9_cluster_assignment_sanity" in sql
    assert "V10_cluster_assignment_ref_integrity" in sql


# ---------------------------------------------------------------------------
# 2. Upsert path: mocked engine + predictor
# ---------------------------------------------------------------------------
@dataclass
class _Result:
    rowcount: int = 0


class FakeConnection:
    """Mimics sqlalchemy Connection for capture-and-replay tests."""

    def __init__(self) -> None:
        self.executions: list[tuple[str, Any]] = []

    def execute(self, statement: Any, params: Any = None) -> _Result:
        self.executions.append((str(statement), params))
        if isinstance(params, list):
            return _Result(rowcount=len(params))
        return _Result(rowcount=1)


@contextmanager
def fake_transaction():
    """Stand-in for accent_fleet.db.engine.transaction()."""
    yield FakeConnection()


class FakePredictor:
    """A predictor that's already loaded and has a fixed feature order."""

    def __init__(self) -> None:
        self.feature_order = [
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
        ]
        self.model_version = "fake-1"
        self.source = "test"
        self._scaler = _Identity()
        # Tag every row with cluster id = (overspeed_per_100km > 5).
        self._kmeans = _ThresholdKMeans(threshold=5.0)

    def ensure_loaded(self) -> None:
        return None


class _Identity:
    def transform(self, X):  # noqa: N803
        return X


class _ThresholdKMeans:
    """Returns 1 if first feature > threshold, else 0."""

    def __init__(self, threshold: float) -> None:
        self.threshold = threshold

    def predict(self, X):  # noqa: N803
        import numpy as np

        return np.array([1 if row[0] > self.threshold else 0 for row in X], dtype=int)

    def transform(self, X):  # noqa: N803
        import numpy as np

        # Two centroids; distance 0 to the assigned one, 9.9 to the other.
        rows = []
        for row in X:
            picked = 1 if row[0] > self.threshold else 0
            rows.append([9.9, 9.9] if picked == 1 else [0.0, 9.9])
            # Distance to assigned centroid (index `picked`) = 0.0 for class 0,
            # or 9.9 for class 1 — fine for the test, we only check the schema.
        return np.array(rows, dtype=float)


import numpy as np  # noqa: E402  — kept after class so _ThresholdKMeans imports cleanly


@pytest.fixture
def sample_features() -> pd.DataFrame:
    """Two devices in 2025-01: one safe (cluster 0), one aggressive (cluster 1)."""
    base = {col: [0.0, 0.0] for col in FakePredictor().feature_order}
    base["overspeed_per_100km"] = [1.0, 12.0]  # row 0 -> cluster 0, row 1 -> cluster 1
    df = pd.DataFrame(base)
    df.insert(0, "tenant_id", [235, 235])
    df.insert(1, "device_id", [1001, 1002])
    df.insert(2, "year_month", ["2025-01", "2025-01"])
    return df


def test_score_partitions_no_touched_months_is_noop():
    """Empty input -> no DB hit, no model load, skipped_reason set."""
    result = batch_scoring.score_partitions([], run_id=99)
    assert result.rows_scored == 0
    assert result.skipped_reason == "no_touched_months"


def test_score_partitions_skips_when_model_unavailable(monkeypatch):
    """A predictor that fails to load -> ScoreResult with skipped_reason, no exception."""

    class _Broken:
        def ensure_loaded(self) -> None:
            raise RuntimeError("no model")

    monkeypatch.setattr(batch_scoring, "_PREDICTOR", _Broken())
    result = batch_scoring.score_partitions(["2025-01"], run_id=1)
    assert result.rows_scored == 0
    assert result.skipped_reason and result.skipped_reason.startswith("no_model")


def test_score_partitions_writes_one_row_per_device(monkeypatch, sample_features):
    """Happy path: every input row produces one INSERTed dict."""
    monkeypatch.setattr(batch_scoring, "_PREDICTOR", FakePredictor())
    monkeypatch.setattr(batch_scoring, "transaction", fake_transaction)

    # Capture the rows submitted to INSERT.
    captured = {}

    def fake_load(months, feature_order):
        return sample_features

    monkeypatch.setattr(batch_scoring, "_load_features", fake_load)

    # We need to inspect what `_upsert_rows` was called with; patch it to record.
    real_upsert = batch_scoring._upsert_rows

    def spy(rows, months, run_id):
        captured["rows"] = list(rows)
        captured["months"] = list(months)
        return real_upsert(captured["rows"], months, run_id) if False else len(captured["rows"])

    monkeypatch.setattr(batch_scoring, "_upsert_rows", spy)

    result = batch_scoring.score_partitions(["2025-01"], run_id=42)

    assert result.rows_scored == 2
    assert result.skipped_reason is None
    rows = captured["rows"]
    assert len(rows) == 2
    # First device safe (cluster 0), second aggressive (cluster 1).
    assert rows[0]["cluster_id"] == 0
    assert rows[1]["cluster_id"] == 1
    # Provenance is filled.
    for r in rows:
        assert r["model_version"] == "fake-1"
        assert r["model_source"] == "test"
        assert r["etl_run_id"] == 42
        assert r["year_month"] == "2025-01"


def test_upsert_deletes_before_inserting(monkeypatch):
    """_upsert_rows must DELETE the target months even when given zero rows.

    This is the idempotency contract: rerunning batch scoring on the same
    months replaces all prior rows. We assert the DELETE happens unconditionally
    and the INSERT is skipped when there are no rows.
    """
    conn = FakeConnection()

    @contextmanager
    def fake_tx():
        yield conn

    monkeypatch.setattr(batch_scoring, "transaction", fake_tx)

    n = batch_scoring._upsert_rows([], ["2025-01"], run_id=7)

    assert n == 0
    assert len(conn.executions) == 1, "with no rows we expect DELETE only"
    sql, params = conn.executions[0]
    assert "DELETE" in sql.upper()
    assert params == {"months": ["2025-01"]}


def test_upsert_round_trip_with_rows(monkeypatch):
    """With rows present: DELETE first, INSERT second, with the supplied params."""
    conn = FakeConnection()

    @contextmanager
    def fake_tx():
        yield conn

    monkeypatch.setattr(batch_scoring, "transaction", fake_tx)

    rows = [
        {
            "tenant_id": 235, "device_id": 1, "year_month": "2025-01",
            "cluster_id": 0, "distance_to_centroid": 0.1,
            "model_version": "v1", "model_source": "test", "etl_run_id": 5,
        },
    ]
    n = batch_scoring._upsert_rows(rows, ["2025-01"], run_id=5)

    assert n == 1
    assert len(conn.executions) == 2
    assert "DELETE" in conn.executions[0][0].upper()
    assert "INSERT INTO marts.fact_device_cluster_assignment" in conn.executions[1][0]
    # Insert receives the row dicts list verbatim.
    assert conn.executions[1][1] == rows
