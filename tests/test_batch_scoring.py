"""
Tests for accent_fleet.ml.batch_scoring and the matching SQL DDL.

These are unit tests — no live Postgres required. We:

  1. Statically parse `sql/27_fact_device_cluster_assignment.sql` AND
     `sql/28_fact_device_risk_score.sql` to assert each declares the
     expected table + indexes (catches a typo in DDL).
  2. Monkeypatch the engine + predictor singletons inside batch_scoring
     so we can verify both upsert paths:
        - cluster:  delete-then-insert, idempotency on rerun, no-model branch.
        - risk:     same shape, plus the per-tenant skip path where a
                    TenantModelMissing exception turns into a counted row
                    drop instead of an aborted batch.

The v0.6 IF rewrite renamed the per-process singletons and the upsert
helpers — see batch_scoring.py for the rationale (clustering and risk
need independently loadable predictors). These tests pin both new names.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import pytest

from accent_fleet.config import SQL_DIR
from accent_fleet.ml import batch_scoring
from accent_fleet.ml.inference import TenantModelMissing


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


def test_risk_score_ddl_present() -> None:
    """
    28_*.sql is the v0.6 risk fact — must declare the table, the (tenant,
    device, month) PK, the model_version + tenant indexes the dashboard
    queries use, and the risk_score/risk_category columns the dashboard
    + the compat view (v_device_risk_profile) read.
    """
    sql = (SQL_DIR / "28_fact_device_risk_score.sql").read_text(encoding="utf-8")

    assert "marts.fact_device_risk_score" in sql
    assert "PRIMARY KEY (tenant_id, device_id, year_month)" in sql

    # Provenance columns + the two business columns the dashboard / compat
    # view read. If a refactor renames these, the dashboard breaks.
    for col in (
        "risk_score", "risk_category",
        "model_version", "model_source", "_etl_run_id",
    ):
        assert col in sql, f"risk DDL is missing {col}"


def test_validation_suite_has_cluster_checks() -> None:
    """V9 + V10 must exist so a stale assignment table fails CI."""
    sql = (SQL_DIR / "99_validation_suite.sql").read_text(encoding="utf-8")
    assert "V9_cluster_assignment_sanity" in sql
    assert "V10_cluster_assignment_ref_integrity" in sql


# ---------------------------------------------------------------------------
# 2. Common fakes
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


# Shared 13-feature order — mirrors FEATURES in train_risk.py so both
# predictors line up with what _load_features will select.
_FEATURE_ORDER = [
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


class FakeClusterPredictor:
    """A cluster predictor that's already loaded with a deterministic kmeans."""

    def __init__(self) -> None:
        self.feature_order = list(_FEATURE_ORDER)
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
        return np.array([1 if row[0] > self.threshold else 0 for row in X], dtype=int)

    def transform(self, X):  # noqa: N803
        # Two centroids; distance 0 to the assigned one, 9.9 to the other.
        rows = []
        for row in X:
            picked = 1 if row[0] > self.threshold else 0
            rows.append([9.9, 9.9] if picked == 1 else [0.0, 9.9])
        return np.array(rows, dtype=float)


class FakeRiskPredictor:
    """
    A RiskPredictor stand-in for the score_risk_partitions tests.

    Tenants in ``known_tenants`` predict deterministically (score = first
    feature, category = "low"); any other tenant_id raises
    TenantModelMissing — the trigger the batch loop turns into a counted
    skip rather than an error.
    """

    def __init__(self, known_tenants: set[int] | None = None) -> None:
        self.feature_order = list(_FEATURE_ORDER)
        self.model_version = "risk-fake-1"
        self.source = "test-risk"
        self.known_tenants = known_tenants if known_tenants is not None else {235}

    def ensure_loaded(self) -> None:
        return None

    def predict_batch(self, *, tenant_id: int, features_df: pd.DataFrame):
        if tenant_id not in self.known_tenants:
            raise TenantModelMissing(f"no model for tenant {tenant_id}")
        # Deterministic: score is the first feature value; category fixed.
        scores = features_df.iloc[:, 0].to_numpy(dtype=float)
        labels = np.array(["low"] * len(features_df), dtype=object)
        return scores, labels


@pytest.fixture
def sample_features() -> pd.DataFrame:
    """Two devices in 2025-01: one safe (cluster 0), one aggressive (cluster 1)."""
    base = {col: [0.0, 0.0] for col in _FEATURE_ORDER}
    base["overspeed_per_100km"] = [1.0, 12.0]  # row 0 -> cluster 0, row 1 -> cluster 1
    df = pd.DataFrame(base)
    df.insert(0, "tenant_id", [235, 235])
    df.insert(1, "device_id", [1001, 1002])
    df.insert(2, "year_month", ["2025-01", "2025-01"])
    return df


@pytest.fixture
def risk_features_two_tenants() -> pd.DataFrame:
    """
    Three rows across two tenants — one known, one unknown. Used to
    verify the per-tenant skip path in score_risk_partitions.
    """
    base = {col: [0.0, 0.0, 0.0] for col in _FEATURE_ORDER}
    base["overspeed_per_100km"] = [3.0, 7.5, 11.0]
    df = pd.DataFrame(base)
    df.insert(0, "tenant_id", [235, 235, 999])  # 999 has no model
    df.insert(1, "device_id", [1001, 1002, 1003])
    df.insert(2, "year_month", ["2025-01", "2025-01", "2025-01"])
    return df


# ---------------------------------------------------------------------------
# 3. Cluster path
# ---------------------------------------------------------------------------
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

    monkeypatch.setattr(batch_scoring, "_CLUSTER_PREDICTOR", _Broken())
    result = batch_scoring.score_partitions(["2025-01"], run_id=1)
    assert result.rows_scored == 0
    assert result.skipped_reason and result.skipped_reason.startswith("no_model")


def test_score_partitions_writes_one_row_per_device(monkeypatch, sample_features):
    """Happy path: every input row produces one INSERTed dict."""
    monkeypatch.setattr(batch_scoring, "_CLUSTER_PREDICTOR", FakeClusterPredictor())
    monkeypatch.setattr(batch_scoring, "transaction", fake_transaction)

    captured: dict[str, Any] = {}

    def fake_load(months, feature_order, *, include_tenant=False):
        return sample_features

    monkeypatch.setattr(batch_scoring, "_load_features", fake_load)

    def spy(rows, months, run_id):
        captured["rows"] = list(rows)
        captured["months"] = list(months)
        return len(captured["rows"])

    monkeypatch.setattr(batch_scoring, "_upsert_cluster_rows", spy)

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


def test_cluster_upsert_deletes_before_inserting(monkeypatch):
    """_upsert_cluster_rows must DELETE the target months even when given zero rows.

    This is the idempotency contract: rerunning batch scoring on the same
    months replaces all prior rows. We assert the DELETE happens unconditionally
    and the INSERT is skipped when there are no rows.
    """
    conn = FakeConnection()

    @contextmanager
    def fake_tx():
        yield conn

    monkeypatch.setattr(batch_scoring, "transaction", fake_tx)

    n = batch_scoring._upsert_cluster_rows([], ["2025-01"], run_id=7)

    assert n == 0
    assert len(conn.executions) == 1, "with no rows we expect DELETE only"
    sql, params = conn.executions[0]
    assert "DELETE" in sql.upper()
    assert params == {"months": ["2025-01"]}


def test_cluster_upsert_round_trip_with_rows(monkeypatch):
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
    n = batch_scoring._upsert_cluster_rows(rows, ["2025-01"], run_id=5)

    assert n == 1
    assert len(conn.executions) == 2
    assert "DELETE" in conn.executions[0][0].upper()
    assert "INSERT INTO marts.fact_device_cluster_assignment" in conn.executions[1][0]
    # Insert receives the row dicts list verbatim.
    assert conn.executions[1][1] == rows


# ---------------------------------------------------------------------------
# 4. Risk path
# ---------------------------------------------------------------------------
def test_score_risk_partitions_no_touched_months_is_noop():
    """Empty input → no DB hit, no model load, skipped_reason set."""
    result = batch_scoring.score_risk_partitions([], run_id=99)
    assert result.rows_scored == 0
    assert result.skipped_reason == "no_touched_months"


def test_score_risk_partitions_skips_when_model_unavailable(monkeypatch):
    """
    A risk predictor that fails to load → ScoreResult with skipped_reason,
    no exception bubbling up. Same posture as the cluster side: the flow
    keeps moving when the artifact isn't ready yet.
    """

    class _Broken:
        feature_order = list(_FEATURE_ORDER)
        model_version = "unloaded"
        source = "none"

        def ensure_loaded(self) -> None:
            raise RuntimeError("no risk model")

    monkeypatch.setattr(batch_scoring, "_RISK_PREDICTOR", _Broken())
    result = batch_scoring.score_risk_partitions(["2025-01"], run_id=1)
    assert result.rows_scored == 0
    assert result.skipped_reason and result.skipped_reason.startswith("no_model")


def test_score_risk_partitions_writes_per_tenant_rows(
    monkeypatch, risk_features_two_tenants
):
    """
    Happy path with two tenants in the input — tenant 235 has a model,
    tenant 999 doesn't.

    Contract:
      * Two rows written (the two tenant-235 devices).
      * skipped_tenant_rows[999] == 1 (the unmodeled tenant's row count).
      * Every written row carries the model_version + risk_category set
        by the fake predictor.
    """
    monkeypatch.setattr(
        batch_scoring, "_RISK_PREDICTOR", FakeRiskPredictor(known_tenants={235})
    )

    captured: dict[str, Any] = {}

    def fake_load(months, feature_order, *, include_tenant=False):
        # The risk path passes include_tenant=True — assert that here so
        # a refactor that drops it gets caught.
        assert include_tenant is True
        return risk_features_two_tenants

    monkeypatch.setattr(batch_scoring, "_load_features", fake_load)

    def spy(rows, months, run_id):
        captured["rows"] = list(rows)
        captured["months"] = list(months)
        return len(captured["rows"])

    monkeypatch.setattr(batch_scoring, "_upsert_risk_rows", spy)

    result = batch_scoring.score_risk_partitions(["2025-01"], run_id=42)

    assert result.rows_scored == 2
    assert result.skipped_reason is None
    assert result.skipped_tenant_rows == {999: 1}
    rows = captured["rows"]
    assert len(rows) == 2
    for r in rows:
        assert r["tenant_id"] == 235
        assert r["model_version"] == "risk-fake-1"
        assert r["model_source"] == "test-risk"
        assert r["etl_run_id"] == 42
        assert r["year_month"] == "2025-01"
        assert r["risk_category"] == "low"
    # Scores match what the fake returned (= overspeed_per_100km).
    assert rows[0]["risk_score"] == pytest.approx(3.0)
    assert rows[1]["risk_score"] == pytest.approx(7.5)


def test_risk_upsert_deletes_before_inserting(monkeypatch):
    """
    Risk path mirrors the cluster idempotency contract: empty input still
    DELETEs the target months. Catches a regression where someone "optimises"
    the empty case by short-circuiting before the delete — which would
    silently keep stale rows from a previous run alive in the mart.
    """
    conn = FakeConnection()

    @contextmanager
    def fake_tx():
        yield conn

    monkeypatch.setattr(batch_scoring, "transaction", fake_tx)

    n = batch_scoring._upsert_risk_rows([], ["2025-01"], run_id=7)

    assert n == 0
    assert len(conn.executions) == 1
    sql, params = conn.executions[0]
    assert "DELETE" in sql.upper()
    assert "marts.fact_device_risk_score" in sql
    assert params == {"months": ["2025-01"]}


def test_risk_upsert_round_trip_with_rows(monkeypatch):
    """DELETE first, INSERT into fact_device_risk_score with the row list verbatim."""
    conn = FakeConnection()

    @contextmanager
    def fake_tx():
        yield conn

    monkeypatch.setattr(batch_scoring, "transaction", fake_tx)

    rows = [
        {
            "tenant_id": 235, "device_id": 1, "year_month": "2025-01",
            "risk_score": 42.5, "risk_category": "moderate",
            "model_version": "v1", "model_source": "test", "etl_run_id": 5,
        },
    ]
    n = batch_scoring._upsert_risk_rows(rows, ["2025-01"], run_id=5)

    assert n == 1
    assert len(conn.executions) == 2
    assert "DELETE" in conn.executions[0][0].upper()
    assert "INSERT INTO marts.fact_device_risk_score" in conn.executions[1][0]
    assert conn.executions[1][1] == rows
