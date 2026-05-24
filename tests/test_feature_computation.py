"""
Unit tests for the feature registry, plus SQL-side parity tests for the two
ML models that the API serves (clustering + per-tenant risk-score IF).

The bottom half of this file holds *parity tests* — they ensure the Python
``ClusterPredictor`` and ``RiskPredictor`` produce the same output as the
persisted fact tables that the dashboard reads from. Drift between the two
would mean the API and dashboard disagree about the same device.

The parity tests skip when the database is unreachable or when no
Production-stage model is registered yet, so the unit tier still passes in
any environment.

History:
  v0.5 → v0.6 this file used to also test a Python ``RiskScorer`` that
         implemented the legacy deterministic weighted formula, and a
         formula-vs-SQL parity test. Both went away when the risk-score
         pathway switched to the per-tenant Isolation Forest model. The
         registry tests are untouched because the feature catalogue is
         independent of the model that consumes it.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from accent_fleet.features import load_feature_registry


# ---------------------------------------------------------------------------
# Registry tests — independent of the risk model the registry feeds.
# ---------------------------------------------------------------------------
def test_registry_has_expected_groups():
    reg = load_feature_registry()
    assert set(reg.groups()) == {
        "trip_volume_patterns",
        "speed_behavior",
        "overspeed_events",
        "alerts",
        "stops_idling",
        "temporal_patterns",
        "harsh_events",
        "telemetry_engine_idling",
    }


def test_registry_count_matches_spec():
    """The v1 design advertises 35+ features. We expect at least 34."""
    reg = load_feature_registry()
    assert len(reg) >= 34


def test_known_features_present():
    reg = load_feature_registry()
    for name in [
        "total_trips",
        "overspeed_per_100km",
        "p95_max_speed",
        "night_trip_ratio",
    ]:
        assert name in reg


# ---------------------------------------------------------------------------
# SQL parity — Python predictors must match the persisted fact tables.
# These integration tests skip when Postgres is unreachable.
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def db_available() -> bool:
    """True if the configured Postgres is reachable."""
    from accent_fleet.db.engine import get_engine

    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


def test_cluster_predictor_matches_persisted_assignment(db_available):
    """
    For a sample of recent rows in ``marts.fact_device_cluster_assignment``,
    re-running the loaded ``ClusterPredictor`` on the matching features in
    ``marts.v_ml_features_full`` must reproduce the persisted cluster_id.

    Skips when:
      - the DB is unreachable, or
      - no Production model is registered yet, or
      - the fact table is empty (first scoring run hasn't happened).

    A drift here means the running API would label a device differently
    than the dashboard shows. That's the bug this test exists to catch.
    """
    if not db_available:
        pytest.skip("postgres not reachable")
    from accent_fleet.db.engine import get_engine
    from accent_fleet.ml.inference import ClusterPredictor

    predictor = ClusterPredictor()
    try:
        predictor.ensure_loaded()
    except RuntimeError as exc:
        pytest.skip(f"no clustering model available: {exc}")

    # We want to compare against assignments produced by the SAME model
    # version that's currently loaded. Otherwise we'd be comparing the
    # current predictor against rows scored by an older model, which is
    # an expected (not buggy) mismatch.
    with get_engine().connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT a.tenant_id, a.device_id, a.year_month,
                       a.cluster_id, a.model_version
                FROM marts.fact_device_cluster_assignment a
                WHERE a.model_version = :mv
                ORDER BY a.scored_at DESC
                LIMIT 25
                """
            ),
            {"mv": predictor.model_version},
        ).all()
        if not rows:
            pytest.skip(
                "no fact_device_cluster_assignment rows for model_version="
                f"{predictor.model_version!r} — re-run batch scoring"
            )

        # Pull features for those exact partitions in one round-trip.
        keys = [(r.tenant_id, r.device_id, r.year_month) for r in rows]
        feature_cols = ", ".join(predictor.feature_order)
        feat_rows = conn.execute(
            text(
                f"""
                SELECT tenant_id, device_id, year_month, {feature_cols}
                FROM marts.v_ml_features_full
                WHERE (tenant_id, device_id, year_month) IN (
                    SELECT * FROM unnest(
                        CAST(:tids AS int[]),
                        CAST(:dids AS bigint[]),
                        CAST(:yms  AS char(7)[])
                    )
                )
                """
            ),
            {
                "tids": [k[0] for k in keys],
                "dids": [k[1] for k in keys],
                "yms":  [k[2] for k in keys],
            },
        ).all()

    feature_by_key = {
        (f.tenant_id, f.device_id, f.year_month): f._mapping for f in feat_rows
    }

    mismatches: list[str] = []
    for r in rows:
        feats = feature_by_key.get((r.tenant_id, r.device_id, r.year_month))
        if feats is None:
            # Row was scored but the features view no longer has it — the
            # mart may have been rebuilt; skip silently.
            continue
        predicted, _ = predictor.predict(dict(feats))
        if predicted != int(r.cluster_id):
            mismatches.append(
                f"({r.tenant_id},{r.device_id},{r.year_month}): "
                f"python={predicted} persisted={r.cluster_id}"
            )

    assert not mismatches, "cluster-assignment parity drift:\n  " + "\n  ".join(mismatches)


def test_risk_predictor_matches_persisted_fact(db_available):
    """
    For a sample of recent rows in ``marts.fact_device_risk_score``,
    re-running the loaded ``RiskPredictor`` on the matching features in
    ``marts.v_ml_features_full`` must reproduce the persisted risk_score
    and risk_category for the same (tenant, device, year_month).

    Why this matters:
      - The API endpoint ``/v1/score/risk`` and the dashboard both
        ultimately read from `RiskPredictor`-produced numbers — the API
        on the hot path, the dashboard via the fact table. If the loaded
        bundle and the persisted fact rows disagree, two surfaces will
        show two scores for the same device, which is exactly the
        operational bug this test exists to catch.
      - We require an exact match on the integer score (the fact column
        is NUMERIC(5,1); we compare at 0.1 tolerance to absorb the float
        rounding that DB ↔ Python round-trips can introduce).

    Skips when:
      - the DB is unreachable
      - no Production-stage risk model is registered yet
      - the fact table is empty (first risk-scoring run hasn't happened)
    """
    if not db_available:
        pytest.skip("postgres not reachable")
    from accent_fleet.db.engine import get_engine
    from accent_fleet.ml.inference import RiskPredictor, TenantModelMissingError

    predictor = RiskPredictor()
    try:
        predictor.ensure_loaded()
    except RuntimeError as exc:
        pytest.skip(f"no risk model available: {exc}")

    # Compare against rows scored by the same registry version we just
    # loaded — otherwise we'd be comparing the current predictor against
    # historic fact rows scored by a previous version, which is an
    # expected (not buggy) mismatch.
    with get_engine().connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT tenant_id, device_id, year_month,
                       risk_score, risk_category, model_version
                FROM marts.fact_device_risk_score
                WHERE model_version = :mv
                ORDER BY scored_at DESC
                LIMIT 25
                """
            ),
            {"mv": predictor.model_version},
        ).all()
        if not rows:
            pytest.skip(
                "no fact_device_risk_score rows for model_version="
                f"{predictor.model_version!r} — re-run batch risk scoring"
            )

        # Pull features in one round-trip. Same shape as the cluster
        # parity test above, only the feature_order differs (IF features
        # vs. KMeans features — both come from v_ml_features_full).
        keys = [(r.tenant_id, r.device_id, r.year_month) for r in rows]
        feature_cols = ", ".join(predictor.feature_order)
        feat_rows = conn.execute(
            text(
                f"""
                SELECT tenant_id, device_id, year_month, {feature_cols}
                FROM marts.v_ml_features_full
                WHERE (tenant_id, device_id, year_month) IN (
                    SELECT * FROM unnest(
                        CAST(:tids AS int[]),
                        CAST(:dids AS bigint[]),
                        CAST(:yms  AS char(7)[])
                    )
                )
                """
            ),
            {
                "tids": [k[0] for k in keys],
                "dids": [k[1] for k in keys],
                "yms":  [k[2] for k in keys],
            },
        ).all()

    feature_by_key = {
        (f.tenant_id, f.device_id, f.year_month): f._mapping for f in feat_rows
    }

    score_mismatches: list[str] = []
    category_mismatches: list[str] = []
    skipped_tenants: set[int] = set()
    for r in rows:
        feats = feature_by_key.get((r.tenant_id, r.device_id, r.year_month))
        if feats is None:
            # Row was scored but the features view no longer has it — the
            # mart may have been rebuilt since; skip silently.
            continue
        try:
            pred = predictor.predict(int(r.tenant_id), dict(feats))
        except TenantModelMissingError:
            # The bundle in memory doesn't contain this tenant. Can happen
            # if the production version was trained without it but the
            # fact still has older rows. Count once and move on.
            skipped_tenants.add(int(r.tenant_id))
            continue

        # Score parity at 0.1 tolerance — the fact stores NUMERIC(5,1),
        # the predictor returns a Python float; a < 0.1 gap is just
        # float↔decimal round-tripping. Anything larger is real drift.
        if abs(float(pred.score) - float(r.risk_score)) > 0.1:
            score_mismatches.append(
                f"({r.tenant_id},{r.device_id},{r.year_month}): "
                f"python={pred.score:.1f} persisted={float(r.risk_score):.1f}"
            )
        if pred.category != r.risk_category:
            category_mismatches.append(
                f"({r.tenant_id},{r.device_id},{r.year_month}): "
                f"python={pred.category!r} persisted={r.risk_category!r}"
            )

    if skipped_tenants:
        # Informational — pytest still passes as long as the matched rows
        # are clean. Log via pytest's terminal so the human knows why N
        # rows didn't participate.
        print(
            f"[risk parity] skipped {len(skipped_tenants)} tenant(s) absent "
            f"from the loaded bundle: {sorted(skipped_tenants)}"
        )

    assert not score_mismatches, (
        "risk-score parity drift:\n  " + "\n  ".join(score_mismatches)
    )
    assert not category_mismatches, (
        "risk-category parity drift:\n  " + "\n  ".join(category_mismatches)
    )
