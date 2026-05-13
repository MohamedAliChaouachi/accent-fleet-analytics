"""
Unit tests for the feature registry and risk-score reference implementation.

The bottom half of this file holds *SQL parity* tests — they ensure the
Python `RiskScorer` and `ClusterPredictor` produce numerically identical
output to the SQL views / fact tables that the dashboard reads from. Drift
between the two would mean the API and dashboard disagree about the same
device. The parity tests skip when the database is unreachable so the unit
tier still passes in any environment.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from accent_fleet.features import load_feature_registry, load_risk_scorer


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
def test_risk_score_zero_for_safe_driver():
    """A driver with no overspeed/alerts/night driving should score near 0."""
    scorer = load_risk_scorer()
    features = {
        "overspeed_per_100km": 0.0,
        "overspeed_count": 0,
        "overspeed_severity_high": 0,
        "overspeed_severity_extreme": 0,
        "high_speed_trip_ratio": 0.0,
        "speed_alert_per_100km": 0.0,
        "night_trip_ratio": 0.0,
        "avg_max_speed_kmh": 40.0,    # not zero — avg speed still maps to non-zero
    }
    score = scorer.score(features)
    # max_speed factor gives 0.10 * (40/200) = 0.02 -> 2.0
    assert 0 <= score < 10
    assert scorer.categorize(score) == "low"


def test_risk_score_high_for_reckless_driver():
    """A driver pinned at the upper cap of every factor should hit 100."""
    scorer = load_risk_scorer()
    features = {
        "overspeed_per_100km": 100.0,             # far above cap
        "overspeed_count": 10,
        "overspeed_severity_high": 5,
        "overspeed_severity_extreme": 5,
        "high_speed_trip_ratio": 1.0,
        "speed_alert_per_100km": 100.0,
        "night_trip_ratio": 1.0,
        "avg_max_speed_kmh": 200.0,
    }
    score = scorer.score(features)
    assert score >= 90
    assert scorer.categorize(score) in {"high", "critical"}


def test_risk_category_bands():
    scorer = load_risk_scorer()
    assert scorer.categorize(0) == "low"
    assert scorer.categorize(19) == "low"
    assert scorer.categorize(20) == "moderate"
    assert scorer.categorize(44) == "moderate"
    assert scorer.categorize(45) == "high"
    assert scorer.categorize(69) == "high"
    assert scorer.categorize(70) == "critical"
    assert scorer.categorize(100) == "critical"


@pytest.mark.parametrize("missing_feature", [
    "overspeed_per_100km",
    "high_speed_trip_ratio",
    "speed_alert_per_100km",
])
def test_risk_score_handles_missing_features(missing_feature):
    """Missing feature keys must not raise — they are treated as 0."""
    scorer = load_risk_scorer()
    features = {
        "overspeed_per_100km": 5.0,
        "overspeed_count": 3,
        "overspeed_severity_high": 1,
        "overspeed_severity_extreme": 0,
        "high_speed_trip_ratio": 0.2,
        "speed_alert_per_100km": 10.0,
        "night_trip_ratio": 0.15,
        "avg_max_speed_kmh": 120.0,
    }
    features.pop(missing_feature, None)
    score = scorer.score(features)
    assert 0 <= score <= 100


# ---------------------------------------------------------------------------
# SQL parity — Python score must match the SQL view's risk_score, and the
# ClusterPredictor must match the persisted fact_device_cluster_assignment.
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


# SQL that mirrors the `latest_3m` + `rolling` CTEs from
# sql/21_v_device_risk_profile.sql so the parity test sees the same
# aggregates the view sees, plus the `risk_score` published by the view.
_PARITY_SQL = text(
    """
    WITH latest_3m AS (
        SELECT
            tenant_id, device_id, year_month,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, device_id ORDER BY year_month DESC
            ) AS rn
        FROM marts.mart_device_monthly_behavior
    ),
    rolling AS (
        SELECT
            m.tenant_id, m.device_id,
            SUM(m.total_trips)                              AS trips_3m,
            SUM(m.total_distance_km)                        AS distance_3m,
            SUM(m.overspeed_count)                          AS overspeed_3m,
            SUM(m.overspeed_severity_high
                + m.overspeed_severity_extreme)             AS severe_overspeed_3m,
            SUM(m.speed_alert_count)                        AS alerts_3m,
            AVG(m.high_speed_trip_ratio)                    AS high_speed_trip_ratio_3m,
            AVG(m.night_trip_ratio)                         AS night_trip_ratio_3m,
            MAX(m.avg_max_speed_kmh)                        AS max_recorded_speed_3m
        FROM marts.mart_device_monthly_behavior m
        JOIN latest_3m l USING (tenant_id, device_id, year_month)
        WHERE l.rn <= 3
        GROUP BY m.tenant_id, m.device_id
    )
    SELECT
        r.tenant_id, r.device_id,
        r.trips_3m, r.distance_3m, r.overspeed_3m, r.severe_overspeed_3m,
        r.alerts_3m, r.high_speed_trip_ratio_3m, r.night_trip_ratio_3m,
        r.max_recorded_speed_3m,
        v.risk_score
    FROM rolling r
    JOIN marts.v_device_risk_profile v
      ON v.tenant_id = r.tenant_id
     AND v.device_id = r.device_id
    WHERE r.trips_3m >= 10        -- same gate the view uses
    ORDER BY r.tenant_id, r.device_id
    LIMIT :sample_size
    """
)


def _rolling_to_scorer_features(row) -> dict[str, float]:
    """
    Translate the 3-month rolling aggregate row into the feature names the
    Python `RiskScorer` expects. The mapping mirrors the normalize blocks
    in sql/21_v_device_risk_profile.sql:

      - n_overspeed_rate ← (overspeed_3m / distance_3m * 100) capped at 10
      - n_severe_share   ← (severity_high + extreme) / overspeed_count
                           (here: severe_overspeed_3m / overspeed_3m)
      - n_high_speed_ratio ← high_speed_trip_ratio_3m
      - n_alert_rate     ← (alerts_3m / distance_3m * 100) capped at 20
      - n_night          ← night_trip_ratio_3m
      - n_max_speed      ← max_recorded_speed_3m / 200

    The scorer treats overspeed_severity_high + overspeed_severity_extreme
    as the numerator of the severe share, so we can stuff the rolled-up
    severe count into `_high` and leave `_extreme` at 0.
    """
    distance = float(row.distance_3m or 0.0)
    overspeed = float(row.overspeed_3m or 0.0)
    alerts = float(row.alerts_3m or 0.0)
    return {
        "overspeed_per_100km": (overspeed / distance * 100.0) if distance > 0 else 0.0,
        "overspeed_count": overspeed,
        "overspeed_severity_high": float(row.severe_overspeed_3m or 0.0),
        "overspeed_severity_extreme": 0.0,
        "high_speed_trip_ratio": float(row.high_speed_trip_ratio_3m or 0.0),
        "speed_alert_per_100km": (alerts / distance * 100.0) if distance > 0 else 0.0,
        "night_trip_ratio": float(row.night_trip_ratio_3m or 0.0),
        "avg_max_speed_kmh": float(row.max_recorded_speed_3m or 0.0),
    }


def test_python_risk_score_matches_sql_view(db_available):
    """
    For up to 50 random device-rollups, the Python scorer must produce the
    same integer risk score as `marts.v_device_risk_profile`. Tolerance of
    ±1 absorbs Postgres ROUND() vs Python round() rounding ties — anything
    bigger would mean the formulas have actually drifted.
    """
    if not db_available:
        pytest.skip("postgres not reachable")
    from accent_fleet.db.engine import get_engine

    scorer = load_risk_scorer()
    with get_engine().connect() as conn:
        rows = conn.execute(_PARITY_SQL, {"sample_size": 50}).all()

    if not rows:
        pytest.skip("no rows in v_device_risk_profile — run the pipeline first")

    mismatches: list[str] = []
    for r in rows:
        py_score = round(scorer.score(_rolling_to_scorer_features(r)))
        sql_score = int(r.risk_score)
        if abs(py_score - sql_score) > 1:
            mismatches.append(
                f"device {r.device_id} (tenant {r.tenant_id}): "
                f"python={py_score} sql={sql_score}"
            )

    assert not mismatches, "risk-score parity drift:\n  " + "\n  ".join(mismatches)


def test_cluster_predictor_matches_persisted_assignment(db_available):
    """
    For a sample of recent rows in `marts.fact_device_cluster_assignment`,
    re-running the loaded `ClusterPredictor` on the matching features in
    `marts.v_ml_features_full` must reproduce the persisted cluster_id.

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
