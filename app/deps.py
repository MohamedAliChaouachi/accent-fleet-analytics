"""
FastAPI dependency providers.

These wrap the shared singletons stored on `app.state` so route handlers
can take them as typed parameters instead of reaching into globals.
"""

from __future__ import annotations

from collections.abc import Iterator

from fastapi import Depends, Request
from sqlalchemy.engine import Connection

from accent_fleet.db.engine import get_engine
from accent_fleet.features.risk_score import RiskScorer
from accent_fleet.ml.inference import ClusterPredictor


def get_db() -> Iterator[Connection]:
    """Yield a short-lived DB connection per request, inside a transaction
    that auto-commits on success and auto-rolls-back on exception.

    SQLAlchemy 2.x autobegins a transaction on the first execute() anyway;
    using `engine.begin()` makes the boundaries explicit so routes don't
    need their own `with conn.begin():` blocks (those would now raise
    "transaction already started"). Treating each request as one
    transaction is the right granularity for our auth writes (login
    inserts a refresh token AND updates last_login_at — they should
    commit together)."""
    engine = get_engine()
    with engine.begin() as conn:
        yield conn


def get_risk_scorer_dep(request: Request) -> RiskScorer:
    return request.app.state.risk_scorer


def get_cluster_predictor_dep(request: Request) -> ClusterPredictor:
    return request.app.state.cluster_predictor


# Re-exported for routes
DbDep = Depends(get_db)
RiskScorerDep = Depends(get_risk_scorer_dep)
ClusterPredictorDep = Depends(get_cluster_predictor_dep)
