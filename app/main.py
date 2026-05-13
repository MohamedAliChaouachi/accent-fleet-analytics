"""
FastAPI application entrypoint.

Boots the scoring service:
  - Loads RiskScorer (pure-Python, YAML-driven) at import time.
  - Lazily loads ClusterPredictor on first request (MLflow may not yet have
    a Production model when the service starts on a fresh stack).
  - Exposes /health, /score/risk, /score/cluster, /devices/* routes.

Run locally:
    uvicorn app.main:app --reload
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from accent_fleet.config import settings
from accent_fleet.ml.inference import ClusterPredictor, get_risk_scorer
from accent_fleet.observability import setup_logging
from app import __version__
from app.auth.admin_routes import router as auth_admin_router
from app.auth.middleware import AuthMiddleware
from app.auth.routes import router as auth_router
from app.middleware import MetricsMiddleware
from app.routes import admin, devices, health, metrics, score
from app.versioning import LEGACY_SUNSET_HUMAN, include_versioned_router

# Configure structured logging once at import time so anything that runs
# before lifespan (e.g. eager singletons in routes) still gets JSON output.
setup_logging()
logger = structlog.get_logger("accent_fleet.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Warm up shared singletons before the first request."""
    s = settings()
    logger.info(
        "api.starting",
        version=__version__,
        pg_host=s.pg_host,
        mlflow=s.mlflow_tracking_uri,
    )

    # RiskScorer is pure-Python — load eagerly so misconfiguration fails fast.
    app.state.risk_scorer = get_risk_scorer()

    # ClusterPredictor depends on MLflow being reachable AND a Production
    # model existing. Both may not be true on a fresh stack — lazy-load.
    app.state.cluster_predictor = ClusterPredictor()
    yield
    logger.info("api.shutting_down")


app = FastAPI(
    title="Accent Fleet Analytics API",
    version=__version__,
    description=(
        "Risk + clustering scoring service for fleet device-month features. "
        "Pairs with the Streamlit dashboard and the Prefect batch pipeline."
    ),
    lifespan=lifespan,
)

# Order matters: middleware wraps requests outside-in, so MetricsMiddleware
# being added last means it's the outermost wrapper and sees the FINAL
# response status (after FastAPI's exception handlers convert errors).
#
# AuthMiddleware is added BEFORE MetricsMiddleware in source order,
# meaning at runtime it runs inside MetricsMiddleware — that's the
# correct order: a 401 from AuthMiddleware should still be observed
# by metrics + access log. See docs/auth_design.md §7.
app.add_middleware(AuthMiddleware)
app.add_middleware(MetricsMiddleware)

# Operational endpoints — never versioned. k8s probes, Prometheus scrapers,
# and uptime checks read these paths from configuration; renaming them
# imposes coordinated changes on infrastructure for zero behavioural gain.
app.include_router(health.router)
app.include_router(metrics.router)

# Business routers — mounted under /v1 (canonical, in OpenAPI schema) and
# again at the legacy bare path (hidden from schema, Deprecation header
# stamped on every response). See app/versioning.py for the policy.
include_versioned_router(app, score.router)
include_versioned_router(app, devices.router)
include_versioned_router(app, admin.router)

# Auth routers — same versioning policy. /v1/auth/* canonical, /auth/*
# legacy (hidden, deprecation headers). The login and refresh paths are
# exempt from JWT enforcement (see app/auth/middleware.py _EXEMPT_PATHS)
# so unauthenticated clients can still acquire the first token pair.
include_versioned_router(app, auth_router)
include_versioned_router(app, auth_admin_router)


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    return {
        "service": "accent-fleet-api",
        "version": __version__,
        "api_version": "v1",
        "docs": "/docs",
        "health": "/health",
        "legacy_sunset": LEGACY_SUNSET_HUMAN,
    }
