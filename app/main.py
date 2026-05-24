"""
FastAPI application entrypoint.

Boots the scoring service:
  - Lazily loads RiskPredictor on first request (per-tenant Isolation Forest
    artifact; MLflow may not yet have a Production model when the service
    starts on a fresh stack).
  - Lazily loads ClusterPredictor on first request (same reasoning).
  - Exposes /health, /score/risk, /score/cluster, /devices/* routes.

Run locally:
    uvicorn app.main:app --reload
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from accent_fleet.config import settings
from accent_fleet.ml.inference import ClusterPredictor, RiskPredictor
from accent_fleet.observability import setup_logging
from app import __version__
from app.ai.routers.ai_query import router as ai_router
from app.auth.admin_routes import router as auth_admin_router
from app.auth.middleware import AuthMiddleware
from app.auth.routes import router as auth_router
from app.middleware import MetricsMiddleware
from app.routes import admin, dashboards, devices, health, metrics, score
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

    # Both predictors depend on MLflow being reachable AND a Production
    # model existing. Neither may be true on a fresh stack, so we
    # construct them in the unloaded state and let the first request
    # trigger ``ensure_loaded()``. /health stays green either way.
    app.state.risk_predictor = RiskPredictor()
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
include_versioned_router(app, dashboards.router)
include_versioned_router(app, admin.router)

# Text2SQL AI assistant — POST /v1/ai/query. JWT-protected like every
# other business router; tenant scoping is enforced from the Principal
# in app/ai/routers/ai_query.py rather than trusted from the body.
include_versioned_router(app, ai_router)

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
