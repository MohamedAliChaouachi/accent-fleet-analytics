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

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from accent_fleet.config import settings
from accent_fleet.ml.inference import ClusterPredictor, get_risk_scorer

from app import __version__
from app.routes import admin, devices, health, score

logger = logging.getLogger("accent_fleet.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Warm up shared singletons before the first request."""
    s = settings()
    logger.info(
        "starting api version=%s pg_host=%s mlflow=%s",
        __version__, s.pg_host, s.mlflow_tracking_uri,
    )

    # RiskScorer is pure-Python — load eagerly so misconfiguration fails fast.
    app.state.risk_scorer = get_risk_scorer()

    # ClusterPredictor depends on MLflow being reachable AND a Production
    # model existing. Both may not be true on a fresh stack — lazy-load.
    app.state.cluster_predictor = ClusterPredictor()
    yield
    logger.info("api shutting down")


app = FastAPI(
    title="Accent Fleet Analytics API",
    version=__version__,
    description=(
        "Risk + clustering scoring service for fleet device-month features. "
        "Pairs with the Streamlit dashboard and the Prefect batch pipeline."
    ),
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(score.router)
app.include_router(devices.router)
app.include_router(admin.router)


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    return {
        "service": "accent-fleet-api",
        "version": __version__,
        "docs": "/docs",
        "health": "/health",
    }
