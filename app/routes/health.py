"""
Health and readiness endpoints.

  GET /health  -> liveness only (process is up)
  GET /ready   -> readiness: DB reachable + MLflow reachable
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter
from sqlalchemy import text

from accent_fleet.config import settings
from accent_fleet.db.engine import get_engine
from app import __version__

# Operational probe router — never versioned (k8s reads these paths).
router = APIRouter(tags=["meta"])
logger = logging.getLogger("accent_fleet.api.health")


@router.get("/health")
def health() -> dict[str, str]:
    """Liveness probe. Returns 200 as long as the process is responsive."""
    return {"status": "ok", "version": __version__}


@router.get("/ready")
def ready() -> dict[str, object]:
    """Readiness probe. Verifies DB + MLflow reachability."""
    s = settings()
    checks: dict[str, object] = {"db": "unknown", "mlflow": "unknown"}

    # DB check — cheap SELECT 1 against the configured Postgres
    # (works for both Azure-hosted and local docker Postgres).
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as exc:  # noqa: BLE001 — surface any failure mode
        logger.warning("db readiness failed: %s", exc)
        checks["db"] = f"error: {type(exc).__name__}"

    # MLflow check — GET /health on the tracking server.
    try:
        url = f"{s.mlflow_tracking_uri.rstrip('/')}/health"
        r = httpx.get(url, timeout=2.0)
        checks["mlflow"] = "ok" if r.status_code == 200 else f"http_{r.status_code}"
    except Exception as exc:  # noqa: BLE001
        logger.warning("mlflow readiness failed: %s", exc)
        checks["mlflow"] = f"error: {type(exc).__name__}"

    healthy = all(v == "ok" for v in checks.values())
    return {"status": "ok" if healthy else "degraded", "checks": checks}
