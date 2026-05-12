"""
Admin endpoints.

  POST /admin/reload-model
        Force the cluster predictor to re-read the latest Production model
        from MLflow (or local disk). Use after promoting a new version so the
        running API picks it up without a container restart.

Authentication: every endpoint here requires an `X-API-Key` header matching
settings.api_admin_key. When the key is not set the router fails closed —
every request returns 401.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Header, HTTPException, status

from accent_fleet.config import settings
from accent_fleet.ml.inference import ClusterPredictor
from app.deps import ClusterPredictorDep

logger = logging.getLogger("accent_fleet.api.admin")

router = APIRouter(prefix="/admin", tags=["admin"])


def _check_admin_key(x_api_key: str | None) -> None:
    """Fail-closed admin auth. Returns silently on match, raises 401 otherwise."""
    expected = settings().api_admin_key
    if not expected:
        # No key configured — never allow admin access. Surfacing this as 401
        # (not 503) keeps the public response identical to a bad-key call so
        # an attacker can't learn whether the endpoint is configured.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="admin endpoints require API_ADMIN_KEY",
        )
    if not x_api_key or x_api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing X-API-Key",
        )


@router.post("/reload-model")
def reload_model(
    predictor: ClusterPredictor = ClusterPredictorDep,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, str]:
    """Force a fresh ClusterPredictor load from MLflow."""
    _check_admin_key(x_api_key)
    logger.info("admin: reloading cluster model")
    info = predictor.reload()
    logger.info("admin: reloaded model_version=%s source=%s", info["model_version"], info["source"])
    return {
        "status": "reloaded",
        "model_version": info["model_version"],
        "source": info["source"],
    }
