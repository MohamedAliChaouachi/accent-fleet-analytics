"""
POST /v1/ai/query — Text2SQL endpoint.

Auth: requires an authenticated principal. The tenant scope used by the
SQL guard comes from the JWT, not from the request body — this is the
single most important security property of the endpoint, so it lives
right at the top of the route and is documented inline.

The router is intentionally thin: it parses the request, resolves the
tenant, hands off to :mod:`app.ai.services.pipeline`, and maps
:class:`PipelineError` stages to HTTP status codes. Everything else is
behind the pipeline boundary so the same code can be exercised from
tests without going through HTTP.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.ai.config import ai_settings
from app.ai.providers.base import LLMProviderError
from app.ai.providers.factory import get_provider
from app.ai.schemas.ai import AIQueryError, AIQueryRequest, AIQueryResponse
from app.ai.services.pipeline import PipelineError, PipelineInput, run
from app.auth.deps import CurrentPrincipalDep
from app.auth.principal import Principal

router = APIRouter(prefix="/ai", tags=["ai"])


# Map each pipeline failure stage to the right HTTP status. The defaults
# match how the React client will want to react:
#   - 400: user/LLM error (bad SQL, wrong table, etc.) — show in UI
#   - 502: upstream LLM failure — retryable
#   - 500: our DB / config error — page on-call
_STAGE_STATUS: dict[str, int] = {
    "llm": status.HTTP_502_BAD_GATEWAY,
    "sql_guard": status.HTTP_400_BAD_REQUEST,
    "tenant_filter": status.HTTP_400_BAD_REQUEST,
    "execution": status.HTTP_500_INTERNAL_SERVER_ERROR,
    "summarization": status.HTTP_500_INTERNAL_SERVER_ERROR,
    "config": status.HTTP_500_INTERNAL_SERVER_ERROR,
}


@router.post(
    "/query",
    response_model=AIQueryResponse,
    responses={
        400: {"model": AIQueryError, "description": "Question could not be turned into safe SQL."},
        502: {"model": AIQueryError, "description": "Upstream LLM provider failure."},
    },
)
def ai_query(
    body: AIQueryRequest,
    principal: Principal = CurrentPrincipalDep,
) -> AIQueryResponse:
    """Run one natural-language question through the Text2SQL pipeline."""
    tenant_id = _resolve_tenant(principal=principal, requested=body.tenant_id)
    try:
        return run(
            inp=PipelineInput(question=body.question, tenant_id=tenant_id),
            provider=get_provider(),
            settings=ai_settings(),
        )
    except LLMProviderError as e:
        # The factory raises this when the provider can't be built
        # (missing API key, missing SDK). Surface as 500 — it's a
        # configuration error, not a user error.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=AIQueryError(stage="config", detail=str(e)).model_dump(),
        ) from e
    except PipelineError as e:
        raise HTTPException(
            status_code=_STAGE_STATUS.get(e.stage, 500),
            detail=AIQueryError(stage=e.stage, detail=e.detail, sql=e.sql).model_dump(),
        ) from e


# ---------------------------------------------------------------------------
# Tenant resolution — the security crux
# ---------------------------------------------------------------------------


def _resolve_tenant(*, principal: Principal, requested: int | None) -> int:
    """Pick the tenant_id the pipeline will bind to ``:tenant_id``.

    Rules (in priority order):

    1. Tenant users / tenant admins: tenant comes from the JWT. If the
       body specifies a different tenant_id, return 403 — the
       distinction matters because silently overriding the body would
       hide an attempted privilege escalation in logs.

    2. Superadmin: must pass ``tenant_id`` explicitly. We deliberately
       don't allow "all tenants" mode in v1 because the SQL guard
       requires a single bind value — supporting cross-tenant queries
       is a Phase 2 feature with its own audit story.
    """
    if principal.is_superadmin:
        if requested is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=AIQueryError(
                    stage="tenant_filter",
                    detail=(
                        "superadmin must specify `tenant_id` in the request "
                        "body — cross-tenant queries are not supported in v1"
                    ),
                ).model_dump(),
            )
        return requested

    # tenant_user / tenant_admin path
    if requested is not None and requested != principal.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=AIQueryError(
                stage="tenant_filter",
                detail="tenant_id in body does not match the authenticated principal",
            ).model_dump(),
        )
    # principal.tenant_id is non-None by the Principal __post_init__
    # invariant for any non-superadmin role.
    assert principal.tenant_id is not None
    return principal.tenant_id
