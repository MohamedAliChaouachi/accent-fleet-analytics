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

from fastapi import APIRouter, HTTPException, Query, status

from app.ai.config import ai_settings
from app.ai.providers.base import LLMProviderError
from app.ai.providers.factory import get_provider
from app.ai.schemas.ai import (
    MAX_HISTORY_PAGE,
    MAX_HISTORY_TURNS,
    AIFeedbackRequest,
    AIFeedbackResponse,
    AIHistoryItem,
    AIHistoryResponse,
    AIQueryError,
    AIQueryRequest,
    AIQueryResponse,
    AISchemaColumn,
    AISchemaResponse,
    AISchemaTable,
)
from app.ai.schemas.catalog import CATALOG
from app.ai.services.audit import write_ai_query_event
from app.ai.services.feedback import FeedbackError, upsert_feedback
from app.ai.services.history import read_user_history
from app.ai.services.pipeline import PipelineError, PipelineInput, run
from app.ai.services.rate_limit import (
    AIRateLimitExceededError,
    get_ai_rate_limiter,
)
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

    # Rate limit AFTER tenant resolution so the tenant bucket sees the
    # effective tenant (matters for the superadmin path, where the
    # principal has tenant_id=None but the request is charged to the
    # selected tenant). The check raises before any LLM call so a
    # throttled request burns zero tokens.
    try:
        get_ai_rate_limiter().check(
            user_id=principal.user_id,
            tenant_id=tenant_id,
        )
    except AIRateLimitExceededError as e:
        write_ai_query_event(
            user_id=principal.user_id,
            tenant_id=tenant_id,
            question=body.question,
            stage="rate_limited",
            error_detail=f"scope={e.scope}",
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"AI query rate limit exceeded ({e.scope}); retry later",
            headers={"Retry-After": str(e.retry_after_seconds)},
        ) from e

    # Trim history to the per-request cap. Client-side bound exists too,
    # but the server is the source of truth — anything older gets
    # dropped silently so a runaway client can't blow up the prompt.
    trimmed_history = tuple(body.history[-MAX_HISTORY_TURNS:])

    try:
        response = run(
            inp=PipelineInput(
                question=body.question,
                tenant_id=tenant_id,
                history=trimmed_history,
            ),
            provider=get_provider(),
            settings=ai_settings(),
        )
    except LLMProviderError as e:
        # The factory raises this when the provider can't be built
        # (missing API key, missing SDK). Surface as 500 — it's a
        # configuration error, not a user error.
        write_ai_query_event(
            user_id=principal.user_id,
            tenant_id=tenant_id,
            question=body.question,
            stage="config",
            error_detail=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=AIQueryError(stage="config", detail=str(e)).model_dump(),
        ) from e
    except PipelineError as e:
        # One audit row per failed pipeline run. `sql_text` captures the
        # offending SQL when the failure came after generation (guard /
        # execution stages); it stays None when the LLM itself failed.
        write_ai_query_event(
            user_id=principal.user_id,
            tenant_id=tenant_id,
            question=body.question,
            stage=e.stage,
            sql_text=e.sql,
            error_detail=e.detail,
        )
        raise HTTPException(
            status_code=_STAGE_STATUS.get(e.stage, 500),
            detail=AIQueryError(stage=e.stage, detail=e.detail, sql=e.sql).model_dump(),
        ) from e

    # Happy path — one row recording what we ran, how long it took, and
    # which provider/model answered. Volume bound is "one row per /ai/query
    # call", which is exactly what we want for cost reporting.
    event_id = write_ai_query_event(
        user_id=principal.user_id,
        tenant_id=tenant_id,
        question=body.question,
        stage="success",
        sql_text=response.sql,
        row_count=response.row_count,
        elapsed_ms=response.elapsed_ms,
        chart_type=response.chart_type,
        provider=response.provider,
        model=response.model,
    )
    # event_id is None only if the audit write failed (fail-open). The
    # client falls back to "feedback unavailable for this turn" in that
    # case; everything else still works.
    response.event_id = event_id
    return response


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


# ---------------------------------------------------------------------------
# GET /ai/history — the caller's own past queries
# ---------------------------------------------------------------------------
#
# Strictly per-user. We don't paginate (yet) — the UI is a sidebar list
# capped at MAX_HISTORY_PAGE rows, which is enough for the conversation-
# history use case. When/if we add a "full history" view, this is the
# right place to add a cursor parameter.


@router.get("/history", response_model=AIHistoryResponse)
def ai_history(
    principal: Principal = CurrentPrincipalDep,
    limit: int = Query(
        default=50,
        ge=1,
        le=MAX_HISTORY_PAGE,
        description=(
            "Maximum number of past queries to return, newest first. "
            f"Capped server-side at {MAX_HISTORY_PAGE}."
        ),
    ),
) -> AIHistoryResponse:
    rows = read_user_history(user_id=principal.user_id, limit=limit)
    return AIHistoryResponse(items=[AIHistoryItem(**r) for r in rows])


# ---------------------------------------------------------------------------
# POST /ai/feedback — thumbs up/down on a past query
# ---------------------------------------------------------------------------


@router.post(
    "/feedback",
    response_model=AIFeedbackResponse,
    status_code=status.HTTP_200_OK,
)
def ai_feedback(
    body: AIFeedbackRequest,
    principal: Principal = CurrentPrincipalDep,
) -> AIFeedbackResponse:
    """Record (or update) the caller's feedback on one past /ai/query call.

    A 404 means the ``event_id`` doesn't correspond to a query owned by
    the authenticated user — either the user is spoofing someone else's
    event_id, or their local conversation state is stale (e.g. audit row
    was aged out). Either way the UI's right move is to drop the local
    thumbs state and re-fetch ``/history``.
    """
    try:
        row = upsert_feedback(
            user_id=principal.user_id,
            event_id=body.event_id,
            value=body.value,
            comment=body.comment,
        )
    except FeedbackError as e:
        if e.kind == "not_found":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=e.detail,
            ) from e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.detail,
        ) from e
    return AIFeedbackResponse(**row)


# ---------------------------------------------------------------------------
# GET /ai/schema — the curated catalog the SQL guard admits
# ---------------------------------------------------------------------------
#
# Static within a deploy. The client can cache the response indefinitely
# (we don't set ETag headers — the response is small enough that adding
# the round-trip back for revalidation is more code than it's worth).


@router.get("/schema", response_model=AISchemaResponse)
def ai_schema(
    principal: Principal = CurrentPrincipalDep,  # noqa: ARG001 — auth required
) -> AISchemaResponse:
    """Return the curated table catalog the AI assistant queries against.

    The response is a serialized snapshot of the in-process
    :data:`app.ai.schemas.catalog.CATALOG` — every table the SQL guard
    will admit. This is intentionally a whitelist, not a live
    ``information_schema`` dump, so adding a table here is an explicit
    decision (see ``app/ai/schemas/catalog.py``).
    """
    tables = [
        AISchemaTable(
            fqname=spec.fqname,
            schema_name=spec.schema,
            name=spec.name,
            description=spec.description,
            grain=spec.grain,
            tenant_scoped=spec.tenant_scoped,
            columns=[
                AISchemaColumn(name=c.name, type=c.type, description=c.description)
                for c in spec.columns
            ],
        )
        for spec in CATALOG.values()
    ]
    return AISchemaResponse(tables=tables)
