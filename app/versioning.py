"""
API versioning policy.

Canonical paths live under ``/v1`` (e.g. ``/v1/score/risk``,
``/v1/devices/{id}/profile``). The pre-versioning paths (``/score/...``,
``/devices/...``, ``/admin/...``) are kept mounted for backwards
compatibility, but every response on them carries:

    Deprecation: true
    Sunset:      <human-readable date>
    Link:        </v1/...>; rel="successor-version"

…so existing clients (the Streamlit dashboard, the bench script, anything
external someone may have wired up) keep working without code changes,
but operators see the headers in `curl -I` and CI logs and know when to
migrate.

Why not break legacy on the v0.7.0 release:
  - The dashboard ships in the same repo and could be updated atomically,
    but we don't control external scrape probes, ad-hoc Postman calls, or
    notebooks people copy-pasted from #data-science. A sunset window is
    cheaper than a Slack thread per consumer.

Operational endpoints — ``/health``, ``/ready``, ``/metrics``, ``/`` —
are intentionally NOT versioned. Their paths are conventions other tooling
(k8s probes, Prometheus, uptime checks) reads from configuration; bumping
them imposes coordinated changes everywhere for zero behavioural gain.
"""

from __future__ import annotations

from fastapi import Depends, FastAPI, Response

# Sunset target: one major release after v0.7.0 lands. Picked far enough
# out that the dashboard and any external callers have a full quarter to
# update; close enough that this isn't dead code in five years.
LEGACY_SUNSET_HTTP_DATE = "Sat, 31 Oct 2026 23:59:59 GMT"
LEGACY_SUNSET_HUMAN = "2026-10-31"

# RFC 8594 specifies Sunset; RFC 9745 (draft) covers Deprecation. Both
# accept boolean-as-string for "deprecated, no specific replacement date";
# we set Sunset explicitly so clients can plan.
_DEPRECATION_HEADER_VALUE = "true"


def _legacy_response_headers(response: Response) -> None:
    """
    FastAPI dependency that stamps every response on a legacy-mounted
    route with deprecation metadata. Applied via
    ``include_router(..., dependencies=[Depends(_legacy_response_headers)])``
    so it covers the whole router without per-endpoint plumbing.
    """
    response.headers["Deprecation"] = _DEPRECATION_HEADER_VALUE
    response.headers["Sunset"] = LEGACY_SUNSET_HTTP_DATE
    # Link header points clients at the versioned replacement. We can't
    # know the exact /v1 path from a dependency (Response doesn't expose
    # the matched route), so we point at the v1 root and trust the
    # consumer to map by suffix. That's also what RFC 8288 expects:
    # successor-version is allowed to point at a version namespace.
    response.headers["Link"] = '</v1>; rel="successor-version"'


# Single Depends instance so FastAPI's caching de-duplicates it across
# routes that get included with the same dependency object.
legacy_route_dependency = Depends(_legacy_response_headers)


def include_versioned_router(
    app: FastAPI,
    router,
    *,
    tag_suffix: str = "",
) -> None:
    """
    Mount a router twice:
      • Once at ``/v1`` (canonical, listed in OpenAPI schema).
      • Once at the bare path (legacy, hidden from OpenAPI to keep the
        /docs page clean and to discourage new code from picking it up).

    The hidden mount still serves traffic — clients reading the OpenAPI
    schema will only see /v1 paths, but anyone hitting the legacy path
    will get a successful response plus the deprecation headers.
    """
    # Canonical /v1 mount — shown in docs.
    app.include_router(
        router,
        prefix="/v1",
        tags=[f"v1{tag_suffix}"] if tag_suffix else None,
    )
    # Legacy mount — hidden from schema, headers stamped on every response.
    app.include_router(
        router,
        dependencies=[legacy_route_dependency],
        include_in_schema=False,
    )
