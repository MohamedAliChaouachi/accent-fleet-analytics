"""
Request-scoped middleware: structured access logging + Prometheus timing.

Wired in `app/main.py` via `app.add_middleware(MetricsMiddleware)`. Records
into the shared Prometheus registry in `accent_fleet.observability` and
emits one structured log line per request with stable fields:

    method, route, status, duration_ms, request_id

`route` is the matched FastAPI route template (e.g. `/devices/{device_id}/profile`)
rather than the raw path, so per-device IDs don't blow up cardinality.
"""

from __future__ import annotations

import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from accent_fleet.observability import (
    http_request_duration_seconds,
    http_requests_in_flight,
    http_requests_total,
)

logger = structlog.get_logger("accent_fleet.api.access")


def _route_template(request: Request) -> str:
    """
    Return the matched route template, e.g. '/devices/{device_id}/profile',
    or the raw path when no route was matched (404, /metrics, ...).

    Using the template keeps Prometheus label cardinality bounded — without
    this, every distinct device_id would create its own time series.
    """
    route = request.scope.get("route")
    if route is not None and hasattr(route, "path"):
        return route.path
    return request.url.path


class MetricsMiddleware(BaseHTTPMiddleware):
    """Record request count + duration, stamp request_id, emit access log."""

    async def dispatch(self, request: Request, call_next):
        # Inbound request_id (e.g. set by a load balancer) wins so we can
        # correlate logs across services. Otherwise mint a fresh one.
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        structlog.contextvars.bind_contextvars(request_id=request_id)

        http_requests_in_flight.inc()
        start = time.perf_counter()
        status_code = 500
        try:
            response: Response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            # Re-raise after metric recording. status remains 500.
            raise
        finally:
            duration_s = time.perf_counter() - start
            route = _route_template(request)
            method = request.method

            http_request_duration_seconds.labels(method=method, route=route).observe(duration_s)
            http_requests_total.labels(
                method=method, route=route, status=str(status_code)
            ).inc()
            http_requests_in_flight.dec()

            # Drop request-scope context AFTER the access log so downstream
            # logging (uvicorn's own access line, etc.) doesn't inherit it.
            logger.info(
                "request",
                method=method,
                route=route,
                status=status_code,
                duration_ms=round(duration_s * 1000.0, 2),
            )
            structlog.contextvars.clear_contextvars()
