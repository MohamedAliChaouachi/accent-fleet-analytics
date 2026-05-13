"""
Prometheus metrics endpoint.

  GET /metrics  -> text-format exposition of every counter / histogram /
                   gauge registered in accent_fleet.observability.REGISTRY.

This endpoint is intentionally unauthenticated — internal scrape targets
(Prometheus server, ops dashboards) should reach it directly. When the
stack is fronted by the nginx --profile auth gateway, add an `allow`
rule for the Prometheus scraper IP and `deny all` for everyone else.
"""

from __future__ import annotations

from fastapi import APIRouter, Response

from accent_fleet.observability import render_metrics

router = APIRouter(tags=["meta"])


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    body, content_type = render_metrics()
    return Response(content=body, media_type=content_type)
