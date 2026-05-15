"""
HTTP-API auth helper for dashboard pages.

The FastAPI service enforces JWT bearer auth on every non-exempt path
(app/auth/middleware.py). Dashboard pages that hit the API — currently
only the What-If scoring page, but the pattern generalises — have to
present an `Authorization: Bearer …` header or they get 401.

This module gives pages a one-liner:

    from dashboard.lib.api import post_json
    r = post_json("/v1/score/risk", payload)

…that hides the bearer-token lifecycle:

  * Lazy login on first call (no boot-time API dependency — Streamlit
    starts cleanly even if the API is still warming up).
  * Module-scoped token cache, refreshed ~30s before declared expiry.
  * On a 401 we wipe the cache, re-login once, and retry. Covers token
    revocation, server restart with a fresh signing key, or the edge
    case where the cushion got eaten by clock skew.

Credentials come from env vars baked into the dashboard container:

  DASHBOARD_API_EMAIL     — defaults to "superadmin@accent.local" so
                            it works out of the box with the M2 seed.
  DASHBOARD_API_PASSWORD  — required when first calling the API. The
                            password the operator captured from
                            scripts/seed_auth.py stdout. Empty value
                            surfaces a clean Streamlit error instead
                            of a confusing 401.

Why not a dedicated `dashboard@service` user
--------------------------------------------
The /score/risk and /score/cluster endpoints don't read tenant data —
they run an in-process scorer over the supplied feature vector. Auth
gates *call* access, not *row* access. So a superadmin token has no
broader blast radius than a tenant_user token for these endpoints.
If we add pages that hit /devices/* or /admin/* the choice becomes
more interesting; for now: cheapest correct thing wins.
"""

from __future__ import annotations

import os
import threading
import time

import httpx
import streamlit as st

API_BASE = os.environ.get("API_BASE_URL", "http://api:8000")
EMAIL = os.environ.get("DASHBOARD_API_EMAIL", "superadmin@accent.local")
_PASSWORD = os.environ.get("DASHBOARD_API_PASSWORD", "")

# Re-login this many seconds before the stamped expiry. JWT access TTL
# defaults to 15 min (jwt_access_ttl_seconds = 900), so 30s of cushion
# is plenty without burning a full re-login per page render.
_REFRESH_CUSHION_SECONDS = 30

_lock = threading.Lock()


@st.cache_resource(show_spinner=False)
def _token_cell() -> dict:
    """
    Single mutable cell that survives Streamlit reruns.

    `@st.cache_resource` returns the same dict object across reruns of the
    same script, so mutating its contents is how we persist the token
    without keeping a true module-level global (which Streamlit's
    threading model can reset under us).
    """
    return {"access_token": None, "expires_at": 0.0}


def _login() -> tuple[str, float]:
    """POST /v1/auth/login → (access_token, expires_at_epoch)."""
    if not _PASSWORD:
        raise RuntimeError(
            "DASHBOARD_API_PASSWORD is empty. Set it in the dashboard "
            "container's environment (docker-compose.yml → "
            "dashboard.environment) to the password printed by "
            f"`scripts/seed_auth.py` for `{EMAIL}` — or override "
            "DASHBOARD_API_EMAIL to a different account first."
        )
    r = httpx.post(
        f"{API_BASE}/v1/auth/login",
        json={"email": EMAIL, "password": _PASSWORD},
        timeout=10.0,
    )
    r.raise_for_status()
    body = r.json()
    return body["access_token"], float(body["expires_at"])


def _bearer() -> str:
    """Return a fresh-enough access token, logging in if needed."""
    cell = _token_cell()
    with _lock:
        if (
            cell["access_token"] is None
            or time.time() >= cell["expires_at"] - _REFRESH_CUSHION_SECONDS
        ):
            cell["access_token"], cell["expires_at"] = _login()
        return cell["access_token"]


def _invalidate_token() -> None:
    """Force the next call to re-login. Called on a server-side 401."""
    cell = _token_cell()
    with _lock:
        cell["access_token"] = None
        cell["expires_at"] = 0.0


def post_json(path: str, payload: dict, *, timeout: float = 5.0) -> httpx.Response:
    """
    POST `payload` to `{API_BASE}{path}` with the bearer token attached.

    On a 401 we wipe the cached token, re-login once, and retry. Any other
    status is returned untouched — callers do their own `.raise_for_status()`
    + error-shape handling so the page can render a domain-specific message.
    """
    url = f"{API_BASE}{path}"

    def _do(token: str) -> httpx.Response:
        return httpx.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )

    resp = _do(_bearer())
    if resp.status_code == 401:
        _invalidate_token()
        resp = _do(_bearer())
    return resp
