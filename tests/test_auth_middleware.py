"""
Middleware-only tests: AuthMiddleware decides whether a request is
allowed through based on the bearer token and AUTH_ENFORCEMENT mode.

These tests do not hit the DB. They mount a tiny FastAPI app with a
single protected route plus the AuthMiddleware, then drive it with
TestClient.
"""

from __future__ import annotations

import time

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth.middleware import AuthMiddleware

SIGNING_KEY = "test-key-padded-to-32-bytes-or-longer-here"


@pytest.fixture(autouse=True)
def _stub_env(monkeypatch):
    monkeypatch.setenv("JWT_SIGNING_KEY", SIGNING_KEY)
    monkeypatch.setenv("JWT_SIGNING_KEY_PREVIOUS", "")
    monkeypatch.setenv("JWT_ACCESS_TTL_SECONDS", "60")
    from accent_fleet.config import settings as settings_fn
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


def _make_app(enforcement: str) -> FastAPI:
    from accent_fleet.config import settings as settings_fn
    # Hot-reload settings so the middleware sees the new enforcement value.
    settings_fn.cache_clear()
    import os
    os.environ["AUTH_ENFORCEMENT"] = enforcement

    app = FastAPI()
    app.add_middleware(AuthMiddleware)

    @app.get("/protected")
    def protected() -> dict[str, str]:
        return {"ok": "yes"}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/auth/login")
    def login() -> dict[str, str]:
        return {"hello": "world"}

    return app


def _valid_token() -> str:
    return jwt.encode(
        {
            "sub": "1", "tnt": 5, "role": "tenant_user",
            "email": "u@t5.local",
            "iat": int(time.time()), "exp": int(time.time()) + 60,
        },
        SIGNING_KEY,
        algorithm="HS256",
    )


# ---------------------------------------------------------------------------
# Advisory mode: never reject; just log.
# ---------------------------------------------------------------------------
class TestAdvisoryMode:
    def test_no_token_passes(self) -> None:
        with TestClient(_make_app("advisory")) as c:
            r = c.get("/protected")
        assert r.status_code == 200

    def test_invalid_token_passes(self) -> None:
        with TestClient(_make_app("advisory")) as c:
            r = c.get("/protected", headers={"Authorization": "Bearer junk"})
        assert r.status_code == 200

    def test_valid_token_passes(self) -> None:
        with TestClient(_make_app("advisory")) as c:
            r = c.get(
                "/protected",
                headers={"Authorization": f"Bearer {_valid_token()}"},
            )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Enforce mode: reject anything that doesn't have a valid token.
# ---------------------------------------------------------------------------
class TestEnforceMode:
    def test_no_token_returns_401(self) -> None:
        with TestClient(_make_app("enforce")) as c:
            r = c.get("/protected")
        assert r.status_code == 401
        assert r.headers.get("WWW-Authenticate") == "Bearer"

    def test_invalid_token_returns_401(self) -> None:
        with TestClient(_make_app("enforce")) as c:
            r = c.get("/protected", headers={"Authorization": "Bearer junk"})
        assert r.status_code == 401

    def test_expired_token_returns_401(self) -> None:
        expired = jwt.encode(
            {
                "sub": "1", "tnt": 5, "role": "tenant_user",
                "iat": int(time.time()) - 600,
                "exp": int(time.time()) - 60,
            },
            SIGNING_KEY,
            algorithm="HS256",
        )
        with TestClient(_make_app("enforce")) as c:
            r = c.get(
                "/protected",
                headers={"Authorization": f"Bearer {expired}"},
            )
        assert r.status_code == 401

    def test_valid_token_passes(self) -> None:
        with TestClient(_make_app("enforce")) as c:
            r = c.get(
                "/protected",
                headers={"Authorization": f"Bearer {_valid_token()}"},
            )
        assert r.status_code == 200

    def test_health_exempt_even_in_enforce(self) -> None:
        with TestClient(_make_app("enforce")) as c:
            r = c.get("/health")
        assert r.status_code == 200

    def test_login_exempt_even_in_enforce(self) -> None:
        with TestClient(_make_app("enforce")) as c:
            r = c.post("/v1/auth/login")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Off mode: middleware is inert.
# ---------------------------------------------------------------------------
class TestOffMode:
    def test_off_lets_everything_through(self) -> None:
        with TestClient(_make_app("off")) as c:
            r = c.get("/protected")
        assert r.status_code == 200
