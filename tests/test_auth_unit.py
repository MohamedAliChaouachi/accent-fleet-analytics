"""
Unit tests for app/auth/ — pure-Python, no DB.

Covers docs/auth_design.md §10.1:
  - Argon2id hash/verify round-trip + per-call salt
  - JWT issuance / verification (expired, malformed, wrong-key)
  - Principal invariant (role/tenant_id bijection)
  - Token-bucket rate limiter behaviour
"""

from __future__ import annotations

import asyncio
import os
import time

import jwt
import pytest

from app.auth.passwords import (
    InvalidPasswordError,
    hash_password,
    needs_rehash,
    verify_password,
)
from app.auth.principal import Principal
from app.auth.rate_limit import (
    LoginRateLimiter,
    RateLimitExceededError,
)


# ---------------------------------------------------------------------------
# Settings shim — give the token tests a stable signing key without
# touching the operator's real .env.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _stub_signing_key(monkeypatch):
    # 32+ bytes — RFC 7518 §3.2 minimum for HS256.
    monkeypatch.setenv(
        "JWT_SIGNING_KEY",
        "unit-test-key-do-not-use-in-prod-padding-padding",
    )
    monkeypatch.setenv("JWT_SIGNING_KEY_PREVIOUS", "")
    monkeypatch.setenv("JWT_ACCESS_TTL_SECONDS", "60")
    # Reset the lru_cache-backed singleton so settings reads the env we
    # just set.
    from accent_fleet.config import settings as settings_fn
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


# ---------------------------------------------------------------------------
# Argon2id
# ---------------------------------------------------------------------------
class TestPasswords:
    def test_hash_then_verify_succeeds(self) -> None:
        h = hash_password("correct-horse-battery-staple")
        verify_password("correct-horse-battery-staple", h)  # no exception

    def test_wrong_password_raises(self) -> None:
        h = hash_password("right")
        with pytest.raises(InvalidPasswordError):
            verify_password("wrong", h)

    def test_same_password_produces_different_hashes(self) -> None:
        """Salt is per-call; identical inputs must not produce identical hashes."""
        h1 = hash_password("alpha")
        h2 = hash_password("alpha")
        assert h1 != h2

    def test_malformed_hash_raises(self) -> None:
        with pytest.raises(InvalidPasswordError):
            verify_password("anything", "not-a-real-hash")

    def test_needs_rehash_false_for_fresh_hash(self) -> None:
        assert needs_rehash(hash_password("x")) is False


# ---------------------------------------------------------------------------
# Principal invariants
# ---------------------------------------------------------------------------
class TestPrincipal:
    def test_tenant_user_with_tenant_id_ok(self) -> None:
        p = Principal(user_id=1, tenant_id=5, role="tenant_user", email="a@b")
        assert p.tenant_id == 5
        assert not p.is_superadmin

    def test_superadmin_must_have_none_tenant(self) -> None:
        with pytest.raises(ValueError):
            Principal(user_id=1, tenant_id=5, role="superadmin", email="x@y")

    def test_tenant_user_must_have_tenant_id(self) -> None:
        with pytest.raises(ValueError):
            Principal(user_id=1, tenant_id=None, role="tenant_user", email="x@y")

    def test_superadmin_with_none_tenant_ok(self) -> None:
        p = Principal(user_id=1, tenant_id=None, role="superadmin", email="x@y")
        assert p.is_superadmin


# ---------------------------------------------------------------------------
# JWT issue + verify
# ---------------------------------------------------------------------------
class TestTokens:
    def _principal(self) -> Principal:
        return Principal(user_id=42, tenant_id=5, role="tenant_user",
                         email="user@tenant5.local")

    def test_issue_then_verify_round_trip(self) -> None:
        from app.auth.tokens import (
            issue_access_token,
            principal_from_payload,
            verify_access_token,
        )
        p = self._principal()
        token, _exp = issue_access_token(p)
        payload = verify_access_token(token)
        assert payload["sub"] == "42"
        assert payload["tnt"] == 5
        assert payload["role"] == "tenant_user"
        roundtripped = principal_from_payload(payload)
        assert roundtripped == p

    def test_expired_token_raises_expired(self) -> None:
        from app.auth.tokens import TokenExpiredError, verify_access_token
        key = os.environ["JWT_SIGNING_KEY"]
        token = jwt.encode(
            {
                "sub": "1",
                "tnt": 5,
                "role": "tenant_user",
                "iat": int(time.time()) - 600,
                "exp": int(time.time()) - 60,
            },
            key,
            algorithm="HS256",
        )
        with pytest.raises(TokenExpiredError):
            verify_access_token(token)

    def test_wrong_signature_raises_invalid(self) -> None:
        from app.auth.tokens import TokenInvalidError, verify_access_token
        token = jwt.encode(
            {
                "sub": "1", "tnt": 5, "role": "tenant_user",
                "iat": int(time.time()), "exp": int(time.time()) + 60,
            },
            "wrong-key-padded-to-32-bytes-or-more-here",
            algorithm="HS256",
        )
        with pytest.raises(TokenInvalidError):
            verify_access_token(token)

    def test_truncated_token_raises_invalid(self) -> None:
        from app.auth.tokens import TokenInvalidError, verify_access_token
        with pytest.raises(TokenInvalidError):
            verify_access_token("not.a.jwt")

    def test_previous_key_verifies_during_rotation(self, monkeypatch) -> None:
        from accent_fleet.config import settings as settings_fn
        from app.auth.tokens import verify_access_token

        old_key = os.environ["JWT_SIGNING_KEY"]
        token = jwt.encode(
            {
                "sub": "1", "tnt": 5, "role": "tenant_user",
                "iat": int(time.time()), "exp": int(time.time()) + 60,
            },
            old_key,
            algorithm="HS256",
        )

        # Rotate: new key is current, old key moves to previous.
        monkeypatch.setenv(
            "JWT_SIGNING_KEY",
            "brand-new-key-after-rotation-padded-padded",
        )
        monkeypatch.setenv("JWT_SIGNING_KEY_PREVIOUS", old_key)
        settings_fn.cache_clear()

        payload = verify_access_token(token)
        assert payload["sub"] == "1"

    def test_payload_missing_role_raises_invalid(self) -> None:
        from app.auth.tokens import (
            TokenInvalidError,
            principal_from_payload,
        )
        with pytest.raises(TokenInvalidError):
            principal_from_payload(
                {"sub": "1", "tnt": 5, "exp": 9999999999}
            )

    def test_payload_inconsistent_role_raises_invalid(self) -> None:
        from app.auth.tokens import (
            TokenInvalidError,
            principal_from_payload,
        )
        # superadmin with a tenant_id violates the Principal invariant.
        with pytest.raises(TokenInvalidError):
            principal_from_payload({
                "sub": "1", "tnt": 5, "role": "superadmin",
                "iat": 0, "exp": 9999999999,
            })


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
class TestRateLimiter:
    def test_under_limit_passes(self) -> None:
        limiter = LoginRateLimiter(max_attempts=3, window_seconds=60)

        async def run():
            for _ in range(3):
                await limiter.check("1.2.3.4")

        asyncio.run(run())

    def test_over_limit_raises(self) -> None:
        limiter = LoginRateLimiter(max_attempts=3, window_seconds=60)

        async def run():
            for _ in range(3):
                await limiter.check("1.2.3.4")
            with pytest.raises(RateLimitExceededError) as exc:
                await limiter.check("1.2.3.4")
            assert exc.value.retry_after_seconds >= 1

        asyncio.run(run())

    def test_separate_ips_have_independent_buckets(self) -> None:
        limiter = LoginRateLimiter(max_attempts=2, window_seconds=60)

        async def run():
            await limiter.check("1.1.1.1")
            await limiter.check("1.1.1.1")
            # 2.2.2.2 still has full bucket.
            await limiter.check("2.2.2.2")

        asyncio.run(run())

    def test_window_expiry_lets_attempts_pass_again(self) -> None:
        limiter = LoginRateLimiter(max_attempts=1, window_seconds=0)

        async def run():
            await limiter.check("9.9.9.9")
            # window=0 means the previous attempt's monotonic time is
            # already <= cutoff on the next call, so the bucket clears.
            await asyncio.sleep(0.01)
            await limiter.check("9.9.9.9")

        asyncio.run(run())
