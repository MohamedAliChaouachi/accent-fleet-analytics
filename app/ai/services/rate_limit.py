"""
In-process rate limiter for /v1/ai/query.

Modeled on :mod:`app.auth.rate_limit` — same asyncio.Lock + per-bucket
list of recent attempt timestamps — but keyed by `(scope, key)` so we
can enforce two independent ceilings on the same request:

  - **per_user**:   throttles one runaway end-user without affecting
                    their tenant peers.
  - **per_tenant**: throttles a whole tenant so even a coordinated
                    cohort can't burn the LLM budget.

Why both, not just one?

  * Per-user only: a tenant with 50 active dashboards still gets 50× the
    nominal limit. The LLM bill is paid per-tenant, so this loses control
    of cost.
  * Per-tenant only: a single bad-actor user inside an otherwise quiet
    tenant locks the whole tenant out — bad UX for incident response,
    because the operator can't tell *who* abused it.

Why in-process (not Redis)?

  Same reason as the login limiter — the API is single-instance in v1.
  When §2.4 splits the API into multiple pods this becomes incorrect
  (each pod sees a fraction of the traffic), and we swap in a distributed
  limiter at that point. The `check()` interface stays the same.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


class AIRateLimitExceededError(Exception):
    """Raised when either the per-user or per-tenant bucket is full."""

    def __init__(self, scope: str, retry_after_seconds: int) -> None:
        super().__init__(
            f"ai rate limit exceeded ({scope}); retry after {retry_after_seconds}s"
        )
        self.scope = scope  # "user" or "tenant" — surfaced in the audit row
        self.retry_after_seconds = retry_after_seconds


@dataclass
class _Bucket:
    # Wall-clock seconds (monotonic) of every attempt within the window.
    # Bounded by the per-bucket max, so list ops stay O(max).
    attempts: list[float] = field(default_factory=list)


class AIRateLimiter:
    """Process-wide limiter. One instance via :func:`get_ai_rate_limiter`.

    The two ceilings are independent: a request must pass *both* to be
    admitted. We charge both buckets on admission so the second of two
    nearly-simultaneous calls reflects the first.
    """

    def __init__(
        self,
        *,
        per_user_max: int,
        per_tenant_max: int,
        window_seconds: int,
    ) -> None:
        self.per_user_max = per_user_max
        self.per_tenant_max = per_tenant_max
        self.window_seconds = window_seconds
        # Separate keyspaces so a user_id collision with a tenant_id
        # can't share a bucket. Plain dicts (the `_lock` protects them).
        self._user: dict[int, _Bucket] = {}
        self._tenant: dict[int, _Bucket] = {}
        # Threading lock (not asyncio.Lock) because the rest of the AI
        # pipeline — SQLAlchemy execute, LLM SDK calls — is sync and runs
        # in FastAPI's threadpool. A threading lock keeps the route sync
        # and concurrent worker threads safe.
        self._lock = threading.Lock()

    def check(self, *, user_id: int, tenant_id: int | None) -> None:
        """Register one attempt and raise if either bucket overflows.

        Called BEFORE the request hits the LLM provider so we never burn
        a token on a rate-limited call. ``tenant_id`` is None for the
        superadmin path (their requests are not charged to a tenant
        bucket — they're charged to their user bucket only).
        """
        now = time.monotonic()
        cutoff = now - self.window_seconds

        with self._lock:
            user_bucket = self._user.setdefault(user_id, _Bucket())
            user_bucket.attempts = [t for t in user_bucket.attempts if t >= cutoff]

            if len(user_bucket.attempts) >= self.per_user_max:
                oldest = user_bucket.attempts[0]
                retry_after = int(oldest + self.window_seconds - now) + 1
                raise AIRateLimitExceededError("user", max(retry_after, 1))

            if tenant_id is not None:
                tenant_bucket = self._tenant.setdefault(tenant_id, _Bucket())
                tenant_bucket.attempts = [
                    t for t in tenant_bucket.attempts if t >= cutoff
                ]
                if len(tenant_bucket.attempts) >= self.per_tenant_max:
                    oldest = tenant_bucket.attempts[0]
                    retry_after = int(oldest + self.window_seconds - now) + 1
                    raise AIRateLimitExceededError("tenant", max(retry_after, 1))

                # Both checks passed — charge tenant bucket too.
                tenant_bucket.attempts.append(now)

            user_bucket.attempts.append(now)

            # Periodic compaction: drop empty buckets so the dicts don't
            # grow unbounded over a long uptime.
            if len(self._user) > 1024:
                self._user = {k: v for k, v in self._user.items() if v.attempts}
            if len(self._tenant) > 1024:
                self._tenant = {k: v for k, v in self._tenant.items() if v.attempts}

    def reset(self) -> None:
        """Test/operational hook: drop every bucket."""
        self._user.clear()
        self._tenant.clear()


_singleton: AIRateLimiter | None = None


def get_ai_rate_limiter() -> AIRateLimiter:
    """Return the process-wide limiter, building it on first call.

    Reads :mod:`app.ai.config` lazily so test code that mutates the
    settings between import and first call still picks up the override.
    """
    global _singleton
    if _singleton is None:
        from app.ai.config import ai_settings

        s = ai_settings()
        _singleton = AIRateLimiter(
            per_user_max=s.rate_limit_user_max,
            per_tenant_max=s.rate_limit_tenant_max,
            window_seconds=s.rate_limit_window_seconds,
        )
    return _singleton


def reset_ai_rate_limiter() -> None:
    """Test-only: drop the singleton so the next ``get_*()`` rebuilds it
    with current settings. Used after a monkeypatch of env vars."""
    global _singleton
    _singleton = None
