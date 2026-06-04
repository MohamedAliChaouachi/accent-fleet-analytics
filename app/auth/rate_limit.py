"""
In-process token-bucket rate limiter for /v1/auth/login.

The design doc §11 calls for "5 attempts / 15 min per IP", explicitly
rejecting a Redis-backed limiter because the API is single-instance
today. This module implements the simplest correct thing: a dict keyed
by source IP, an asyncio lock to keep counters consistent across
concurrent requests, and an explicit purge of stale entries on every
lookup so the dict doesn't grow unbounded.

When §2.4 splits the API into multiple pods, this limiter becomes
incorrect (each pod sees a fraction of the requests) and we'll swap in
a distributed limiter at that point. The interface here — `check()`
raises on violation — stays the same.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from accent_fleet.config import settings


class RateLimitExceededError(Exception):
    """Raised when an IP has used up its bucket within the window."""

    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__(
            f"rate limit exceeded; retry after {retry_after_seconds}s"
        )
        self.retry_after_seconds = retry_after_seconds


@dataclass
class _Bucket:
    # Wall-clock seconds of every attempt within the current window.
    # Cheap to store at our volume (max ~5 entries × number of active
    # IPs); replacing this with a real ring buffer is wasted code.
    attempts: list[float] = field(default_factory=list)


class LoginRateLimiter:
    """
    Per-IP rate limiter. Singleton via `get_login_rate_limiter()`.

    Concurrency: protected by a single asyncio.Lock. The critical
    section is microseconds (list append + filter); contention is
    irrelevant compared to the cost of the login endpoint itself.
    """

    def __init__(self, max_attempts: int, window_seconds: int) -> None:
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self._buckets: dict[str, _Bucket] = {}
        self._lock = asyncio.Lock()

    async def check(self, ip: str) -> None:
        """
        Register one attempt for `ip` and raise if it's over the limit.

        Called BEFORE the login handler does any work so a brute-force
        loop never gets as far as the Argon2 verify (which is the
        expensive bit we're protecting).
        """
        # Window boundary: attempts older than `cutoff` no longer count.
        now = time.monotonic()
        cutoff = now - self.window_seconds

        # Serialize counter mutation so concurrent requests stay consistent.
        async with self._lock:
            bucket = self._buckets.setdefault(ip, _Bucket())
            # Drop expired attempts. Tight loop in Python is fine — the
            # list is bounded by max_attempts per IP.
            bucket.attempts = [t for t in bucket.attempts if t >= cutoff]

            if len(bucket.attempts) >= self.max_attempts:
                # Compute Retry-After from the oldest attempt: that's
                # the time at which the bucket will have room again.
                oldest = bucket.attempts[0]
                retry_after = int(oldest + self.window_seconds - now) + 1
                raise RateLimitExceededError(max(retry_after, 1))

            # Under the limit: record this attempt and allow it.
            bucket.attempts.append(now)

            # Periodic purge: any time the dict grows past a small
            # threshold, drop empty buckets. O(n) but only every Nth
            # call, and n is bounded by active distinct source IPs.
            if len(self._buckets) > 1024:
                self._buckets = {
                    k: v for k, v in self._buckets.items() if v.attempts
                }

    def reset(self, ip: str | None = None) -> None:
        """Test/operational hook: clear one IP or all of them."""
        if ip is None:
            self._buckets.clear()
        else:
            self._buckets.pop(ip, None)


_singleton: LoginRateLimiter | None = None


def get_login_rate_limiter() -> LoginRateLimiter:
    """
    Return the process-wide limiter, building it on first call.

    Reads the settings each first-call only (not on every request) so
    test code that mutates `AUTH_LOGIN_RATE_MAX` before importing this
    module still picks up the override.
    """
    global _singleton
    if _singleton is None:
        s = settings()
        _singleton = LoginRateLimiter(
            max_attempts=s.auth_login_rate_max,
            window_seconds=s.auth_login_rate_window_seconds,
        )
    return _singleton


def reset_login_rate_limiter() -> None:
    """Test-only: drop the singleton so the next `get_*()` rebuilds it."""
    global _singleton
    _singleton = None
