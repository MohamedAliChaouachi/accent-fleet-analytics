"""Redis client wrapper with caching utilities.

Provides a simple key-value cache for:
- Dashboard data (60 second TTL)
- AI query results (5 minute TTL)
- User sessions
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import structlog

logger = structlog.get_logger("accent_fleet.cache")

_redis_client: Optional[Any] = None


def get_redis() -> Optional[Any]:
    """Get or create Redis client. Returns None if Redis is unavailable."""
    global _redis_client

    if _redis_client is not None:
        return _redis_client

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")

    try:
        import redis.asyncio as redis
        _redis_client = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        logger.info("redis.connected", url=redis_url)
        return _redis_client
    except ImportError:
        logger.warning("redis.not_installed")
        return None
    except Exception as e:
        logger.error("redis.connection_failed", error=str(e))
        return None


async def cache_get(key: str) -> Optional[Any]:
    """Get value from cache. Returns None if not found or Redis unavailable."""
    client = get_redis()
    if client is None:
        return None

    try:
        value = await client.get(key)
        if value:
            return json.loads(value)
        return None
    except Exception as e:
        logger.error("cache.get_failed", key=key, error=str(e))
        return None


async def cache_set(key: str, value: Any, ttl: int = 60) -> bool:
    """Set value in cache with TTL (seconds). Returns True if successful."""
    client = get_redis()
    if client is None:
        return False

    try:
        serialized = json.dumps(value)
        await client.setex(key, ttl, serialized)
        return True
    except Exception as e:
        logger.error("cache.set_failed", key=key, error=str(e))
        return False


async def cache_delete(key: str) -> bool:
    """Delete key from cache."""
    client = get_redis()
    if client is None:
        return False

    try:
        await client.delete(key)
        return True
    except Exception as e:
        logger.error("cache.delete_failed", key=key, error=str(e))
        return False
