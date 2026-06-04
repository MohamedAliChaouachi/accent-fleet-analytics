"""Redis caching layer for dashboard and AI queries."""

from app.cache.redis_client import get_redis, cache_get, cache_set, cache_delete

__all__ = [get_redis, cache_get, cache_set, cache_delete]
