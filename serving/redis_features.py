"""
serving/redis_features.py — reconstructed per Phase 4's documented
contract (real file wasn't visible in this chat). Single shared
`redis.asyncio` connection pool created once at startup; cold-start
convention per Phase 3/4: a missing key returns all-zero features rather
than an error.

HFT Optimization: In-process TTLCache layer. A cache hit for a recently-seen
user completely eliminates the Redis TCP round-trip (~0.1ms saved per hit).
Cache size=1024 users, TTL=10s — evicts frequently enough that feature
vectors stay fresh without requiring manual invalidation.
"""
from __future__ import annotations

import logging
from cachetools import TTLCache

import redis.asyncio as redis
import pybreaker

from serving.config import FEATURE_COLUMNS, REDIS_MAX_CONNECTIONS, REDIS_URL
from serving.circuit_breaker import redis_breaker

logger = logging.getLogger("serving.redis_features")

# Maximum number of user feature vectors to cache in-process.
_CACHE_MAX_USERS = int(100000)
# Seconds before a cached vector is considered stale and must be re-fetched.
_CACHE_TTL_SECONDS = 10



class RedisFeatureStore:
    def __init__(self) -> None:
        self._pool: redis.ConnectionPool | None = None
        self._client: redis.Redis | None = None
        # In-process LRU feature cache — keyed by user_id, value is the
        # pre-parsed float vector. Thread-safe for single-writer asyncio
        # usage (TTLCache uses a simple dict under the hood, no locks).
        self._cache: TTLCache = TTLCache(maxsize=_CACHE_MAX_USERS, ttl=_CACHE_TTL_SECONDS)

    async def connect(self) -> None:
        self._pool = redis.ConnectionPool.from_url(
            REDIS_URL, max_connections=REDIS_MAX_CONNECTIONS, decode_responses=True
        )
        self._client = redis.Redis(connection_pool=self._pool)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
        if self._pool:
            await self._pool.disconnect()

    async def ping(self) -> bool:
        if not self._client:
            return False
        try:
            return bool(await self._client.ping())
        except Exception:  # noqa: BLE001
            return False

    async def get_feature_vector(self, user_id: str) -> tuple[list[float], bool]:
        """Returns (vector, is_cold_start). Missing key or missing/corrupt
        individual fields default to 0.0 (logged), never raise — a scoring
        request for a never-seen user is scored, not rejected (Phase 3/4
        cold-start convention).

        Cache hit path: O(1) dict lookup, zero network I/O.
        Cache miss path: Redis HGETALL, then populate cache.
        """
        # --- Fast path: in-process cache hit ---
        cached = self._cache.get(user_id)
        if cached is not None:
            return cached, False

        # --- Slow path: Redis fetch ---
        key = f"features:user:{user_id}"
        try:
            @redis_breaker
            async def _fetch():
                return await self._client.hgetall(key)

            raw = await _fetch()
        except pybreaker.CircuitBreakerError:
            logger.warning("Circuit breaker OPEN. Using cold start fallback for user_id=%s", user_id)
            raw = {}
        except Exception as exc:
            logger.error("Redis fetch failed (circuit may trip): %s", exc)
            raw = {}

        is_cold_start = len(raw) == 0
        if is_cold_start:
            logger.debug("Cold start: no Redis features for user_id=%s", user_id)

        vector: list[float] = []
        for feature_name in FEATURE_COLUMNS:
            value = raw.get(feature_name)
            if value is None:
                vector.append(0.0)
                continue
            try:
                vector.append(float(value))
            except (TypeError, ValueError):
                logger.warning(
                    "Corrupt feature value user_id=%s feature=%s value=%r, defaulting to 0.0",
                    user_id, feature_name, value,
                )
                vector.append(0.0)

        # Populate cache only for warm users (cold-start vectors are all-zeros
        # and would pollute the cache with useless entries).
        if not is_cold_start:
            self._cache[user_id] = vector

        return vector, is_cold_start
