import logging
import asyncpg
from serving.config import POSTGRES_DSN, POSTGRES_POOL_MIN_SIZE, POSTGRES_POOL_MAX_SIZE

logger = logging.getLogger("serving.db_async")

class AsyncPostgresPool:
    def __init__(self):
        self._pool = None

    async def connect(self):
        if not self._pool:
            logger.info("Initializing asyncpg connection pool")
            self._pool = await asyncpg.create_pool(
                dsn=POSTGRES_DSN,
                min_size=POSTGRES_POOL_MIN_SIZE,
                max_size=POSTGRES_POOL_MAX_SIZE,
            )

    async def close(self):
        if self._pool:
            await self._pool.close()

    def acquire(self):
        if not self._pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
        return self._pool.acquire()

db_pool = AsyncPostgresPool()
