import asyncpg
import logging
from typing import Optional
from app.config import settings

logger = logging.getLogger("event_analytics.db")


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        dsn_preview = settings.DATABASE_URL[:20] + "..."
        logger.info("Connecting to database: %s", dsn_preview)
        try:
            self.pool = await asyncpg.create_pool(dsn=settings.DATABASE_URL)
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.critical("Failed to create database pool: %s", e, exc_info=True)
            raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def execute(self, query: str, *args):
        if not self.pool:
            raise RuntimeError("Database not connected")
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetch(self, query: str, *args):
        if not self.pool:
            raise RuntimeError("Database not connected")
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        if not self.pool:
            raise RuntimeError("Database not connected")
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args)


db = Database()
