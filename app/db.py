import asyncpg
import os
from typing import Optional
from app.config import settings

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        print(f"Connecting to DB with URL: {settings.DATABASE_URL[:20]}...")
        try:
            self.pool = await asyncpg.create_pool(dsn=settings.DATABASE_URL)
            print("DB Pool created successfully")
        except Exception as e:
            print(f"Failed to create DB pool: {e}")
            raise e

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            print("DB Pool closed")

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
