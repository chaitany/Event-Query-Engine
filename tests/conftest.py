import pytest
import pytest_asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from httpx import AsyncClient, ASGITransport

DATABASE_URL = os.environ["DATABASE_URL"]
TEST_SCHEMA = "test_analytics"


async def _init_conn(conn):
    await conn.execute(f"SET search_path TO {TEST_SCHEMA};")


async def _setup_schema_once(pool):
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1);",
            TEST_SCHEMA
        )
        if exists:
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")
            await conn.execute("DROP MATERIALIZED VIEW IF EXISTS mv_dau_daily CASCADE;")
            await conn.execute("DROP MATERIALIZED VIEW IF EXISTS mv_events_by_type CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS events CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE;")
        else:
            await conn.execute(f"CREATE SCHEMA {TEST_SCHEMA};")
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")

        await conn.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("""
            CREATE TABLE events (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(255) NOT NULL,
                user_id INTEGER REFERENCES users(id),
                payload JSONB DEFAULT '{}'::jsonb,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("CREATE INDEX idx_te_payload ON events USING GIN (payload);")
        await conn.execute("CREATE INDEX idx_te_type_time ON events (event_type, timestamp DESC);")
        await conn.execute("CREATE INDEX idx_te_user_time ON events (user_id, timestamp DESC);")
        await conn.execute("""
            CREATE MATERIALIZED VIEW mv_dau_daily AS
            SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(DISTINCT user_id) AS dau
            FROM events WHERE user_id IS NOT NULL GROUP BY 1 WITH NO DATA;
        """)
        await conn.execute("CREATE UNIQUE INDEX idx_te_mv_dau_day ON mv_dau_daily (day);")
        await conn.execute("""
            CREATE MATERIALIZED VIEW mv_events_by_type AS
            SELECT event_type, COUNT(*) AS count FROM events GROUP BY event_type WITH NO DATA;
        """)
        await conn.execute("CREATE UNIQUE INDEX idx_te_mv_event_type ON mv_events_by_type (event_type);")


async def _insert_seed(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM events;")
        await conn.execute("DELETE FROM users;")

        user = await conn.fetchrow(
            "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id, username, email, created_at;",
            "test_user", "test@example.com"
        )
        user_id = user["id"]

        now = datetime.utcnow()
        events = [
            ("user_signup", user_id, '{"source": "organic"}', now - timedelta(days=2)),
            ("page_view", user_id, '{"path": "/home"}', now - timedelta(days=2)),
            ("page_view", user_id, '{"path": "/products"}', now - timedelta(days=1)),
            ("purchase", user_id, '{"amount": 49.99, "currency": "USD"}', now - timedelta(days=1)),
            ("page_view", user_id, '{"path": "/checkout"}', now),
            ("purchase", user_id, '{"amount": 29.99, "currency": "USD"}', now),
        ]
        for etype, uid, payload, ts in events:
            await conn.execute(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, $2, $3::jsonb, $4);",
                etype, uid, payload, ts
            )

        await conn.execute("REFRESH MATERIALIZED VIEW mv_dau_daily;")
        await conn.execute("REFRESH MATERIALIZED VIEW mv_events_by_type;")
    return {"user_id": user_id, "user": dict(user), "event_count": len(events)}


@pytest_asyncio.fixture
async def test_pool():
    pool = await asyncpg.create_pool(
        dsn=DATABASE_URL, init=_init_conn,
        statement_cache_size=0, min_size=1, max_size=5
    )
    await _setup_schema_once(pool)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def db_conn(test_pool):
    conn = await test_pool.acquire()
    yield conn
    await test_pool.release(conn)


@pytest_asyncio.fixture
async def seed_data(test_pool):
    return await _insert_seed(test_pool)


@pytest_asyncio.fixture
async def app_client(test_pool, seed_data):
    from app.db import db as app_db
    app_db.pool = test_pool

    from app.repositories.event_repository import event_repository
    event_repository._cache = {}

    from app.main import app
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
