from app.db import db
from app.models.event import EventCreate, EventResponse, UserCreate, UserResponse
from typing import List, Dict, Any, Optional
import json
import time
import logging
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger("event_analytics.repository")


class EventRepository:
    def __init__(self):
        self._cache: Dict[str, Dict] = {}
        self._cache_ttl = 300

    async def create_schema(self):
        logger.info("Creating database schema (tables, indexes, materialized views)")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(255) NOT NULL,
                user_id INTEGER REFERENCES users(id),
                payload JSONB DEFAULT '{}'::jsonb,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_payload ON events USING GIN (payload);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, timestamp DESC);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_user_time ON events (user_id, timestamp DESC);")

        await db.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dau_daily AS
            SELECT 
                DATE_TRUNC('day', timestamp) AS day,
                COUNT(DISTINCT user_id) AS dau
            FROM events
            WHERE user_id IS NOT NULL
            GROUP BY 1
            WITH NO DATA;
        """)
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_dau_day ON mv_dau_daily (day);")

        await db.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_events_by_type AS
            SELECT 
                event_type,
                COUNT(*) AS count
            FROM events
            GROUP BY event_type
            WITH NO DATA;
        """)
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_event_type ON mv_events_by_type (event_type);")

        logger.info("Schema creation complete")

    async def refresh_materialized_views(self):
        logger.info("Refreshing materialized views concurrently")
        start = time.time()
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dau_daily;")
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_events_by_type;")
        self._cache = {}
        elapsed = (time.time() - start) * 1000
        logger.info("Materialized views refreshed in %.1fms", elapsed)

    async def create_user(self, user: UserCreate) -> UserResponse:
        query = """
        INSERT INTO users (username, email)
        VALUES ($1, $2)
        ON CONFLICT (username) DO UPDATE SET email = EXCLUDED.email
        RETURNING id, username, email, created_at;
        """
        row = await db.fetchrow(query, user.username, user.email)
        return UserResponse(
            id=row['id'],
            username=row['username'],
            email=row['email'],
            created_at=row['created_at']
        )

    async def create_event(self, event: EventCreate) -> EventResponse:
        query = """
        INSERT INTO events (event_type, user_id, payload, timestamp)
        VALUES ($1, $2, $3, $4)
        RETURNING id, event_type, user_id, payload, timestamp, created_at;
        """
        payload_json = json.dumps(event.payload)
        row = await db.fetchrow(query, event.event_type, event.user_id, payload_json, event.timestamp)

        payload_data = row['payload']
        if isinstance(payload_data, str):
            payload_data = json.loads(payload_data)

        return EventResponse(
            id=row['id'],
            event_type=row['event_type'],
            user_id=row['user_id'],
            payload=payload_data,
            timestamp=row['timestamp'],
            created_at=row['created_at']
        )

    async def bulk_insert_events(self, events: List[EventCreate]):
        start_time = time.time()
        async with db.pool.acquire() as connection:
            async with connection.transaction():
                records = [
                    (e.event_type, e.user_id, json.dumps(e.payload), e.timestamp)
                    for e in events
                ]
                await connection.executemany("""
                    INSERT INTO events (event_type, user_id, payload, timestamp)
                    VALUES ($1, $2, $3, $4)
                """, records)

        latency = (time.time() - start_time) * 1000
        logger.info("Bulk ingestion: %d events in %.1fms", len(events), latency)

    async def list_events(self, limit: int = 100) -> List[EventResponse]:
        query = """
        SELECT id, event_type, user_id, payload, timestamp, created_at 
        FROM events 
        ORDER BY timestamp DESC 
        LIMIT $1;
        """
        rows = await db.fetch(query, limit)
        results = []
        for row in rows:
            payload_data = row['payload']
            if isinstance(payload_data, str):
                payload_data = json.loads(payload_data)

            results.append(EventResponse(
                id=row['id'],
                event_type=row['event_type'],
                user_id=row['user_id'],
                payload=payload_data,
                timestamp=row['timestamp'],
                created_at=row['created_at']
            ))
        return results

    async def _get_cached_query(self, cache_key: str, query_fn):
        now = time.time()
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            if now - entry['time'] < self._cache_ttl:
                logger.debug("Cache hit: %s", cache_key)
                return entry['data']

        data = await query_fn()
        self._cache[cache_key] = {'time': now, 'data': data}
        return data

    async def get_dau(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        async def run_query():
            query = """
            SELECT day, dau FROM mv_dau_daily 
            WHERE day >= $1 AND day <= $2
            ORDER BY day DESC;
            """
            try:
                rows = await asyncio.wait_for(db.fetch(query, start_date, end_date), timeout=5.0)
                return [dict(row) for row in rows]
            except asyncio.TimeoutError:
                logger.warning("DAU materialized view query timed out, falling back to raw query")
                fallback = """
                SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(DISTINCT user_id) AS dau
                FROM events WHERE timestamp >= $1 AND timestamp <= $2 AND user_id IS NOT NULL
                GROUP BY 1 ORDER BY 1 DESC;
                """
                rows = await db.fetch(fallback, start_date, end_date)
                return [dict(row) for row in rows]
            except Exception:
                logger.warning("DAU materialized view unavailable, using raw query", exc_info=True)
                fallback = """
                SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(DISTINCT user_id) AS dau
                FROM events WHERE timestamp >= $1 AND timestamp <= $2 AND user_id IS NOT NULL
                GROUP BY 1 ORDER BY 1 DESC;
                """
                rows = await db.fetch(fallback, start_date, end_date)
                return [dict(row) for row in rows]

        return await self._get_cached_query(f"dau_{start_date}_{end_date}", run_query)

    async def get_events_by_type(self, start_date: datetime, end_date: datetime, event_type: str = None) -> List[Dict[str, Any]]:
        async def run_query():
            query = """
            SELECT event_type, count FROM mv_events_by_type
            WHERE event_type = COALESCE($1, event_type)
            ORDER BY count DESC;
            """
            try:
                rows = await asyncio.wait_for(db.fetch(query, event_type), timeout=5.0)
                return [dict(row) for row in rows]
            except asyncio.TimeoutError:
                logger.warning("Events-by-type MV query timed out, falling back to raw query")
            except Exception:
                logger.warning("Events-by-type MV unavailable, using raw query", exc_info=True)

            where_clause = "WHERE timestamp >= $1 AND timestamp <= $2"
            params: list = [start_date, end_date]
            if event_type:
                where_clause += " AND event_type = $3"
                params.append(event_type)
            query = f"SELECT event_type, COUNT(*) AS count FROM events {where_clause} GROUP BY event_type ORDER BY count DESC;"
            rows = await db.fetch(query, *params)
            return [dict(row) for row in rows]

        return await self._get_cached_query(f"events_by_type_{start_date}_{end_date}_{event_type}", run_query)

    async def get_funnel_analysis(self, start_date: datetime, end_date: datetime, funnel_steps: List[str]) -> List[Dict[str, Any]]:
        if not funnel_steps:
            return []

        query = """
        WITH user_steps AS (
            SELECT user_id, event_type, MIN(timestamp) AS first_step_time
            FROM events WHERE timestamp >= $1 AND timestamp <= $2 AND event_type = ANY($3) AND user_id IS NOT NULL
            GROUP BY user_id, event_type
        ),
        funnel_agg AS (
            SELECT user_id,
                """ + ",\n".join([f"MIN(CASE WHEN event_type = '{step}' THEN first_step_time END) AS step_{i}" for i, step in enumerate(funnel_steps)]) + """
            FROM user_steps GROUP BY user_id
        )
        SELECT """ + ",\n".join([f"COUNT(step_{i}) AS count_step_{i}" for i in range(len(funnel_steps))]) + " FROM funnel_agg;"

        row = await asyncio.wait_for(db.fetchrow(query, start_date, end_date, funnel_steps), timeout=10.0)

        result = []
        for i, step in enumerate(funnel_steps):
            count = row[f"count_step_{i}"]
            result.append({
                "step_name": step,
                "count": count,
                "conversion_rate": (count / row["count_step_0"] * 100) if row["count_step_0"] > 0 else 0
            })
        return result


event_repository = EventRepository()
