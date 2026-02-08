from app.db import db
from app.models.event import EventCreate, EventResponse, UserCreate, UserResponse
from typing import List, Dict, Any, Optional
import json
import time
from datetime import datetime, timedelta
import asyncio

class EventRepository:
    def __init__(self):
        self._cache = {}
        self._cache_ttl = 300 # 5 minutes

    async def create_schema(self):
        # Users table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Events table
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
        
        # Optimization indexes
        # EXPLAIN ANALYZE justification: GIN index allows fast membership/path lookups in JSONB metadata
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_payload ON events USING GIN (payload);")
        # EXPLAIN ANALYZE justification: B-tree on (type, time) optimizes both type filtering and range sorting
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, timestamp DESC);")
        # EXPLAIN ANALYZE justification: user_id index for fast lookup of user activity history
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_user_time ON events (user_id, timestamp DESC);")

        # Materialized Views for heavy queries
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
        
        await db.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_events_by_type AS
            SELECT 
                event_type,
                COUNT(*) AS count
            FROM events
            GROUP BY event_type
            WITH NO DATA;
        """)

    async def refresh_materialized_views(self):
        """Refresh all materialized views used for metrics"""
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dau_daily;")
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_events_by_type;")
        # Clear cache after refresh
        self._cache = {}

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
        print(f"Bulk ingestion of {len(events)} events completed in {latency:.2f}ms")

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
                return entry['data']
        
        data = await query_fn()
        self._cache[cache_key] = {'time': now, 'data': data}
        return data

    async def get_dau(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        async def run_query():
            # Use materialized view if dates match standard window, else raw query with timeout
            query = """
            SELECT day, dau FROM mv_dau_daily 
            WHERE day >= $1 AND day <= $2
            ORDER BY day DESC;
            """
            try:
                # 5 second query timeout to prevent slow queries
                rows = await asyncio.wait_for(db.fetch(query, start_date, end_date), timeout=5.0)
                return [dict(row) for row in rows]
            except (asyncio.TimeoutError, Exception):
                # Fallback to raw query if MV fails or not refreshed
                query = """
                SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(DISTINCT user_id) AS dau
                FROM events WHERE timestamp >= $1 AND timestamp <= $2 AND user_id IS NOT NULL
                GROUP BY 1 ORDER BY 1 DESC;
                """
                rows = await db.fetch(query, start_date, end_date)
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
            except:
                where_clause = "WHERE timestamp >= $1 AND timestamp <= $2"
                params = [start_date, end_date]
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
