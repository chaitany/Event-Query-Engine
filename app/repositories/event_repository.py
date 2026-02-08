from app.db import db
from app.models.event import EventCreate, EventResponse, UserCreate, UserResponse
from typing import List
import json
import time

class EventRepository:
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
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_payload ON events USING GIN (payload);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, timestamp DESC);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_user_time ON events (user_id, timestamp DESC);")

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
        # Prepare data for bulk insert using COPY or transaction
        # asyncpg.connection.copy_records_to_table is fast but needs raw records
        # We'll use a transaction with multiple inserts as a fallback or executemany
        async with db.pool.acquire() as connection:
            async with connection.transaction():
                # Prepare records
                records = [
                    (e.event_type, e.user_id, json.dumps(e.payload), e.timestamp)
                    for e in events
                ]
                # Using executemany for batch insertion
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

event_repository = EventRepository()
