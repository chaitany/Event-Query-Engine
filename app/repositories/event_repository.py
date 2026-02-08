from app.db import db
from app.models.event import EventCreate, EventResponse, UserCreate, UserResponse
from typing import List
import json

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

        # Events table optimized for high-volume writes and analytical queries
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
        
        # Optimization: GIN index for JSONB payload queries
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_payload ON events USING GIN (payload);")
        
        # Optimization: Composite index for time-based analytical queries
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, timestamp DESC);")
        
        # Index for user-based analysis
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
