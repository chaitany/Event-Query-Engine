from app.db import db
from app.models.event import EventCreate, EventResponse
from typing import List

class EventRepository:
    async def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(255) NOT NULL,
            payload JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        await db.execute(query)

    async def create(self, event: EventCreate) -> EventResponse:
        query = """
        INSERT INTO events (event_type, payload)
        VALUES ($1, $2)
        RETURNING id, event_type, payload, created_at;
        """
        # asyncpg handles dict to jsonb conversion automatically if configured, 
        # but sometimes needs explicit json.dumps. 
        # asyncpg's default codec for jsonb handles python dicts.
        import json
        payload_json = json.dumps(event.payload)
        
        row = await db.fetchrow(query, event.event_type, payload_json)
        
        # Need to parse the jsonb back to dict if asyncpg returns string
        # Typically asyncpg returns string for JSONB unless a codec is set.
        # Let's handle it safely.
        payload_data = row['payload']
        if isinstance(payload_data, str):
            payload_data = json.loads(payload_data)

        return EventResponse(
            id=row['id'],
            event_type=row['event_type'],
            payload=payload_data,
            created_at=row['created_at']
        )

    async def list_events(self, limit: int = 100) -> List[EventResponse]:
        query = """
        SELECT id, event_type, payload, created_at 
        FROM events 
        ORDER BY created_at DESC 
        LIMIT $1;
        """
        rows = await db.fetch(query, limit)
        import json
        
        results = []
        for row in rows:
            payload_data = row['payload']
            if isinstance(payload_data, str):
                payload_data = json.loads(payload_data)
                
            results.append(EventResponse(
                id=row['id'],
                event_type=row['event_type'],
                payload=payload_data,
                created_at=row['created_at']
            ))
        return results

event_repository = EventRepository()
