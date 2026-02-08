from app.repositories.event_repository import event_repository
from app.models.event import EventCreate, EventResponse, UserCreate, UserResponse
from typing import List, Union, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger("event_analytics.service")


class EventService:
    async def initialize_db(self):
        await event_repository.create_schema()
        try:
            await db.execute("REFRESH MATERIALIZED VIEW mv_dau_daily;")
            await db.execute("REFRESH MATERIALIZED VIEW mv_events_by_type;")
            logger.info("Initial materialized view refresh complete")
        except Exception:
            logger.debug("Skipped initial MV refresh (views may have no data yet)")

    async def create_user(self, user_data: UserCreate) -> UserResponse:
        return await event_repository.create_user(user_data)

    async def log_event(self, event_data: EventCreate) -> EventResponse:
        return await event_repository.create_event(event_data)

    async def ingest_events(self, events: Union[EventCreate, List[EventCreate]]):
        if isinstance(events, list):
            await event_repository.bulk_insert_events(events)
        else:
            await event_repository.create_event(events)

    async def get_recent_events(self, limit: int = 100) -> List[EventResponse]:
        return await event_repository.list_events(limit)

    async def get_dau(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        return await event_repository.get_dau(start_date, end_date)

    async def get_events_by_type(self, start_date: datetime, end_date: datetime, event_type: str = None) -> List[Dict[str, Any]]:
        return await event_repository.get_events_by_type(start_date, end_date, event_type)

    async def get_funnel_analysis(self, start_date: datetime, end_date: datetime, funnel_steps: List[str]) -> List[Dict[str, Any]]:
        return await event_repository.get_funnel_analysis(start_date, end_date, funnel_steps)

    async def refresh_metrics(self):
        await event_repository.refresh_materialized_views()

    async def seed_events(self):
        try:
            user = await self.create_user(UserCreate(username="demo_user", email="demo@example.com"))
            events = await self.get_recent_events(1)
            if not events:
                logger.info("Seeding database with demo events")
                seeds = [
                    EventCreate(event_type="user_signup", user_id=user.id, payload={"source": "campaign_spring"}, timestamp=datetime.utcnow()),
                    EventCreate(event_type="page_view", user_id=user.id, payload={"path": "/landing"}, timestamp=datetime.utcnow()),
                    EventCreate(event_type="purchase", user_id=user.id, payload={"amount": 99.99, "currency": "USD"}, timestamp=datetime.utcnow())
                ]
                await self.ingest_events(seeds)
                await self.refresh_metrics()
                logger.info("Database seeded and metrics refreshed")
            else:
                logger.debug("Database already contains events, skipping seed")
        except Exception as e:
            logger.warning("Seeding skipped or failed: %s", e)


from app.db import db
event_service = EventService()
