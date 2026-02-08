from app.repositories.event_repository import event_repository
from app.models.event import EventCreate, EventResponse
from typing import List

class EventService:
    async def initialize_db(self):
        await event_repository.create_table()

    async def log_event(self, event_data: EventCreate) -> EventResponse:
        return await event_repository.create(event_data)

    async def get_recent_events(self, limit: int = 100) -> List[EventResponse]:
        return await event_repository.list_events(limit)

    async def seed_events(self):
        events = await self.get_recent_events(1)
        if not events:
            print("Seeding database with initial events...")
            seeds = [
                EventCreate(event_type="user_signup", payload={"email": "alice@example.com", "source": "campaign_spring"}),
                EventCreate(event_type="page_view", payload={"path": "/landing", "referrer": "google"}),
                EventCreate(event_type="item_view", payload={"item_id": "SKU-9982", "category": "electronics"}),
                EventCreate(event_type="add_to_cart", payload={"item_id": "SKU-9982", "quantity": 1}),
                EventCreate(event_type="checkout_start", payload={"cart_value": 199.50})
            ]
            for seed in seeds:
                await self.log_event(seed)
            print("Database seeded.")

event_service = EventService()
