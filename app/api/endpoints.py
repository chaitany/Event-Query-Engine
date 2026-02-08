from fastapi import APIRouter, HTTPException, Query
from app.models.event import EventCreate, EventResponse
from app.services.event_service import event_service
from typing import List

router = APIRouter()

@router.post("/events", response_model=EventResponse, status_code=201)
async def create_event(event: EventCreate):
    return await event_service.log_event(event)

@router.get("/events", response_model=List[EventResponse])
async def list_events(limit: int = Query(100, ge=1, le=1000)):
    return await event_service.get_recent_events(limit)
