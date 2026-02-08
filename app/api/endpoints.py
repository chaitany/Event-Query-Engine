from fastapi import APIRouter, HTTPException, Query, Request
from app.models.event import EventCreate, EventResponse
from app.services.event_service import event_service
from typing import List, Union, Dict, Any
import time
from datetime import datetime, timedelta

router = APIRouter()

# Simple in-memory rate limiting (for demo purposes)
RATE_LIMIT = {}
MAX_REQUESTS = 100
WINDOW = 60 # seconds

def check_rate_limit(client_ip: str):
    now = time.time()
    if client_ip not in RATE_LIMIT:
        RATE_LIMIT[client_ip] = []
    
    # Clean old requests
    RATE_LIMIT[client_ip] = [t for t in RATE_LIMIT[client_ip] if now - t < WINDOW]
    
    if len(RATE_LIMIT[client_ip]) >= MAX_REQUESTS:
        return False
    
    RATE_LIMIT[client_ip].append(now)
    return True

@router.post("/events", status_code=202)
async def ingest_events(request: Request, events: Union[EventCreate, List[EventCreate]]):
    if not check_rate_limit(request.client.host):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    start_time = time.time()
    try:
        await event_service.ingest_events(events)
        latency = (time.time() - start_time) * 1000
        print(f"Ingestion API latency: {latency:.2f}ms")
        return {"status": "accepted", "ingested": len(events) if isinstance(events, list) else 1}
    except Exception as e:
        print(f"Ingestion failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/events", response_model=List[EventResponse])
async def list_events(limit: int = Query(100, ge=1, le=1000)):
    return await event_service.get_recent_events(limit)

# --- Analytics Endpoints ---

@router.get("/analytics/dau")
async def get_dau(
    start_date: str = Query(None),
    end_date: str = Query(None)
):
    if not start_date:
        start_date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    if not end_date:
        end_date = datetime.utcnow().isoformat()
    return await event_service.get_dau(start_date, end_date)

@router.get("/analytics/events-by-type")
async def get_events_by_type(
    start_date: str = Query(None),
    end_date: str = Query(None),
    event_type: str = Query(None)
):
    if not start_date:
        start_date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    if not end_date:
        end_date = datetime.utcnow().isoformat()
    return await event_service.get_events_by_type(start_date, end_date, event_type)

@router.get("/analytics/funnel")
async def get_funnel_analysis(
    start_date: str = Query(None),
    end_date: str = Query(None),
    steps: List[str] = Query(["user_signup", "page_view", "purchase"])
):
    if not start_date:
        start_date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    if not end_date:
        end_date = datetime.utcnow().isoformat()
    return await event_service.get_funnel_analysis(start_date, end_date, steps)
