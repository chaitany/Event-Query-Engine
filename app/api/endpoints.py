from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from app.models.event import EventCreate, EventResponse
from app.services.event_service import event_service
from typing import List, Union, Dict, Any
import time
import logging
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger("event_analytics.api")

router = APIRouter()

RATE_LIMIT: Dict[str, List[float]] = {}
MAX_REQUESTS = 100
WINDOW = 60


def check_rate_limit(client_ip: str) -> bool:
    now = time.time()
    if client_ip not in RATE_LIMIT:
        RATE_LIMIT[client_ip] = []
    RATE_LIMIT[client_ip] = [t for t in RATE_LIMIT[client_ip] if now - t < WINDOW]
    if len(RATE_LIMIT[client_ip]) >= MAX_REQUESTS:
        return False
    RATE_LIMIT[client_ip].append(now)
    return True


@router.post("/events", status_code=202)
async def ingest_events(request: Request, events: Union[EventCreate, List[EventCreate]]):
    client_ip = request.client.host if request.client else "unknown"
    if not check_rate_limit(client_ip):
        logger.warning("Rate limit exceeded for %s", client_ip)
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    start_time = time.time()
    try:
        await event_service.ingest_events(events)
        latency = (time.time() - start_time) * 1000
        count = len(events) if isinstance(events, list) else 1
        logger.info("Ingested %d event(s) in %.1fms from %s", count, latency, client_ip)
        return {"status": "accepted", "ingested": count}
    except Exception as e:
        logger.error("Ingestion failed: %s", e, exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/events", response_model=List[EventResponse])
async def list_events(limit: int = Query(100, ge=1, le=1000)):
    return await event_service.get_recent_events(limit)


@router.get("/analytics/dau")
async def get_dau(start_date: datetime = Query(None), end_date: datetime = Query(None)):
    start_date = start_date or (datetime.utcnow() - timedelta(days=30))
    end_date = end_date or datetime.utcnow()
    try:
        return await event_service.get_dau(start_date, end_date)
    except asyncio.TimeoutError:
        logger.error("DAU query timed out for range %s to %s", start_date, end_date)
        raise HTTPException(status_code=504, detail="Query timed out")
    except Exception as e:
        logger.error("DAU query failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Analytics query failed")


@router.get("/analytics/events-by-type")
async def get_events_by_type(start_date: datetime = Query(None), end_date: datetime = Query(None), event_type: str = Query(None)):
    start_date = start_date or (datetime.utcnow() - timedelta(days=30))
    end_date = end_date or datetime.utcnow()
    try:
        return await event_service.get_events_by_type(start_date, end_date, event_type)
    except asyncio.TimeoutError:
        logger.error("Events-by-type query timed out")
        raise HTTPException(status_code=504, detail="Query timed out")
    except Exception as e:
        logger.error("Events-by-type query failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Analytics query failed")


@router.get("/analytics/funnel")
async def get_funnel_analysis(start_date: datetime = Query(None), end_date: datetime = Query(None), steps: List[str] = Query(["user_signup", "page_view", "purchase"])):
    start_date = start_date or (datetime.utcnow() - timedelta(days=30))
    end_date = end_date or datetime.utcnow()
    try:
        return await event_service.get_funnel_analysis(start_date, end_date, steps)
    except asyncio.TimeoutError:
        logger.error("Funnel query timed out for steps %s", steps)
        raise HTTPException(status_code=504, detail="Query timed out")
    except Exception as e:
        logger.error("Funnel query failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Analytics query failed")


@router.post("/admin/refresh-metrics")
async def refresh_metrics():
    try:
        await event_service.refresh_metrics()
        return {"status": "success", "message": "Materialized views refreshed"}
    except Exception as e:
        logger.error("Metrics refresh failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Refresh failed: {str(e)}")
