from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from contextlib import asynccontextmanager
from app.db import db
from app.api import endpoints
from app.services.event_service import event_service
import sys

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Lifespan startup initialized")
    try:
        await db.connect()
        await event_service.initialize_db()
        await event_service.seed_events()
        print("Lifespan startup complete")
    except Exception as e:
        print(f"Lifespan startup failed: {e}")
        # We should probably exit if DB fails
        sys.exit(1)
    
    yield
    
    print("Lifespan shutdown initialized")
    await db.disconnect()
    print("Lifespan shutdown complete")

app = FastAPI(
    title="Event Analytics & Query Engine",
    description="Production-ready Python backend using FastAPI and asyncpg",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(endpoints.router, prefix="/api")

@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/docs")

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "event-analytics"}
