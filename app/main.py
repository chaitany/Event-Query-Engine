from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from contextlib import asynccontextmanager
from app.db import db
from app.api import endpoints
from app.services.event_service import event_service
from app.config import settings, configure_logging
import logging
import sys
import time

configure_logging()
logger = logging.getLogger("event_analytics.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated")
    try:
        await db.connect()
        await event_service.initialize_db()
        await event_service.seed_events()
        logger.info("Application startup complete")
    except Exception as e:
        logger.critical("Startup failed: %s", e, exc_info=True)
        sys.exit(1)

    yield

    logger.info("Application shutdown initiated")
    await db.disconnect()
    logger.info("Application shutdown complete")


app = FastAPI(
    title="Event Analytics & Query Engine",
    description=(
        "Production-ready event ingestion and analytics API built with FastAPI and asyncpg. "
        "Supports event logging, DAU metrics, funnel analysis, and materialized-view-backed aggregations."
    ),
    version="1.0.0",
    lifespan=lifespan
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


app.include_router(endpoints.router, prefix="/api")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed = (time.time() - start) * 1000
    logger.info(
        "%s %s %d %.1fms",
        request.method,
        request.url.path,
        response.status_code,
        elapsed,
    )
    return response


@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/docs")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "event-analytics"}
