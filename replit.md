# replit.md

## Overview

This is a **Python FastAPI backend** serving as an **Event Analytics & Query Engine**. It logs and queries events stored in PostgreSQL using asyncpg (raw SQL, no ORM). The Node.js entry point (`server/index.ts`) is a thin launcher that spawns a Python Uvicorn server on port 5000. The frontend scaffolding (React/Vite) exists from the project template but is not used — the backend is the primary artifact.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Overall Structure

- **`app/`** — Python FastAPI backend (the actual API server)
- **`server/`** — Node.js entry point that spawns the Python backend
- **`tests/`** — pytest test suite with isolated test schema
- **`client/`** — React frontend (template scaffolding, not actively used)
- **`shared/`** — Drizzle ORM schemas (legacy from template, not used by Python backend)

### Backend Architecture

- **Framework**: Python FastAPI with async support
- **Server**: Uvicorn (spawned by Node.js via `server/index.ts`)
- **Database Driver**: asyncpg (async PostgreSQL driver, raw SQL)
- **Configuration**: pydantic-settings loading from environment variables
- **Logging**: Python logging module with structured format (`event_analytics.*` hierarchy)
- **Architecture Pattern**: Controller → Service → Repository
  - `app/api/endpoints.py` — Thin route handlers (HTTP concerns only)
  - `app/services/event_service.py` — Business logic orchestration
  - `app/repositories/event_repository.py` — SQL queries, caching, materialized views
  - `app/models/event.py` — Pydantic models for request/response validation
  - `app/db.py` — asyncpg connection pool (singleton)
  - `app/config.py` — Env-based config + logging configuration
  - `app/main.py` — FastAPI app, lifespan, middleware, global error handler
- **API Prefix**: All API routes are under `/api`
- **Health Check**: `GET /health`
- **Startup**: Connects to DB, creates schema (idempotent), seeds demo data, refreshes MVs

### Database

- **PostgreSQL** via `DATABASE_URL` environment variable
- **Python-side schema is authoritative** — creates `users` and `events` tables via SQL in `event_repository.py`
- **Materialized views**: `mv_dau_daily`, `mv_events_by_type` with unique indexes for concurrent refresh
- **Indexes**: GIN on JSONB payload, composite B-tree on (type, time) and (user_id, time)
- **Drizzle schema** in `shared/` is vestigial from the Node.js template — not used

### Performance Features

- Materialized views for heavy analytics aggregations
- 5-minute in-memory cache per query signature
- asyncio.wait_for timeouts (5s analytics, 10s funnel) with fallback to raw queries
- Sliding-window rate limiting (100 req/min per IP)
- Bulk ingestion via executemany in a single transaction

### Key Files

| File | Purpose |
|------|---------|
| `server/index.ts` | Entry point — spawns Python Uvicorn |
| `app/main.py` | FastAPI app, lifespan, middleware, error handler |
| `app/api/endpoints.py` | API route handlers |
| `app/services/event_service.py` | Business logic |
| `app/repositories/event_repository.py` | SQL queries, caching, MVs |
| `app/db.py` | asyncpg connection pool |
| `app/models/event.py` | Pydantic models |
| `app/config.py` | Environment config + logging setup |
| `tests/conftest.py` | Test fixtures (isolated schema, pool, ASGI client) |
| `README.md` | Comprehensive project documentation |

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `LOG_LEVEL` | No | `INFO` | Python logging level |

### Testing

- **pytest** + **pytest-asyncio** + **httpx**
- Tests use isolated `test_analytics` PostgreSQL schema
- Run: `python -m pytest tests/ -v`
- 25 tests: database operations (6), API endpoints (12), analytics queries (7)

### Recent Changes

- 2026-02-08: Added structured logging (replaced all print statements with Python logging module)
- 2026-02-08: Added global exception handler and per-endpoint error handling (timeouts → 504, server errors → 500)
- 2026-02-08: Added request logging middleware with latency tracking
- 2026-02-08: Created comprehensive README.md for portfolio presentation
- 2026-02-08: Completed test suite with 25 tests across 3 files
- 2026-02-08: Implemented materialized views, in-memory caching, query timeouts
