# Event Analytics & Query Engine

A production-ready event ingestion and analytics API built with **Python 3.11**, **FastAPI**, and **asyncpg** (raw SQL, no ORM). Designed to demonstrate clean backend architecture, async PostgreSQL patterns, and practical scalability tradeoffs.

---

## Architecture

```
app/
  main.py                  FastAPI app, lifespan, middleware, global error handler
  config.py                pydantic-settings for env-based configuration + logging setup
  db.py                    asyncpg connection pool (singleton)
  models/
    event.py               Pydantic request/response models
  api/
    endpoints.py           Route handlers (thin controller layer)
  services/
    event_service.py       Business logic orchestration
  repositories/
    event_repository.py    Raw SQL queries, caching, materialized view management

tests/
  conftest.py              Fixtures: isolated test schema, pool, ASGI client, seed data
  test_database.py         Direct database operation tests
  test_endpoints.py        API endpoint integration tests
  test_analytics.py        Analytics query + materialized view tests

server/
  index.ts                 Node.js entry point that spawns uvicorn on port 5000
```

### Layered Design

The backend follows a strict **Controller → Service → Repository** pattern:

- **Endpoints** (`api/endpoints.py`) handle HTTP concerns: parsing, validation, rate limiting, status codes. No SQL here.
- **Service** (`services/event_service.py`) orchestrates business logic and delegates to the repository. Thin by design — the analytics domain doesn't demand complex orchestration.
- **Repository** (`repositories/event_repository.py`) owns all SQL, caching, materialized view management, and query timeout logic. This is the heaviest layer.

This separation makes the repository swappable (e.g., for a different data store) without touching HTTP or business logic.

---

## Database Schema

PostgreSQL with raw SQL via asyncpg. No ORM — intentional choice for full control over query optimization, JSONB indexing, and materialized views.

### Tables

```sql
CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(255) UNIQUE NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE events (
    id            SERIAL PRIMARY KEY,
    event_type    VARCHAR(255) NOT NULL,
    user_id       INTEGER REFERENCES users(id),
    payload       JSONB DEFAULT '{}'::jsonb,
    timestamp     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### Indexes

| Index | Type | Column(s) | Rationale |
|-------|------|-----------|-----------|
| `idx_events_payload` | GIN | `payload` | Fast JSONB containment/path queries (`@>`, `?`, `->`) |
| `idx_events_type_time` | B-tree | `(event_type, timestamp DESC)` | Composite index for type filtering + time-range scans |
| `idx_events_user_time` | B-tree | `(user_id, timestamp DESC)` | User activity lookups ordered by recency |

### Materialized Views

```sql
-- Daily active users, refreshed via admin endpoint
CREATE MATERIALIZED VIEW mv_dau_daily AS
SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(DISTINCT user_id) AS dau
FROM events WHERE user_id IS NOT NULL GROUP BY 1;

-- Event counts by type
CREATE MATERIALIZED VIEW mv_events_by_type AS
SELECT event_type, COUNT(*) AS count FROM events GROUP BY event_type;
```

Both have unique indexes to support `REFRESH MATERIALIZED VIEW CONCURRENTLY`, which avoids locking the view during refresh.

---

## API Reference

### Health Check

```
GET /health
```

Returns `{"status": "ok", "service": "event-analytics"}`. No database dependency — always responds.

### Event Ingestion

```
POST /api/events
```

Accepts a single event or a JSON array of events. Rate-limited to 100 requests per IP per 60 seconds.

**Single event:**
```json
{
  "event_type": "page_view",
  "user_id": 1,
  "payload": {"path": "/dashboard", "referrer": "google.com"},
  "timestamp": "2026-02-08T12:00:00Z"
}
```

**Batch:**
```json
[
  {"event_type": "page_view", "user_id": 1, "payload": {"path": "/home"}},
  {"event_type": "purchase", "user_id": 1, "payload": {"amount": 49.99}}
]
```

Response: `202 Accepted` with `{"status": "accepted", "ingested": <count>}`

### List Events

```
GET /api/events?limit=50
```

Returns recent events (newest first). `limit` range: 1–1000, default 100.

### Analytics: Daily Active Users

```
GET /api/analytics/dau?start_date=2026-01-01T00:00:00Z&end_date=2026-02-08T23:59:59Z
```

Returns daily unique user counts. Uses materialized view with 5-second timeout + automatic fallback to raw query.

### Analytics: Events by Type

```
GET /api/analytics/events-by-type?event_type=purchase
```

Optional `event_type` filter. Date range parameters default to last 30 days.

### Analytics: Funnel Analysis

```
GET /api/analytics/funnel?steps=user_signup&steps=page_view&steps=purchase
```

Ordered funnel with per-step counts and conversion rates relative to step 0. Uses a CTE-based query that tracks each user's first occurrence of each step type.

### Admin: Refresh Metrics

```
POST /api/admin/refresh-metrics
```

Triggers concurrent refresh of all materialized views. Clears the in-memory cache.

### OpenAPI Docs

Interactive Swagger UI at `/docs` and ReDoc at `/redoc`.

---

## Sample curl Commands

```bash
# Health check
curl http://localhost:5000/health

# Ingest a single event
curl -X POST http://localhost:5000/api/events \
  -H "Content-Type: application/json" \
  -d '{"event_type": "page_view", "user_id": 1, "payload": {"path": "/pricing"}}'

# Batch ingest
curl -X POST http://localhost:5000/api/events \
  -H "Content-Type: application/json" \
  -d '[
    {"event_type": "user_signup", "user_id": 2, "payload": {"source": "organic"}},
    {"event_type": "page_view", "user_id": 2, "payload": {"path": "/dashboard"}},
    {"event_type": "purchase", "user_id": 2, "payload": {"amount": 29.99, "currency": "USD"}}
  ]'

# List recent events
curl http://localhost:5000/api/events?limit=10

# DAU for a date range
curl "http://localhost:5000/api/analytics/dau?start_date=2026-01-01T00:00:00Z&end_date=2026-02-08T23:59:59Z"

# Events by type (all types)
curl http://localhost:5000/api/analytics/events-by-type

# Events by type (filtered)
curl "http://localhost:5000/api/analytics/events-by-type?event_type=purchase"

# Funnel analysis
curl "http://localhost:5000/api/analytics/funnel?steps=user_signup&steps=page_view&steps=purchase"

# Refresh materialized views
curl -X POST http://localhost:5000/api/admin/refresh-metrics
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string (e.g., `postgresql://user:pass@host:5432/dbname`) |
| `LOG_LEVEL` | No | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) |

On Replit, `DATABASE_URL` is automatically provided when a PostgreSQL database is attached to the project.

---

## Running the Project

### On Replit

Click **Run**. The workflow executes `npm run dev`, which spawns uvicorn via `server/index.ts`. The API is available at port 5000. On startup the app:

1. Connects to PostgreSQL and creates the connection pool
2. Runs schema creation (tables, indexes, materialized views) — all idempotent
3. Seeds demo data if the database is empty
4. Refreshes materialized views

### Locally

```bash
# Requires Python 3.11+ and a running PostgreSQL instance
export DATABASE_URL="postgresql://user:pass@localhost:5432/events_db"

pip install fastapi uvicorn asyncpg pydantic-settings

uvicorn app.main:app --host 0.0.0.0 --port 5000 --reload
```

### Running Tests

```bash
python -m pytest tests/ -v
```

Tests use an isolated `test_analytics` PostgreSQL schema — they never touch production data. The test suite includes 25 tests covering:

- **Database operations** (6): User CRUD, unique constraints, event insertion, JSONB queries, bulk insert, ordering
- **API endpoints** (12): Health check, single/batch ingestion, validation errors, event listing, all analytics endpoints, admin refresh
- **Analytics queries** (7): Materialized view population/refresh, DAU calculation, null-user exclusion, funnel step ordering, event aggregation

---

## Performance & Scalability

### What's Implemented

| Optimization | Mechanism | Tradeoff |
|-------------|-----------|----------|
| **Materialized views** | Pre-computed DAU and event-by-type aggregations | Data is stale until refreshed; `REFRESH CONCURRENTLY` avoids read-locks but requires unique indexes |
| **In-memory cache** | 5-minute TTL per query signature | Reduces DB load for repeated analytics queries; cache is process-local (not shared across workers) |
| **Query timeouts** | `asyncio.wait_for` (5s analytics, 10s funnel) | Prevents slow queries from blocking workers; materialized views have automatic fallback to raw queries |
| **Connection pooling** | asyncpg `create_pool` | Reuses connections; pool size is default (10) — tune `min_size`/`max_size` for production load |
| **GIN index on JSONB** | `idx_events_payload` | Enables fast payload filtering; slight write overhead per insert |
| **Composite B-tree indexes** | `(event_type, timestamp DESC)`, `(user_id, timestamp DESC)` | Covers the most common query patterns without requiring index-only scans |
| **Rate limiting** | In-memory sliding window (100 req/min per IP) | Protects against burst traffic; not distributed — each worker has its own counter |

### What Would Change at Scale

**Single-worker limitations:**
- In-memory cache and rate limiting are process-local. At multiple workers, switch to Redis for shared state.
- Rate limiting uses an unbounded dict — add periodic cleanup or use a fixed-size LRU.

**Materialized view refresh:**
- Currently manual via admin endpoint. In production, schedule with `pg_cron` or an external scheduler (e.g., Celery beat) at 5–15 minute intervals depending on freshness requirements.
- `REFRESH CONCURRENTLY` is non-blocking for reads but still incurs full recomputation. For very large tables (100M+ rows), consider incremental materialized views or a streaming aggregation layer (e.g., ksqlDB, Materialize).

**Event ingestion at high volume:**
- `executemany` is sufficient for moderate throughput. For sustained high-volume ingestion (>10K events/sec), switch to `COPY` protocol via `connection.copy_records_to_table()` which bypasses per-row parsing.
- Consider buffering writes with an async queue (e.g., asyncio.Queue → batch writer task) to decouple HTTP response latency from DB write latency.

**Funnel query:**
- Current CTE-based approach scans the full event range. For large datasets, pre-aggregate user-step pairs into a summary table, or use window functions with partitioned tables.
- Step names are interpolated into SQL via f-string. This is safe because step values come from query parameters validated by FastAPI, but a parameterized approach using array operations would be more defensive.

**Horizontal scaling:**
- The API is stateless (aside from process-local cache/rate-limit). Deploy behind a load balancer with multiple uvicorn workers (`--workers N`) or Gunicorn with uvicorn workers.
- Move cache to Redis. Move rate limiting to Redis or an API gateway (e.g., Kong, nginx rate limiting module).

---

## Logging

Structured logging via Python's `logging` module with a consistent format:

```
2026-02-08 11:19:39 | INFO     | event_analytics.db | Database connection pool created successfully
2026-02-08 11:19:39 | INFO     | event_analytics.repository | Schema creation complete
2026-02-08 11:19:42 | INFO     | event_analytics.main | GET /api/events 200 3.8ms
```

Logger hierarchy:
- `event_analytics.main` — Startup, shutdown, request logging middleware
- `event_analytics.api` — Endpoint-level events (ingestion counts, rate limit warnings)
- `event_analytics.service` — Business logic events (seeding, MV refresh)
- `event_analytics.repository` — Query-level events (bulk insert timing, cache hits, fallback warnings)
- `event_analytics.db` — Connection pool lifecycle

Set `LOG_LEVEL=DEBUG` to see cache hit/miss details and MV fallback reasoning.

---

## Error Handling

| Layer | Mechanism |
|-------|-----------|
| **Global** | `@app.exception_handler(Exception)` catches unhandled errors, logs stack trace, returns generic 500 |
| **Endpoint** | Each analytics endpoint catches `asyncio.TimeoutError` (→ 504) and general exceptions (→ 500) |
| **Ingestion** | Validation errors via Pydantic (→ 422), business errors (→ 400), rate limit (→ 429) |
| **Repository** | Materialized view queries have automatic fallback to raw SQL on timeout or failure |
| **Database** | `RuntimeError` if queries are attempted before pool initialization |
| **Startup** | Critical failures during startup log the error and call `sys.exit(1)` |

---

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Runtime | Python 3.11 | Async support, typing, performance improvements |
| Framework | FastAPI | Async-native, auto OpenAPI docs, Pydantic validation |
| DB Driver | asyncpg | Fastest Python PostgreSQL driver; no ORM overhead |
| Validation | Pydantic v2 | Runtime type checking for request/response models |
| Config | pydantic-settings | Type-safe env var loading with `.env` support |
| Testing | pytest + pytest-asyncio + httpx | Async test support with ASGI transport for endpoint tests |
| Database | PostgreSQL | JSONB support, materialized views, GIN indexes |
