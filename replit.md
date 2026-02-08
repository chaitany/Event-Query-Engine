# replit.md

## Overview

This is a hybrid full-stack application combining a **React frontend** with a **Python FastAPI backend**. The project is an **Event Analytics & Query Engine** that logs and queries events stored in a PostgreSQL database. The Node.js entry point (`server/index.ts`) acts as a process launcher that spawns a Python Uvicorn server. The frontend is a React SPA built with Vite, using shadcn/ui components and TailwindCSS.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Overall Structure

The project follows a **polyglot architecture** with three main directories:

- **`client/`** — React frontend (Vite + TypeScript)
- **`server/`** — Node.js entry point that spawns the Python backend
- **`app/`** — Python FastAPI backend (the actual API server)
- **`shared/`** — Shared TypeScript schema definitions (Drizzle ORM, mostly legacy/unused by the Python side)

### Frontend Architecture

- **Framework**: React 18 with TypeScript
- **Bundler**: Vite (config in `vite.config.ts`)
- **Routing**: Wouter (lightweight client-side router)
- **State/Data Fetching**: TanStack React Query
- **UI Components**: shadcn/ui (new-york style) with Radix UI primitives
- **Styling**: TailwindCSS with CSS variables for theming, PostCSS
- **Path Aliases**: `@/` maps to `client/src/`, `@shared/` maps to `shared/`
- The frontend currently has minimal pages (just a 404 page) — routes need to be added in `client/src/App.tsx`

### Backend Architecture

- **Framework**: Python FastAPI with async support
- **Server**: Uvicorn (spawned by Node.js via `server/index.ts`)
- **Database Driver**: asyncpg (async PostgreSQL driver)
- **Configuration**: pydantic-settings loading from environment variables
- **Architecture Pattern**: Service-Repository pattern
  - `app/api/endpoints.py` — API route handlers
  - `app/services/event_service.py` — Business logic layer
  - `app/repositories/event_repository.py` — Database access layer
  - `app/models/event.py` — Pydantic models for request/response validation
  - `app/db.py` — Database connection pool management
- **API Prefix**: All API routes are under `/api`
- **Health Check**: `GET /health`
- **Startup**: On startup, the app connects to the database, creates tables if they don't exist, and seeds demo data

### Database

- **PostgreSQL** is required (connection via `DATABASE_URL` environment variable)
- **Two table systems exist** (potential conflict):
  1. **Python side** (active): Creates `users` and `events` tables directly via SQL in `event_repository.py`. Events table has JSONB payload with GIN indexes for analytics queries.
  2. **Drizzle side** (legacy/unused): `shared/schema.ts` defines a `users` table with Drizzle ORM. The Drizzle config (`drizzle.config.ts`) is set up but the Python backend doesn't use it.
- The Python-side schema is the authoritative one. The Drizzle schema in `shared/` is vestigial from the original Node.js template.
- Schema push command: `npm run db:push` (for Drizzle — not used by the Python backend)

### Server Entry Point

- `server/index.ts` is a Node.js script that spawns `uvicorn app.main:app` as a child process on port 5000
- It forwards stdio and handles SIGTERM/SIGINT for graceful shutdown
- The `npm run dev` command runs this via `tsx`

### Build System

- `script/build.ts` handles production builds: builds the Vite frontend to `dist/public/` and bundles the server with esbuild to `dist/index.cjs`
- The Express static file server (`server/static.ts`) and Vite dev middleware (`server/vite.ts`) exist but are **not actively used** since the backend is Python/FastAPI

### Key Files

| File | Purpose |
|------|---------|
| `server/index.ts` | Entry point — spawns Python Uvicorn |
| `app/main.py` | FastAPI app definition and lifespan |
| `app/api/endpoints.py` | API route handlers |
| `app/services/event_service.py` | Business logic |
| `app/repositories/event_repository.py` | Database queries |
| `app/db.py` | asyncpg connection pool |
| `app/models/event.py` | Pydantic models |
| `app/config.py` | Environment config |
| `shared/schema.ts` | Drizzle schema (legacy) |
| `client/src/App.tsx` | React app root with router |
| `vite.config.ts` | Vite configuration |

## External Dependencies

### Database
- **PostgreSQL** — Required. Connected via `DATABASE_URL` environment variable. Used by both the Python asyncpg driver and the Drizzle config (though Drizzle is not actively used).

### Python Packages
- **FastAPI** — Web framework
- **Uvicorn** — ASGI server
- **asyncpg** — Async PostgreSQL driver
- **pydantic-settings** — Configuration management from environment

### Node.js / Frontend Packages
- **Vite** — Dev server and bundler
- **React** + **ReactDOM** — UI framework
- **TanStack React Query** — Server state management
- **Wouter** — Client-side routing
- **shadcn/ui** — Component library (Radix UI + Tailwind)
- **Drizzle ORM** + **drizzle-kit** — Database ORM (configured but not actively used by the Python backend)
- **esbuild** — Server bundling for production

### Replit-Specific
- `@replit/vite-plugin-runtime-error-modal` — Runtime error overlay
- `@replit/vite-plugin-cartographer` — Dev tooling (dev only)
- `@replit/vite-plugin-dev-banner` — Dev banner (dev only)