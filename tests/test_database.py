import pytest
import json
from datetime import datetime, timedelta

TEST_SCHEMA = "test_analytics"


class TestUserOperations:
    @pytest.mark.asyncio
    async def test_create_user(self, test_pool):
        async with test_pool.acquire() as conn:
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")
            await conn.execute("DELETE FROM events;")
            await conn.execute("DELETE FROM users;")
            row = await conn.fetchrow(
                "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id, username, email;",
                "db_test_user", "dbtest@example.com"
            )
            assert row["username"] == "db_test_user"
            assert row["email"] == "dbtest@example.com"
            assert row["id"] is not None

    @pytest.mark.asyncio
    async def test_unique_username_constraint(self, test_pool):
        async with test_pool.acquire() as conn:
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")
            await conn.execute("DELETE FROM events;")
            await conn.execute("DELETE FROM users;")
            await conn.execute(
                "INSERT INTO users (username, email) VALUES ($1, $2);",
                "unique_user", "u1@example.com"
            )
            with pytest.raises(Exception):
                await conn.execute(
                    "INSERT INTO users (username, email) VALUES ($1, $2);",
                    "unique_user", "u2@example.com"
                )


class TestEventOperations:
    @pytest.mark.asyncio
    async def test_insert_event(self, test_pool):
        async with test_pool.acquire() as conn:
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")
            await conn.execute("DELETE FROM events;")
            await conn.execute("DELETE FROM users;")
            user = await conn.fetchrow(
                "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id;",
                "evt_user", "evt@example.com"
            )
            now = datetime.utcnow()
            row = await conn.fetchrow(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, $2, $3::jsonb, $4) RETURNING id, event_type, user_id, payload;",
                "page_view", user["id"], '{"path": "/test"}', now
            )
            assert row["event_type"] == "page_view"
            assert row["user_id"] == user["id"]
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            assert payload["path"] == "/test"

    @pytest.mark.asyncio
    async def test_jsonb_payload_query(self, test_pool):
        async with test_pool.acquire() as conn:
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")
            await conn.execute("DELETE FROM events;")
            await conn.execute("DELETE FROM users;")
            user = await conn.fetchrow(
                "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id;",
                "jsonb_user", "jsonb@example.com"
            )
            now = datetime.utcnow()
            await conn.execute(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, $2, $3::jsonb, $4);",
                "purchase", user["id"], '{"amount": 99.99, "currency": "USD"}', now
            )
            row = await conn.fetchrow(
                "SELECT * FROM events WHERE payload->>'currency' = $1;", "USD"
            )
            assert row is not None
            assert row["event_type"] == "purchase"

    @pytest.mark.asyncio
    async def test_bulk_insert_events(self, test_pool):
        async with test_pool.acquire() as conn:
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")
            await conn.execute("DELETE FROM events;")
            await conn.execute("DELETE FROM users;")
            user = await conn.fetchrow(
                "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id;",
                "bulk_user", "bulk@example.com"
            )
            now = datetime.utcnow()
            records = [
                ("click", user["id"], '{}', now - timedelta(minutes=i))
                for i in range(10)
            ]
            await conn.executemany(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, $2, $3::jsonb, $4);",
                records
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM events;")
            assert count == 10

    @pytest.mark.asyncio
    async def test_event_ordering(self, test_pool):
        async with test_pool.acquire() as conn:
            await conn.execute(f"SET search_path TO {TEST_SCHEMA};")
            await conn.execute("DELETE FROM events;")
            await conn.execute("DELETE FROM users;")
            user = await conn.fetchrow(
                "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id;",
                "order_user", "order@example.com"
            )
            now = datetime.utcnow()
            await conn.execute(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, $2, $3::jsonb, $4);",
                "first", user["id"], '{}', now - timedelta(hours=2)
            )
            await conn.execute(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, $2, $3::jsonb, $4);",
                "second", user["id"], '{}', now
            )
            rows = await conn.fetch("SELECT event_type FROM events ORDER BY timestamp DESC;")
            assert rows[0]["event_type"] == "second"
            assert rows[1]["event_type"] == "first"
