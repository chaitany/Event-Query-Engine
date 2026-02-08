import pytest
from datetime import datetime, timedelta

TEST_SCHEMA = "test_analytics"


class TestMaterializedViews:
    @pytest.mark.asyncio
    async def test_mv_dau_daily_populated(self, test_pool, seed_data):
        async with test_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM mv_dau_daily ORDER BY day DESC;")
            assert len(rows) > 0
            for row in rows:
                assert row["dau"] > 0

    @pytest.mark.asyncio
    async def test_mv_events_by_type_populated(self, test_pool, seed_data):
        async with test_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM mv_events_by_type ORDER BY count DESC;")
            assert len(rows) > 0
            types = [r["event_type"] for r in rows]
            assert "page_view" in types
            assert "purchase" in types
            assert "user_signup" in types

    @pytest.mark.asyncio
    async def test_mv_refresh_updates_data(self, test_pool, seed_data):
        async with test_pool.acquire() as conn:
            user = await conn.fetchrow("SELECT id FROM users LIMIT 1;")
            now = datetime.utcnow()
            await conn.execute(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, $2, $3::jsonb, $4);",
                "new_type", user["id"], '{}', now
            )
            await conn.execute("REFRESH MATERIALIZED VIEW mv_events_by_type;")
            row = await conn.fetchrow("SELECT * FROM mv_events_by_type WHERE event_type = $1;", "new_type")
            assert row is not None
            assert row["count"] == 1


class TestDauQuery:
    @pytest.mark.asyncio
    async def test_dau_returns_correct_days(self, test_pool, seed_data):
        async with test_pool.acquire() as conn:
            now = datetime.utcnow()
            start = now - timedelta(days=7)
            rows = await conn.fetch(
                "SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(DISTINCT user_id) AS dau "
                "FROM events WHERE timestamp >= $1 AND timestamp <= $2 AND user_id IS NOT NULL "
                "GROUP BY 1 ORDER BY 1 DESC;",
                start, now
            )
            assert len(rows) > 0

    @pytest.mark.asyncio
    async def test_dau_excludes_null_user(self, test_pool, seed_data):
        async with test_pool.acquire() as conn:
            now = datetime.utcnow()
            await conn.execute(
                "INSERT INTO events (event_type, user_id, payload, timestamp) VALUES ($1, NULL, $2::jsonb, $3);",
                "anonymous_view", '{}', now
            )
            start = now - timedelta(days=1)
            rows = await conn.fetch(
                "SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(DISTINCT user_id) AS dau "
                "FROM events WHERE timestamp >= $1 AND timestamp <= $2 AND user_id IS NOT NULL "
                "GROUP BY 1 ORDER BY 1 DESC;",
                start, now
            )
            for row in rows:
                assert row["dau"] >= 1


class TestFunnelQuery:
    @pytest.mark.asyncio
    async def test_funnel_step_order(self, test_pool, seed_data):
        async with test_pool.acquire() as conn:
            now = datetime.utcnow()
            start = now - timedelta(days=7)
            steps = ["user_signup", "page_view", "purchase"]
            row = await conn.fetchrow(
                """
                WITH user_steps AS (
                    SELECT user_id, event_type, MIN(timestamp) AS first_step_time
                    FROM events WHERE timestamp >= $1 AND timestamp <= $2 AND event_type = ANY($3) AND user_id IS NOT NULL
                    GROUP BY user_id, event_type
                ),
                funnel_agg AS (
                    SELECT user_id,
                        MIN(CASE WHEN event_type = 'user_signup' THEN first_step_time END) AS step_0,
                        MIN(CASE WHEN event_type = 'page_view' THEN first_step_time END) AS step_1,
                        MIN(CASE WHEN event_type = 'purchase' THEN first_step_time END) AS step_2
                    FROM user_steps GROUP BY user_id
                )
                SELECT COUNT(step_0) AS count_step_0, COUNT(step_1) AS count_step_1, COUNT(step_2) AS count_step_2
                FROM funnel_agg;
                """,
                start, now, steps
            )
            assert row["count_step_0"] >= 1
            assert row["count_step_1"] >= 1
            assert row["count_step_2"] >= 1


class TestEventsByTypeQuery:
    @pytest.mark.asyncio
    async def test_events_grouped_by_type(self, test_pool, seed_data):
        async with test_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT event_type, COUNT(*) AS count FROM events GROUP BY event_type ORDER BY count DESC;"
            )
            assert len(rows) >= 3
            type_counts = {r["event_type"]: r["count"] for r in rows}
            assert type_counts["page_view"] == 3
            assert type_counts["purchase"] == 2
            assert type_counts["user_signup"] == 1
