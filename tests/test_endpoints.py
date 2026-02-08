import pytest
from datetime import datetime, timedelta

TEST_SCHEMA = "test_analytics"


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_check(self, app_client):
        resp = await app_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"


class TestIngestionEndpoints:
    @pytest.mark.asyncio
    async def test_ingest_single_event(self, app_client, seed_data):
        resp = await app_client.post("/api/events", json={
            "event_type": "test_click",
            "user_id": seed_data["user_id"],
            "payload": {"button": "cta"},
            "timestamp": datetime.utcnow().isoformat()
        })
        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["ingested"] == 1

    @pytest.mark.asyncio
    async def test_ingest_batch_events(self, app_client, seed_data):
        events = [
            {
                "event_type": f"batch_event_{i}",
                "user_id": seed_data["user_id"],
                "payload": {"index": i},
                "timestamp": datetime.utcnow().isoformat()
            }
            for i in range(5)
        ]
        resp = await app_client.post("/api/events", json=events)
        assert resp.status_code == 202
        data = resp.json()
        assert data["ingested"] == 5

    @pytest.mark.asyncio
    async def test_ingest_invalid_event(self, app_client):
        resp = await app_client.post("/api/events", json={"bad": "data"})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_list_events(self, app_client, seed_data):
        resp = await app_client.get("/api/events?limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    @pytest.mark.asyncio
    async def test_list_events_limit(self, app_client, seed_data):
        resp = await app_client.get("/api/events?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) <= 2


class TestAnalyticsEndpoints:
    @pytest.mark.asyncio
    async def test_dau_endpoint(self, app_client, seed_data):
        now = datetime.utcnow()
        start = (now - timedelta(days=7)).isoformat()
        end = now.isoformat()
        resp = await app_client.get(f"/api/analytics/dau?start_date={start}&end_date={end}")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        for entry in data:
            assert "day" in entry
            assert "dau" in entry
            assert entry["dau"] > 0

    @pytest.mark.asyncio
    async def test_events_by_type_endpoint(self, app_client, seed_data):
        now = datetime.utcnow()
        start = (now - timedelta(days=7)).isoformat()
        end = now.isoformat()
        resp = await app_client.get(f"/api/analytics/events-by-type?start_date={start}&end_date={end}")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        types = [d["event_type"] for d in data]
        assert "page_view" in types

    @pytest.mark.asyncio
    async def test_events_by_type_filter(self, app_client, seed_data):
        now = datetime.utcnow()
        start = (now - timedelta(days=7)).isoformat()
        end = now.isoformat()
        resp = await app_client.get(f"/api/analytics/events-by-type?start_date={start}&end_date={end}&event_type=purchase")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_funnel_endpoint(self, app_client, seed_data):
        now = datetime.utcnow()
        start = (now - timedelta(days=7)).isoformat()
        end = now.isoformat()
        resp = await app_client.get(
            f"/api/analytics/funnel?start_date={start}&end_date={end}&steps=user_signup&steps=page_view&steps=purchase"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["step_name"] == "user_signup"
        assert data[1]["step_name"] == "page_view"
        assert data[2]["step_name"] == "purchase"
        for step in data:
            assert "count" in step
            assert "conversion_rate" in step

    @pytest.mark.asyncio
    async def test_funnel_conversion_rates(self, app_client, seed_data):
        now = datetime.utcnow()
        start = (now - timedelta(days=7)).isoformat()
        end = now.isoformat()
        resp = await app_client.get(
            f"/api/analytics/funnel?start_date={start}&end_date={end}&steps=user_signup&steps=page_view&steps=purchase"
        )
        data = resp.json()
        assert data[0]["count"] >= 1
        assert data[0]["conversion_rate"] == 100.0


class TestAdminEndpoints:
    @pytest.mark.asyncio
    async def test_refresh_metrics(self, app_client, seed_data):
        resp = await app_client.post("/api/admin/refresh-metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
