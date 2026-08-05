from unittest.mock import AsyncMock, patch


async def test_health_returns_ok(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "db": "connected",
        "object_storage": "connected",
    }


async def test_health_returns_503_when_db_check_times_out(async_client):
    with patch(
        "openctopus_server.api.health._check_db",
        new_callable=AsyncMock,
        side_effect=TimeoutError,
    ):
        response = await async_client.get("/health")
    assert response.status_code == 503
    assert response.json()["db"] == "disconnected"
    assert response.json()["object_storage"] == "connected"


async def test_health_returns_503_when_object_storage_check_times_out(async_client):
    with patch(
        "openctopus_server.api.health._check_object_storage",
        new_callable=AsyncMock,
        side_effect=TimeoutError,
    ):
        response = await async_client.get("/health")
    assert response.status_code == 503
    assert response.json() == {
        "status": "error",
        "db": "connected",
        "object_storage": "disconnected",
    }
