import httpx

from openctopus_server.services.system_config import validate_llm_identity


def _mock_models_response(model: str, status: int = 200) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            json={"object": "list", "data": [{"id": model, "object": "model"}]},
        )
    return httpx.MockTransport(handler)


def _mock_models_missing(model: str) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "other-model", "object": "model"}]},
        )
    return httpx.MockTransport(handler)


async def test_get_config_defaults(admin_client):
    response = await admin_client.get("/api/admin/config")
    assert response.status_code == 200
    body = response.json()
    assert body["quota_bytes"] == 524288000
    assert body["shared_workspace_quota_bytes"] == 524288000
    assert body["llm_endpoint"] is None
    assert body["llm_api_key"] is None
    assert body["llm_model"] is None


async def test_patch_config_llm_success(admin_client, monkeypatch):
    original = validate_llm_identity

    async def mock_validate(endpoint, api_key, model, *, client=None):
        mock_transport = _mock_models_response(model)
        mock_client = httpx.AsyncClient(transport=mock_transport)
        await original(endpoint, api_key, model, client=mock_client)
        await mock_client.aclose()

    monkeypatch.setattr(
        "openctopus_server.services.system_config.validate_llm_identity", mock_validate
    )

    response = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_endpoint": "http://fake-llm/v1",
            "llm_api_key": "fake-key",
            "llm_model": "fake-model",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["llm_endpoint"] == "http://fake-llm/v1"
    assert body["llm_api_key"] == "<redacted>"
    assert body["llm_model"] == "fake-model"


async def test_patch_config_unknown_key_returns_422(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"unknown_key": "value"},
    )
    assert response.status_code == 422


async def test_patch_config_invalid_value_returns_422(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"quota_bytes": 0},
    )
    assert response.status_code == 422


async def test_patch_config_llm_non_200_returns_400(admin_client, monkeypatch):
    original = validate_llm_identity

    async def mock_validate(endpoint, api_key, model, *, client=None):
        mock_transport = _mock_models_response(model, status=500)
        mock_client = httpx.AsyncClient(transport=mock_transport)
        await original(endpoint, api_key, model, client=mock_client)
        await mock_client.aclose()

    monkeypatch.setattr(
        "openctopus_server.services.system_config.validate_llm_identity", mock_validate
    )

    response = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_endpoint": "http://fake-llm/v1",
            "llm_api_key": "fake-key",
            "llm_model": "fake-model",
        },
    )
    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


async def test_patch_config_llm_model_absent_returns_400(admin_client, monkeypatch):
    original = validate_llm_identity

    async def mock_validate(endpoint, api_key, model, *, client=None):
        mock_transport = _mock_models_missing(model)
        mock_client = httpx.AsyncClient(transport=mock_transport)
        await original(endpoint, api_key, model, client=mock_client)
        await mock_client.aclose()

    monkeypatch.setattr(
        "openctopus_server.services.system_config.validate_llm_identity", mock_validate
    )

    response = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_endpoint": "http://fake-llm/v1",
            "llm_api_key": "fake-key",
            "llm_model": "fake-model",
        },
    )
    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


async def test_patch_config_redacted_marker_rejected(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"llm_api_key": "<redacted>"},
    )
    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


async def test_non_admin_get_config_returns_403(user_client):
    response = await user_client.get("/api/admin/config")
    assert response.status_code == 403
    assert response.json()["code"] == "auth_forbidden"
