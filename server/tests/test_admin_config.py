import asyncio

import httpx

from openctopus_server.services import system_config
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
    assert body["llm_max_output_tokens"] == 16384


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


async def test_patch_max_output_tokens(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"llm_max_output_tokens": 32768},
    )
    assert response.status_code == 200
    assert response.json()["llm_max_output_tokens"] == 32768


async def test_patch_output_above_context_rejected(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_max_context_tokens": 8192,
            "llm_max_output_tokens": 8193,
        },
    )
    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


async def test_patch_compaction_threshold_requires_larger_context(admin_client):
    without_context = await admin_client.patch(
        "/api/admin/config",
        json={"llm_compaction_threshold_tokens": 16000},
    )
    assert without_context.status_code == 400
    assert without_context.json()["code"] == "config_validation_failed"

    equal_to_context = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_max_output_tokens": 4000,
            "llm_max_context_tokens": 16000,
            "llm_compaction_threshold_tokens": 16000,
        },
    )
    assert equal_to_context.status_code == 400
    assert equal_to_context.json()["code"] == "config_validation_failed"

    valid = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_max_output_tokens": 4000,
            "llm_max_context_tokens": 16001,
            "llm_compaction_threshold_tokens": 16000,
        },
    )
    assert valid.status_code == 200


async def test_concurrent_token_limit_updates_keep_pair_valid(admin_client, monkeypatch):
    initial = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_max_output_tokens": 5000,
            "llm_max_context_tokens": 10000,
        },
    )
    assert initial.status_code == 200

    original_get_all_rows = system_config._get_all_rows
    initial_read_tasks: set[object] = set()
    initial_read_count = 0
    both_initial_reads = asyncio.Event()

    async def synchronize_initial_reads(db):
        nonlocal initial_read_count
        rows = await original_get_all_rows(db)
        task = asyncio.current_task()
        if task not in initial_read_tasks:
            initial_read_tasks.add(task)
            if initial_read_count < 2:
                initial_read_count += 1
                if initial_read_count == 2:
                    both_initial_reads.set()
                try:
                    await asyncio.wait_for(both_initial_reads.wait(), timeout=0.2)
                except TimeoutError:
                    pass
        return rows

    monkeypatch.setattr(system_config, "_get_all_rows", synchronize_initial_reads)
    output_response, context_response = await asyncio.gather(
        admin_client.patch(
            "/api/admin/config",
            json={"llm_max_output_tokens": 9000},
        ),
        admin_client.patch(
            "/api/admin/config",
            json={"llm_max_context_tokens": 6000},
        ),
    )

    assert sorted([output_response.status_code, context_response.status_code]) == [200, 400]
    stored = (await admin_client.get("/api/admin/config")).json()
    assert stored["llm_max_output_tokens"] <= stored["llm_max_context_tokens"]


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
