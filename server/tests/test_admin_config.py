import asyncio

import httpx
import pytest

from openctopus_server.dto.config import AdminConfig
from openctopus_server.network_policy import DEFAULT_SSRF_DENYLIST
from openctopus_server.services import system_config
from openctopus_server.services.system_config import validate_llm_identity


def _mock_models_response(
    model: str,
    status: int = 200,
    captured: dict[str, str] | None = None,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured["path"] = request.url.path
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
    assert body["llm_max_context_tokens"] is None
    assert body["llm_compaction_threshold_tokens"] is None
    assert body["llm_max_concurrent_requests"] is None
    assert body["llm_max_output_tokens"] == 16384
    assert body["default_soul"] == "You are OpenOctopus, the user's personal AI partner."
    assert body["web_fetch_denylist"] == list(DEFAULT_SSRF_DENYLIST)
    assert set(body) == set(AdminConfig.model_fields)


def test_admin_config_openapi_requires_nullable_unconfigured_fields():
    schema = AdminConfig.model_json_schema(mode="serialization")

    assert set(schema["required"]) == set(AdminConfig.model_fields)
    for field in {
        "llm_endpoint",
        "llm_api_key",
        "llm_model",
        "llm_max_context_tokens",
        "llm_compaction_threshold_tokens",
        "llm_max_concurrent_requests",
    }:
        assert {item.get("type") for item in schema["properties"][field]["anyOf"]} == {
            "string" if field in {"llm_endpoint", "llm_api_key", "llm_model"} else "integer",
            "null",
        }


async def test_patch_web_fetch_denylist_canonicalizes_and_allows_empty(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={
            "web_fetch_denylist": [
                "10.1.2.3",
                "192.168.9.7/24",
                "EXAMPLE.COM.",
                "Example.NET:8443",
                "2001:db8::1",
                "[2001:db8::2]:8443",
                "BÜCHER.Example.",
            ]
        },
    )

    assert response.status_code == 200
    assert response.json()["web_fetch_denylist"] == [
        "10.1.2.3/32",
        "192.168.9.0/24",
        "example.com",
        "example.net:8443",
        "2001:db8::1/128",
        "[2001:db8::2]:8443",
        "xn--bcher-kva.example",
    ]

    cleared = await admin_client.patch(
        "/api/admin/config",
        json={"web_fetch_denylist": []},
    )
    assert cleared.status_code == 200
    assert cleared.json()["web_fetch_denylist"] == []


async def test_invalid_web_fetch_denylist_does_not_save_any_config_field(admin_client):
    initial = await admin_client.patch(
        "/api/admin/config",
        json={"quota_bytes": 1234, "web_fetch_denylist": ["example.com"]},
    )
    assert initial.status_code == 200

    invalid = await admin_client.patch(
        "/api/admin/config",
        json={
            "quota_bytes": 5678,
            "web_fetch_denylist": ["EXAMPLE.COM.", "example.com"],
        },
    )
    assert invalid.status_code == 400
    assert invalid.json()["code"] == "config_validation_failed"

    stored = (await admin_client.get("/api/admin/config")).json()
    assert stored["quota_bytes"] == 1234
    assert stored["web_fetch_denylist"] == ["example.com"]


@pytest.mark.parametrize(
    "entry",
    [
        "https://example.com/path",
        "*.example.com",
        "example.com/path",
        "user@example.com",
        "example.com:0",
        "example.com:65536",
    ],
)
async def test_invalid_web_fetch_denylist_entry_is_rejected(admin_client, entry):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"web_fetch_denylist": [entry]},
    )

    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


@pytest.mark.parametrize(
    "field",
    [
        "quota_bytes",
        "shared_workspace_quota_bytes",
        "llm_endpoint",
        "llm_api_key",
        "llm_model",
        "llm_max_context_tokens",
        "llm_compaction_threshold_tokens",
        "llm_max_concurrent_requests",
        "llm_max_output_tokens",
        "default_soul",
        "web_fetch_denylist",
    ],
)
async def test_explicit_null_config_field_is_rejected_without_changes(admin_client, field):
    before = (await admin_client.get("/api/admin/config")).json()
    rejected = await admin_client.patch(
        "/api/admin/config",
        json={field: None},
    )

    assert rejected.status_code == 400
    assert rejected.json() == {
        "code": "config_validation_failed",
        "message": "Config values cannot be null",
    }
    assert (await admin_client.get("/api/admin/config")).json() == before


async def test_patch_omitted_fields_keep_existing_values(admin_client):
    initial = await admin_client.patch(
        "/api/admin/config",
        json={
            "quota_bytes": 1234,
            "shared_workspace_quota_bytes": 2345,
            "web_fetch_denylist": ["example.com"],
        },
    )
    assert initial.status_code == 200

    updated = await admin_client.patch(
        "/api/admin/config",
        json={"quota_bytes": 3456},
    )

    assert updated.status_code == 200
    assert updated.json()["quota_bytes"] == 3456
    assert updated.json()["shared_workspace_quota_bytes"] == 2345
    assert updated.json()["web_fetch_denylist"] == ["example.com"]


async def test_patch_default_soul_is_hot_updated(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"default_soul": "You are the company assistant."},
    )

    assert response.status_code == 200
    assert response.json()["default_soul"] == "You are the company assistant."
    assert (await admin_client.get("/api/admin/config")).json()["default_soul"] == (
        "You are the company assistant."
    )


@pytest.mark.parametrize("value", ["", "   ", "x" * 32_001])
async def test_invalid_default_soul_is_rejected(admin_client, value):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"default_soul": value},
    )

    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


async def test_patch_config_llm_success(admin_client, monkeypatch):
    original = validate_llm_identity
    captured: dict[str, str] = {}

    async def mock_validate(endpoint, api_key, model, *, client=None):
        mock_transport = _mock_models_response(model, captured=captured)
        mock_client = httpx.AsyncClient(transport=mock_transport)
        await original(endpoint, api_key, model, client=mock_client)
        await mock_client.aclose()

    monkeypatch.setattr(
        "openctopus_server.services.system_config.validate_llm_identity", mock_validate
    )

    response = await admin_client.patch(
        "/api/admin/config",
        json={
            "llm_endpoint": "http://fake-llm",
            "llm_api_key": "fake-key",
            "llm_model": "fake-model",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["llm_endpoint"] == "http://fake-llm"
    assert body["llm_api_key"] == "<redacted>"
    assert body["llm_model"] == "fake-model"
    assert captured["path"] == "/v1/models"


async def test_patch_config_unknown_key_returns_400(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"unknown_key": "value"},
    )
    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


async def test_patch_config_invalid_value_returns_400(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"quota_bytes": 0},
    )
    assert response.status_code == 400
    assert response.json()["code"] == "config_validation_failed"


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
            "llm_endpoint": "http://fake-llm",
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
            "llm_endpoint": "http://fake-llm",
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
