from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Device
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.mcp_catalog import EMPTY_CATALOG_DIGEST
from openctopus_server.devices.protocol import (
    ConfigValidateResultFrame,
    DeviceConfigFrame,
    McpValidationFailure,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
)
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceRegistry,
    DeviceUnavailableError,
    DeviceValidationError,
)
from openctopus_server.services import devices


async def _register(client: Any, *, email: str = "owner@test.com") -> dict[str, Any]:
    response = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "testpassword", "name": "Owner"},
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _request_as(
    client: Any,
    identity: dict[str, Any],
    method: str,
    url: str,
    **kwargs: Any,
) -> Any:
    client.cookies.clear()
    headers = dict(kwargs.pop("headers", {}))
    headers["Authorization"] = f"Bearer {identity['jwt']}"
    return await client.request(method, url, headers=headers, **kwargs)


class _ConfigRegistry:
    def __init__(self) -> None:
        self.online = False
        self.validations: list[dict[str, Any]] = []
        self.pushes: list[dict[str, Any]] = []

    async def is_online(self, _device_id: UUID, *, user_id: UUID) -> bool:
        del user_id
        return self.online

    @asynccontextmanager
    async def config_update_lock(self, **_kwargs: object):
        yield

    async def validate_config(self, **kwargs: Any) -> Any:
        if not self.online:
            raise DeviceUnavailableError("offline")
        self.validations.append(kwargs)
        names = kwargs["validate_servers"]
        source = SourceMcpCatalog(
            version=1,
            servers=[SourceMcpServerCatalog(name=name) for name in names],
        )
        return type(
            "Validation",
            (),
            {
                "id": UUID("0198e2c8-592a-7000-8000-000000000010"),
                "handle": ConnectionHandle(
                    device_id=kwargs["device_id"],
                    generation=1,
                ),
                "source_catalog": source,
            },
        )()

    async def begin_config_update(self, **_kwargs: object) -> bool:
        return self.online

    async def abort_config_update(self, **_kwargs: object) -> None:
        return None

    async def push_config(self, **kwargs: Any) -> bool:
        self.pushes.append(kwargs)
        return True


def test_stored_catalog_accepts_the_jsonb_uuid_representation() -> None:
    catalog = devices.parse_stored_mcp_catalog(
        {
            "version": 1,
            "digest": "09c6199190ca1fe4d56e1b344d85f9b2967742df1a24ccf3757a3e755292cc90",
            "servers": [
                {
                    "name": "demo",
                    "entries": [
                        {
                            "entry_id": "0198e2c8-592a-7000-8000-000000000010",
                            "server": "demo",
                            "surface": "tool",
                            "raw_name": "search",
                            "invocation_identity": "search",
                            "final_name": "mcp_demo_search",
                            "provider_description": "Search with demo.",
                            "input_schema": {
                                "type": "object",
                                "properties": {},
                                "additionalProperties": False,
                            },
                            "output_schema": None,
                            "enabled": True,
                        }
                    ],
                }
            ],
        }
    )

    assert catalog.servers[0].entries[0].entry_id == UUID(
        "0198e2c8-592a-7000-8000-000000000010"
    )


async def _create_device(async_client: Any, owner: dict[str, Any]) -> dict[str, Any]:
    response = await _request_as(
        async_client,
        owner,
        "POST",
        "/api/devices",
        json={"name": "Laptop"},
    )
    assert response.status_code == 201, response.text
    return response.json()["device"]


async def test_device_config_get_redacts_secrets_and_returns_last_good_discovery(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _ConfigRegistry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    device = await _create_device(async_client, owner)

    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        row.mcp_servers = [
            {
                "name": "github",
                "transport": "stdio",
                "command": "github-mcp",
                "args": [],
                "cwd": None,
                "env": {"TOKEN": "super-secret"},
                "enabled_capabilities": None,
            }
        ]
        row.config_revision = 4
        await db.commit()

    response = await _request_as(
        async_client,
        owner,
        "GET",
        "/api/devices/laptop/config",
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["device"] == {
        "name": "laptop",
        "online": False,
        "config_revision": 4,
    }
    assert payload["mcp_servers"][0]["env"] == {"TOKEN": "<redacted>"}
    assert "super-secret" not in response.text
    assert payload["mcp_catalog_digest"] == EMPTY_CATALOG_DIGEST
    assert payload["mcp_discovered"] == {
        "github": {"tools": [], "resources": [], "resource_templates": [], "prompts": []}
    }


async def test_patch_requires_revision_and_offline_pure_removal_is_atomic(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _ConfigRegistry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    device = await _create_device(async_client, owner)
    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        row.mcp_servers = [
            {
                "name": "github",
                "transport": "stdio",
                "command": "github-mcp",
                "args": [],
                "cwd": None,
                "env": {},
                "enabled_capabilities": [],
            }
        ]
        await db.commit()

    missing = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"mcp_servers": []},
    )
    assert missing.status_code == 400

    removed = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": []},
    )
    assert removed.status_code == 200, removed.text
    assert removed.json()["device"]["config_revision"] == 2
    assert removed.json()["mcp_servers"] == []
    assert removed.json()["mcp_catalog_digest"] == EMPTY_CATALOG_DIGEST
    assert registry.validations == []

    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        assert row.mcp_servers == []
        assert row.mcp_catalog["digest"] == EMPTY_CATALOG_DIGEST
        assert row.config_revision == 2


async def test_secret_marker_exact_noop_does_not_validate_or_increment_revision(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _ConfigRegistry()
    registry.online = True
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    device = await _create_device(async_client, owner)
    stored = {
        "name": "github",
        "transport": "stdio",
        "command": "github-mcp",
        "args": ["--stdio"],
        "cwd": None,
        "env": {"TOKEN": "super-secret"},
        "enabled_capabilities": [],
    }
    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        row.mcp_servers = [stored]
        await db.commit()

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={
            "base_config_revision": 1,
            "mcp_servers": [{**stored, "env": {"TOKEN": "<redacted>"}}],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["device"]["config_revision"] == 1
    assert registry.validations == []
    assert registry.pushes == []
    assert "super-secret" not in response.text


async def test_secret_marker_cannot_move_to_another_sink(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _ConfigRegistry()
    registry.online = True
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    device = await _create_device(async_client, owner)
    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        row.mcp_servers = [
            {
                "name": "github",
                "transport": "stdio",
                "command": "old-command",
                "args": [],
                "cwd": None,
                "env": {"TOKEN": "super-secret"},
                "enabled_capabilities": [],
            }
        ]
        await db.commit()

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={
            "base_config_revision": 1,
            "mcp_servers": [
                {
                    "name": "github",
                    "transport": "stdio",
                    "command": "new-command",
                    "args": [],
                    "cwd": None,
                    "env": {"TOKEN": "<redacted>"},
                    "enabled_capabilities": [],
                }
            ],
        },
    )

    assert response.status_code in {400, 422}
    assert "super-secret" not in response.text
    assert registry.validations == []

    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        assert row.mcp_servers[0]["command"] == "old-command"


async def test_online_add_validates_before_saving_and_stale_revision_does_not_send(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _ConfigRegistry()
    registry.online = True
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    device = await _create_device(async_client, owner)
    config = {
        "name": "corp",
        "transport": "streamable_http",
        "url": "https://mcp.example.test/mcp",
        "headers": {"Authorization": "Bearer secret"},
        "enabled_capabilities": [],
    }

    stale = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 9, "mcp_servers": [config]},
    )
    assert stale.status_code == 409
    assert registry.validations == []

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [config]},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["device"]["config_revision"] == 2
    assert payload["mcp_servers"][0]["headers"] == {"authorization": "<redacted>"}
    assert len(registry.validations) == 1
    assert registry.validations[0]["validate_servers"] == ("corp",)
    assert len(registry.pushes) == 1
    assert registry.pushes[0]["frame_id"] == UUID("0198e2c8-592a-7000-8000-000000000010")

    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        assert row.mcp_servers[0]["headers"] == {"authorization": "Bearer secret"}
        assert row.config_revision == 2


async def test_offline_add_and_failed_remote_validation_do_not_save(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _ConfigRegistry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    device = await _create_device(async_client, owner)
    config = {
        "name": "corp",
        "transport": "stdio",
        "command": "corp-mcp",
        "args": [],
        "cwd": None,
        "env": {},
        "enabled_capabilities": [],
    }

    offline = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [config]},
    )
    assert offline.status_code == 409
    assert offline.json()["code"] == "device_offline"

    registry.online = True

    async def fail_validation(**_kwargs: object) -> object:
        raise DeviceValidationError(
            (
                McpValidationFailure(
                    name="corp",
                    stage="initialize",
                    code="config_validation_failed",
                    message="MCP initialize failed",
                ),
            )
        )

    registry.validate_config = fail_validation  # type: ignore[method-assign]
    failed = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [config]},
    )
    assert failed.status_code == 422
    assert failed.json() == {
        "code": "config_validation_failed",
        "message": "MCP validation failed for 'corp'",
    }

    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.id == UUID(device["id"])))
        assert row is not None
        assert row.mcp_servers == []
        assert row.config_revision == 1


class _Transport:
    def __init__(self) -> None:
        self.sent: asyncio.Queue[str] = asyncio.Queue()

    async def send_text(self, payload: str) -> None:
        await self.sent.put(payload)

    async def send_binary(self, payload: bytes) -> None:
        del payload

    async def close(self, code: int, reason: str) -> None:
        del code, reason


async def test_registry_validation_exchange_is_generation_scoped_and_late_result_is_ignored() -> (
    None
):
    registry = DeviceRegistry()
    device_id = UUID("0198e2c8-592a-7000-8000-000000000001")
    user_id = UUID("0198e2c8-592a-7000-8000-000000000002")
    transport = _Transport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        secret_transport_safe=True,
    )
    assert handle is not None
    config = DeviceConfigFrame(
        workspace_path="~/workspace",
        restrict_to_workspace=True,
        ssrf_denylist=[],
        mcp_servers=[],
    )

    pending = asyncio.create_task(
        registry.validate_config(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="laptop",
            base_config_revision=1,
            candidate_config=config,
            validate_servers=("github",),
            timeout_seconds=1,
        )
    )
    sent = json.loads(await transport.sent.get())
    result = ConfigValidateResultFrame(
        id=UUID(sent["id"]),
        ok=True,
        source_catalog=SourceMcpCatalog(
            version=1,
            servers=[SourceMcpServerCatalog(name="github")],
        ),
        failures=[],
    )
    assert await registry.resolve_config_validate_result(handle, result)
    validated = await pending
    assert validated.id == result.id
    assert validated.source_catalog == result.source_catalog

    timed_out = asyncio.create_task(
        registry.validate_config(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="laptop",
            base_config_revision=1,
            candidate_config=config,
            validate_servers=("github",),
            timeout_seconds=0.01,
        )
    )
    second = json.loads(await transport.sent.get())
    try:
        await timed_out
    except TimeoutError:
        pass
    else:
        raise AssertionError("validation should time out")
    # A result that was validly issued but expired is a tombstone hit, not a
    # protocol violation and must not close the healthy generation.
    late = result.model_copy(update={"id": UUID(second["id"])})
    assert await registry.resolve_config_validate_result(handle, late)
    assert await registry.is_current(handle)
