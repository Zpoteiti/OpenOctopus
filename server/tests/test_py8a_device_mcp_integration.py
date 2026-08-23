from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.api import devices as devices_api
from openctopus_server.db.models import Device, SystemConfig
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.mcp_catalog import build_persisted_catalog
from openctopus_server.devices.mcp_models import (
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
    parse_mcp_server_configs,
)
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.devices.registry import ConnectionHandle, DeviceUnavailableError
from openctopus_server.mcp.catalog import build_server_persisted_catalog
from openctopus_server.mcp.models import (
    ServerMcpEnvelope,
    empty_server_mcp_envelope,
    parse_server_mcp_configs,
    server_mcp_envelope_storage,
)


async def test_device_api_projection_retries_across_server_authority_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = empty_server_mcp_envelope()
    second = first.model_copy(update={"config_revision": 2})
    envelopes = iter((first, second, second, second))
    device_reads = 0

    async def load_server(_db):  # type: ignore[no-untyped-def]
        return next(envelopes)

    async def load_devices(_db, *, user_id):  # type: ignore[no-untyped-def]
        nonlocal device_reads
        del user_id
        device_reads += 1
        return []

    monkeypatch.setattr(devices_api.server_mcp, "load_envelope", load_server)
    monkeypatch.setattr(devices_api.devices, "list_owned", load_devices)

    snapshots, envelope, _projection = await devices_api._owner_mcp_projection(  # noqa: SLF001
        object(),  # type: ignore[arg-type]
        user_id=UUID("01890f7c-bb80-7000-8000-000000000099"),
    )

    assert snapshots == []
    assert envelope.config_revision == 2
    assert device_reads == 2


async def _register(client: Any) -> dict[str, Any]:
    response = await client.post(
        "/api/auth/register",
        json={"email": "owner@test.com", "password": "testpassword", "name": "Owner"},
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


async def _create_device(client: Any, owner: dict[str, Any]) -> dict[str, Any]:
    response = await _request_as(
        client,
        owner,
        "POST",
        "/api/devices",
        json={"name": "Laptop"},
    )
    assert response.status_code == 201, response.text
    return response.json()["device"]


def _stdio_config(name: str, *, command: str = "demo-mcp") -> dict[str, object]:
    return {
        "name": name,
        "transport": "stdio",
        "command": command,
        "args": [],
        "cwd": None,
        "env": {},
        "enabled_capabilities": [],
    }


def _source(name: str, raw_name: str = "find") -> SourceMcpCatalog:
    return SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name=name,
                tools=[
                    SourceMcpTool(
                        raw_name=raw_name,
                        input_schema={"type": "object", "properties": {}},
                    )
                ],
            )
        ],
    )


def _server_envelope(
    name: str,
    *,
    raw_name: str = "find",
    revision: int = 2,
) -> ServerMcpEnvelope:
    configs = parse_server_mcp_configs(
        [
            {
                "name": name,
                "transport": "streamable_http",
                "url": "https://mcp.example.test/mcp",
                "headers": {},
                "enabled_capabilities": [],
            }
        ]
    )
    catalog = build_server_persisted_catalog(
        configs,
        _source(name, raw_name),
        entry_id_factory=new_uuid7,
    )
    return ServerMcpEnvelope(
        version=1,
        config_revision=revision,
        mcp_servers=list(configs),
        mcp_catalog=catalog,
    )


async def _store_server_envelope(pg_engine: Any, envelope: ServerMcpEnvelope) -> None:
    async with AsyncSession(pg_engine) as db:
        db.add(
            SystemConfig(
                key="server_mcp",
                value=server_mcp_envelope_storage(envelope),
            )
        )
        await db.commit()


class _Registry:
    def __init__(self) -> None:
        self.online = False
        self.validations: list[dict[str, Any]] = []
        self.aborted = 0
        self.discarded = 0

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
        return type(
            "Validation",
            (),
            {
                "id": UUID("0198e2c8-592a-7000-8000-000000000010"),
                "handle": ConnectionHandle(device_id=kwargs["device_id"], generation=1),
                "source_catalog": SourceMcpCatalog(
                    version=1,
                    servers=[
                        SourceMcpServerCatalog(name=name)
                        for name in kwargs["validate_servers"]
                    ],
                ),
            },
        )()

    async def begin_config_update(self, **_kwargs: object) -> bool:
        return self.online

    async def abort_config_update(self, **_kwargs: object) -> None:
        self.aborted += 1

    async def discard_validated_config(self, _validation: object) -> None:
        self.discarded += 1

    async def push_config(self, **_kwargs: object) -> bool:
        return True


async def test_reserved_server_name_rejects_device_add_before_remote_validation(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _Registry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    await _create_device(async_client, owner)
    await _store_server_envelope(pg_engine, _server_envelope("search"))

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [_stdio_config("search")]},
    )

    assert response.status_code == 409, response.text
    assert response.json()["code"] == "mcp_name_reserved_by_server"
    assert registry.validations == []


async def test_reserved_existing_config_allows_noop_and_delete_but_not_modify(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _Registry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    created = await _create_device(async_client, owner)
    stored = _stdio_config("search")
    configs = parse_mcp_server_configs([stored])
    catalog = build_persisted_catalog(
        configs,
        _source("search"),
        entry_id_factory=new_uuid7,
    )
    await _store_server_envelope(pg_engine, _server_envelope("search"))
    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        row.mcp_servers = [config.storage_dict() for config in configs]
        row.mcp_catalog = catalog.model_dump(mode="json")
        await db.commit()

    noop = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [stored]},
    )
    assert noop.status_code == 200, noop.text
    assert noop.json()["device"]["config_revision"] == 1
    assert noop.json()["mcp_servers"][0]["effective_status"] == "shadowed_by_server"

    modified = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={
            "base_config_revision": 1,
            "mcp_servers": [_stdio_config("search", command="changed-mcp")],
        },
    )
    assert modified.status_code == 409, modified.text
    assert modified.json()["code"] == "mcp_name_reserved_by_server"
    assert registry.validations == []

    deleted = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": []},
    )
    assert deleted.status_code == 200, deleted.text
    assert deleted.json()["device"]["config_revision"] == 2
    assert deleted.json()["mcp_servers"] == []


async def test_reserved_existing_config_can_rename_to_an_unreserved_name(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _Registry()
    registry.online = True
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    created = await _create_device(async_client, owner)
    configs = parse_mcp_server_configs([_stdio_config("search")])
    catalog = build_persisted_catalog(
        configs,
        _source("search"),
        entry_id_factory=new_uuid7,
    )
    await _store_server_envelope(pg_engine, _server_envelope("search"))
    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        row.mcp_servers = [config.storage_dict() for config in configs]
        row.mcp_catalog = catalog.model_dump(mode="json")
        await db.commit()

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [_stdio_config("other")]},
    )

    assert response.status_code == 200, response.text
    assert response.json()["device"]["config_revision"] == 2
    assert response.json()["mcp_servers"][0]["name"] == "other"
    assert response.json()["mcp_servers"][0]["effective_status"] == "active"
    assert registry.validations[0]["validate_servers"] == ("other",)


async def test_device_projection_shadows_and_restores_without_device_write(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _Registry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    created = await _create_device(async_client, owner)
    configs = parse_mcp_server_configs([_stdio_config("search")])
    catalog = build_persisted_catalog(
        configs,
        _source("search"),
        entry_id_factory=new_uuid7,
    )
    await _store_server_envelope(pg_engine, _server_envelope("search"))
    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        row.mcp_servers = [config.storage_dict() for config in configs]
        row.mcp_catalog = catalog.model_dump(mode="json")
        await db.commit()

    shadowed = await _request_as(
        async_client,
        owner,
        "GET",
        "/api/devices/laptop/config",
    )

    assert shadowed.status_code == 200, shadowed.text
    payload = shadowed.json()
    assert payload["mcp_servers"][0]["effective_status"] == "shadowed_by_server"
    assert payload["mcp_servers"][0]["shadowed_by"] == "search"
    capability = payload["mcp_discovered"]["search"]["tools"][0]
    assert capability["enabled"] is True
    assert capability["provider_visible"] is False
    assert capability["suppression_reason"] == "server_namespace_reserved"

    listed = await _request_as(async_client, owner, "GET", "/api/devices")
    assert listed.status_code == 200, listed.text
    summary = listed.json()[0]
    assert summary["mcp_enabled_capability_count"] == 1
    assert summary["mcp_provider_visible_capability_count"] == 0
    assert summary["mcp_suppressed_capability_count"] == 1

    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(SystemConfig).where(SystemConfig.key == "server_mcp"))
        assert row is not None
        empty = empty_server_mcp_envelope().model_copy(update={"config_revision": 3})
        row.value = server_mcp_envelope_storage(empty)
        await db.commit()

    restored = await _request_as(
        async_client,
        owner,
        "GET",
        "/api/devices/laptop/config",
    )
    restored_payload = restored.json()
    assert restored_payload["mcp_servers"][0]["effective_status"] == "active"
    assert restored_payload["mcp_servers"][0]["shadowed_by"] is None
    restored_capability = restored_payload["mcp_discovered"]["search"]["tools"][0]
    assert restored_capability["provider_visible"] is True
    assert restored_capability["suppression_reason"] is None

    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        assert row.config_revision == 1
        assert row.mcp_catalog == catalog.model_dump(mode="json")


async def test_exact_final_name_collision_is_capability_only_suppression(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    registry = _Registry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    created = await _create_device(async_client, owner)
    configs = parse_mcp_server_configs([_stdio_config("foo")])
    catalog = build_persisted_catalog(
        configs,
        _source("foo", "bar_baz"),
        entry_id_factory=new_uuid7,
    )
    await _store_server_envelope(
        pg_engine,
        _server_envelope("foo_bar", raw_name="baz"),
    )
    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        row.mcp_servers = [config.storage_dict() for config in configs]
        row.mcp_catalog = catalog.model_dump(mode="json")
        await db.commit()

    response = await _request_as(
        async_client,
        owner,
        "GET",
        "/api/devices/laptop/config",
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["mcp_servers"][0]["effective_status"] == "active"
    assert payload["mcp_servers"][0]["shadowed_by"] is None
    capability = payload["mcp_discovered"]["foo"]["tools"][0]
    assert capability["provider_visible"] is False
    assert capability["suppression_reason"] == "server_final_name_collision"


async def test_changed_device_server_cannot_commit_an_exact_server_name_collision(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    class _CollisionRegistry(_Registry):
        async def validate_config(self, **kwargs: Any) -> Any:
            validation = await super().validate_config(**kwargs)
            validation.source_catalog = _source("foo", "bar_baz")
            return validation

    registry = _CollisionRegistry()
    registry.online = True
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    created = await _create_device(async_client, owner)
    await _store_server_envelope(
        pg_engine,
        _server_envelope("foo_bar", raw_name="baz"),
    )

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [_stdio_config("foo")]},
    )

    assert response.status_code == 409, response.text
    assert response.json()["code"] == "mcp_schema_collision"
    assert registry.aborted == 1
    assert registry.discarded == 1
    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        assert row.mcp_servers == []
        assert row.config_revision == 1


async def test_device_can_modify_a_later_collision_to_disable_it(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    class _CollisionRegistry(_Registry):
        async def validate_config(self, **kwargs: Any) -> Any:
            validation = await super().validate_config(**kwargs)
            validation.source_catalog = _source("foo", "bar_baz")
            return validation

    registry = _CollisionRegistry()
    registry.online = True
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    created = await _create_device(async_client, owner)
    configs = parse_mcp_server_configs([_stdio_config("foo")])
    catalog = build_persisted_catalog(
        configs,
        _source("foo", "bar_baz"),
        entry_id_factory=new_uuid7,
    )
    await _store_server_envelope(
        pg_engine,
        _server_envelope("foo_bar", raw_name="baz"),
    )
    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        row.mcp_servers = [config.storage_dict() for config in configs]
        row.mcp_catalog = catalog.model_dump(mode="json")
        await db.commit()
    disabled = {**_stdio_config("foo"), "enabled_capabilities": None}

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [disabled]},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["device"]["config_revision"] == 2
    capability = payload["mcp_discovered"]["foo"]["tools"][0]
    assert capability["enabled"] is False
    assert capability["provider_visible"] is False
    assert capability["suppression_reason"] is None


async def test_admin_reservation_added_after_validation_wins_final_commit_race(
    async_client: Any,
    test_app: Any,
    pg_engine: Any,
) -> None:
    envelope = _server_envelope("search")

    class _RacingRegistry(_Registry):
        async def begin_config_update(self, **_kwargs: object) -> bool:
            await _store_server_envelope(pg_engine, envelope)
            return True

    registry = _RacingRegistry()
    registry.online = True
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    created = await _create_device(async_client, owner)

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"base_config_revision": 1, "mcp_servers": [_stdio_config("search")]},
    )

    assert response.status_code == 409, response.text
    assert response.json()["code"] == "mcp_name_reserved_by_server"
    assert len(registry.validations) == 1
    assert registry.aborted == 1
    assert registry.discarded == 1
    async with AsyncSession(pg_engine) as db:
        row = await db.get(Device, UUID(created["id"]))
        assert row is not None
        assert row.mcp_servers == []
        assert row.config_revision == 1


async def test_name_conflict_with_mcp_payload_returns_device_name_taken(
    async_client: Any,
    test_app: Any,
) -> None:
    registry = _Registry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client)
    await _create_device(async_client, owner)
    second = await _request_as(
        async_client,
        owner,
        "POST",
        "/api/devices",
        json={"name": "Desktop"},
    )
    assert second.status_code == 201, second.text

    response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={
            "base_config_revision": 1,
            "name": "desktop",
            "mcp_servers": [],
        },
    )

    assert response.status_code == 409, response.text
    assert response.json()["code"] == "device_name_taken"
