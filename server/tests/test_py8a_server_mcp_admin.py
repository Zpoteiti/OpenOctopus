from __future__ import annotations

import asyncio
import traceback
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.api.admin.server_mcp import (
    ServerMcpCandidateValidator,
    ValidatedServerMcpCandidate,
    get_server_mcp_candidate_validator,
)
from openctopus_server.db.advisory import (
    lock_global_mcp_catalog_write,
    lock_server_mcp_config_write,
)
from openctopus_server.db.models import SystemConfig
from openctopus_server.devices.mcp_models import (
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
)
from openctopus_server.dto.server_mcp import ServerMcpRuntimeSlot
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.mcp.models import (
    ServerMcpEnvelope,
    ServerMcpServerConfig,
    empty_server_mcp_envelope,
    parse_server_mcp_configs,
    server_mcp_envelope_storage,
)
from openctopus_server.mcp.routes import build_composite_mcp_snapshot
from openctopus_server.mcp.supervisor import ServerMcpSupervisor
from openctopus_server.services import server_mcp


def _remote_config(
    *,
    url: str = "https://mcp.example.test/mcp",
    authorization: str = "Bearer first-secret",
) -> dict[str, object]:
    return {
        "name": "search",
        "transport": "streamable_http",
        "url": url,
        "headers": {"authorization": authorization},
        "enabled_capabilities": [],
        "max_concurrent_calls": 8,
    }


def _source_catalog(names: tuple[str, ...]) -> SourceMcpCatalog:
    return SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name=name,
                tools=[
                    SourceMcpTool(
                        raw_name="query",
                        description="Search the shared index.",
                        input_schema={
                            "type": "object",
                            "properties": {"q": {"type": "string"}},
                            "required": ["q"],
                            "additionalProperties": False,
                        },
                    )
                ],
            )
            for name in names
        ],
    )


@dataclass(frozen=True)
class FakeCandidate:
    source_catalog: SourceMcpCatalog


class _RuntimeClient:
    def __init__(
        self,
        *,
        enter: Callable[[], Awaitable[None]] | None = None,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.session = object()
        self.transport = object()
        self._enter = enter
        self._close = close

    async def __aenter__(self) -> object:
        if self._enter is not None:
            await self._enter()
        return self

    async def close(self) -> None:
        if self._close is not None:
            await self._close()


class FakeValidator(ServerMcpCandidateValidator):
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
        self.changed_calls: list[tuple[str, ...]] = []
        self.preflight_calls: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
        self.discarded: list[ValidatedServerMcpCandidate] = []
        self.published: list[
            tuple[ValidatedServerMcpCandidate, ServerMcpEnvelope]
        ] = []
        self.reconciled: list[ServerMcpEnvelope] = []
        self.before_return: Callable[[], Awaitable[None]] | None = None
        self.publish_error: BaseException | None = None
        self.reconcile_error: BaseException | None = None
        self.refresh: tuple[str, ...] = ()
        self.source_catalog: SourceMcpCatalog | None = None

    async def preflight(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
    ) -> None:
        self.preflight_calls.append(
            (tuple(config.name for config in configs), changed_names)
        )

    def refresh_names(self, envelope: ServerMcpEnvelope) -> tuple[str, ...]:
        del envelope
        return self.refresh

    def runtime_snapshot(
        self, envelope: ServerMcpEnvelope
    ) -> dict[str, ServerMcpRuntimeSlot]:
        del envelope
        return {}

    async def validate(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
        validate_servers: tuple[str, ...],
    ) -> ValidatedServerMcpCandidate:
        self.changed_calls.append(changed_names)
        self.calls.append(
            (tuple(config.name for config in configs), validate_servers)
        )
        if self.before_return is not None:
            await self.before_return()
        return FakeCandidate(
            source_catalog=self.source_catalog or _source_catalog(validate_servers)
        )

    async def discard(self, candidate: ValidatedServerMcpCandidate) -> None:
        self.discarded.append(candidate)

    async def publish(
        self,
        candidate: ValidatedServerMcpCandidate,
        envelope: ServerMcpEnvelope,
    ) -> None:
        self.published.append((candidate, envelope))
        if self.publish_error is not None:
            raise self.publish_error

    async def reconcile(self, envelope: ServerMcpEnvelope) -> None:
        self.reconciled.append(envelope)
        if self.reconcile_error is not None:
            raise self.reconcile_error


@pytest.fixture
def fake_validator(test_app) -> FakeValidator:
    validator = FakeValidator()
    test_app.dependency_overrides[get_server_mcp_candidate_validator] = lambda: validator
    yield validator
    test_app.dependency_overrides.pop(get_server_mcp_candidate_validator, None)


async def _stored_row(pg_engine) -> SystemConfig | None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        return await db.scalar(
            select(SystemConfig).where(SystemConfig.key == server_mcp.SERVER_MCP_CONFIG_KEY)
        )


async def _store_envelope(pg_engine, envelope: ServerMcpEnvelope) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(
            SystemConfig(
                key=server_mcp.SERVER_MCP_CONFIG_KEY,
                value=server_mcp_envelope_storage(envelope),
            )
        )
        await db.commit()


async def test_admin_get_synthesizes_revision_one_without_seeding_a_row(
    admin_client,
    pg_engine,
) -> None:
    response = await admin_client.get("/api/admin/server-mcp")

    assert response.status_code == 200
    assert response.json() == {
        "config_revision": 1,
        "mcp_servers": [],
        "mcp_catalog_digest": (
            "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf"
        ),
        "mcp_discovered": {},
        "runtimes": {},
    }
    assert await _stored_row(pg_engine) is None


async def test_admin_get_reports_replacement_active_and_draining_generations(
    admin_client,
    pg_engine,
    test_app,
) -> None:
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def delayed_close() -> None:
        close_started.set()
        await release_close.wait()

    clients = iter((_RuntimeClient(close=delayed_close), _RuntimeClient()))

    async def discover(name: str, _session: object) -> SourceMcpServerCatalog:
        return _source_catalog((name,)).servers[0]

    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: next(clients),
        discoverer=discover,
    )
    test_app.state.server_mcp_supervisor = supervisor
    try:
        first_configs = parse_server_mcp_configs([_remote_config()])
        first_candidate = await supervisor.validate(
            configs=first_configs,
            changed_names=("search",),
            validate_servers=("search",),
        )
        first = server_mcp.build_candidate_envelope(
            empty_server_mcp_envelope(),
            first_configs,
            validate_servers=("search",),
            source_catalog=first_candidate.source_catalog,
        )
        await supervisor.publish(first_candidate, first)
        first_generation = supervisor.ready_generations(first)["search"]

        changed_payload = first_configs[0].storage_dict()
        changed_payload["max_concurrent_calls"] = 9
        second_configs = parse_server_mcp_configs([changed_payload])
        second_candidate = await supervisor.validate(
            configs=second_configs,
            changed_names=("search",),
            validate_servers=("search",),
        )
        second = server_mcp.build_candidate_envelope(
            first,
            second_configs,
            validate_servers=("search",),
            source_catalog=second_candidate.source_catalog,
        )
        await _store_envelope(pg_engine, second)
        await supervisor.publish(second_candidate, second)
        await close_started.wait()

        response = await admin_client.get("/api/admin/server-mcp")

        assert response.status_code == 200, response.text
        slot = response.json()["runtimes"]["search"]
        assert slot["configured"] is True
        assert slot["active"]["state"] == "ready"
        assert slot["active"]["config_revision"] == 3
        assert slot["active"]["runtime_generation"] != str(first_generation)
        assert slot["draining"]["state"] == "draining"
        assert slot["draining"]["config_revision"] == 2
        assert slot["draining"]["runtime_generation"] == str(first_generation)
    finally:
        release_close.set()
        await supervisor.shutdown()


async def test_all_configured_mcp_can_be_down_without_losing_schema_or_health(
    admin_client,
    pg_engine,
    test_app,
) -> None:
    configs = parse_server_mcp_configs(
        [
            {
                "name": "first",
                "transport": "stdio",
                "command": "missing-first",
                "enabled_capabilities": [],
            },
            {
                "name": "second",
                "transport": "stdio",
                "command": "missing-second",
                "enabled_capabilities": [],
            },
        ]
    )
    envelope = server_mcp.build_candidate_envelope(
        empty_server_mcp_envelope(),
        configs,
        validate_servers=("first", "second"),
        source_catalog=_source_catalog(("first", "second")),
    )
    await _store_envelope(pg_engine, envelope)

    async def missing_executable() -> None:
        raise FileNotFoundError("missing MCP executable")

    async def discover(name: str, _session: object) -> SourceMcpServerCatalog:
        return _source_catalog((name,)).servers[0]

    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: _RuntimeClient(
            enter=missing_executable
        ),
        discoverer=discover,
    )
    test_app.state.server_mcp_supervisor = supervisor
    try:
        await supervisor.start(envelope)
        for _ in range(50):
            runtimes = supervisor.runtime_snapshot(envelope)
            if len(runtimes) == 2 and all(
                slot.active is not None and slot.active.state == "unavailable"
                for slot in runtimes.values()
            ):
                break
            await asyncio.sleep(0)

        assert supervisor.ready_generations(envelope) == {
            "first": None,
            "second": None,
        }
        assert all(
            slot.active is not None and slot.active.state == "unavailable"
            for slot in supervisor.runtime_snapshot(envelope).values()
        )

        admin_response = await admin_client.get("/api/admin/server-mcp")
        assert admin_response.status_code == 200, admin_response.text
        assert set(admin_response.json()["mcp_discovered"]) == {"first", "second"}

        composite = build_composite_mcp_snapshot(
            envelope,
            [],
            runtime_generations=supervisor.ready_generations(envelope),
        )
        assert {schema.name for schema in composite.schemas} == {
            "mcp_first_query",
            "mcp_second_query",
        }
        assert all(route.runtime_generation is None for route in composite.server_routes)

        health = await admin_client.get("/health")
        assert health.status_code == 200, health.text
        assert health.json() == {
            "status": "ok",
            "db": "connected",
            "object_storage": "connected",
        }
    finally:
        await supervisor.shutdown()


async def test_exact_noop_repairs_an_invalid_local_authority_fence(
    admin_client,
    fake_validator: FakeValidator,
    test_app,
) -> None:
    test_app.state.server_mcp_authority.invalidate()

    response = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": []},
    )

    assert response.status_code == 200
    assert fake_validator.calls == []
    assert [envelope.config_revision for envelope in fake_validator.reconciled] == [1]
    snapshot = test_app.state.server_mcp_authority.snapshot
    assert snapshot.valid is True
    assert snapshot.config_revision == 1


async def test_exact_noop_retries_runtime_reconcile_when_fence_already_matches(
    admin_client,
    fake_validator: FakeValidator,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200
    fake_validator.publish_error = RuntimeError("publish failed")
    fake_validator.reconcile_error = RuntimeError("reconcile failed")

    with pytest.raises(RuntimeError, match="reconcile failed"):
        await admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 2, "mcp_servers": []},
        )

    fake_validator.publish_error = None
    fake_validator.reconcile_error = None
    repaired = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 3, "mcp_servers": []},
    )

    assert repaired.status_code == 200
    assert repaired.json()["config_revision"] == 3
    assert [envelope.config_revision for envelope in fake_validator.reconciled] == [3, 3]


async def test_server_mcp_admin_api_requires_admin(async_client, user_client) -> None:
    async_client.cookies.clear()
    unauthenticated = await async_client.get("/api/admin/server-mcp")
    assert unauthenticated.status_code == 401
    assert unauthenticated.json()["code"] == ErrorCode.AUTH_UNAUTHORIZED

    login = await user_client.post(
        "/api/auth/login",
        json={"email": "user@test.com", "password": "testpassword"},
    )
    assert login.status_code == 200
    forbidden = await user_client.get("/api/admin/server-mcp")
    assert forbidden.status_code == 403
    assert forbidden.json()["code"] == ErrorCode.AUTH_FORBIDDEN

    put = await user_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": []},
    )
    assert put.status_code == 403
    assert put.json()["code"] == ErrorCode.AUTH_FORBIDDEN


async def test_first_effective_put_writes_revision_two_atomic_envelope_and_redacts(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    response = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["config_revision"] == 2
    assert body["mcp_servers"] == [
        _remote_config(authorization="<redacted>")
    ]
    assert body["mcp_discovered"] == {
        "search": {
            "tools": [
                {
                    "raw_name": "query",
                    "final_name": "mcp_search_query",
                    "enabled": True,
                }
            ],
            "resources": [],
            "resource_templates": [],
            "prompts": [],
        }
    }
    assert body["mcp_catalog_digest"]
    assert body["runtimes"] == {}
    assert fake_validator.calls == [(('search',), ('search',))]
    assert len(fake_validator.published) == 1
    assert fake_validator.published[0][0] is not None
    assert fake_validator.published[0][1].config_revision == 2

    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value["version"] == 1
    assert row.value["config_revision"] == 2
    assert row.value["mcp_servers"][0]["headers"] == {
        "authorization": "Bearer first-secret"
    }
    assert row.value["mcp_catalog"]["digest"] == body["mcp_catalog_digest"]
    assert server_mcp.parse_stored_envelope(row.value).config_revision == 2


async def test_request_and_stored_envelope_are_strict(
    admin_client,
    pg_engine,
) -> None:
    unknown = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 1,
            "mcp_servers": [],
            "unexpected": True,
        },
    )
    assert unknown.status_code == 422
    assert unknown.json() == {
        "code": "config_validation_failed",
        "message": "Server MCP configuration is invalid",
    }

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(
            SystemConfig(
                key=server_mcp.SERVER_MCP_CONFIG_KEY,
                value={
                    "version": 1,
                    "config_revision": 1,
                    "mcp_servers": [],
                    "mcp_catalog": {
                        "version": 1,
                        "digest": (
                            "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf"
                        ),
                        "servers": [],
                    },
                    "unexpected": True,
                },
            )
        )
        await db.commit()
        with pytest.raises(server_mcp.ServerMcpStorageCorruptionError):
            await server_mcp.load_envelope(db)


async def test_exact_noop_retains_secret_skips_validation_and_preserves_timestamp(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200
    first_row = await _stored_row(pg_engine)
    assert first_row is not None
    first_updated_at = first_row.updated_at

    noop = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 2,
            "mcp_servers": [_remote_config(authorization="<redacted>")],
        },
    )

    assert noop.status_code == 200
    assert noop.json()["config_revision"] == 2
    assert fake_validator.calls == [(('search',), ('search',))]
    assert len(fake_validator.published) == 1
    second_row = await _stored_row(pg_engine)
    assert second_row is not None
    assert second_row.updated_at == first_updated_at
    assert second_row.value["mcp_servers"][0]["headers"]["authorization"] == (
        "Bearer first-secret"
    )


async def test_whole_list_reordering_is_a_canonical_noop(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    calc = {
        "name": "calc",
        "transport": "stdio",
        "command": "python",
        "enabled_capabilities": [],
    }
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 1,
            "mcp_servers": [_remote_config(), calc],
        },
    )
    assert created.status_code == 200
    first_row = await _stored_row(pg_engine)
    assert first_row is not None

    reordered = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 2,
            "mcp_servers": [
                _remote_config(authorization="<redacted>"),
                calc,
            ][::-1],
        },
    )

    assert reordered.status_code == 200
    assert reordered.json()["config_revision"] == 2
    assert [config["name"] for config in reordered.json()["mcp_servers"]] == [
        "calc",
        "search",
    ]
    assert fake_validator.calls == [
        (("calc", "search"), ("calc", "search")),
    ]
    second_row = await _stored_row(pg_engine)
    assert second_row is not None
    assert second_row.updated_at == first_row.updated_at


async def test_same_config_refresh_replaces_runtime_without_writing_when_catalog_matches(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200
    first_row = await _stored_row(pg_engine)
    assert first_row is not None
    first_updated_at = first_row.updated_at
    fake_validator.refresh = ("search",)

    refreshed = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 2,
            "mcp_servers": [_remote_config(authorization="<redacted>")],
        },
    )

    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["config_revision"] == 2
    assert fake_validator.calls == [
        (("search",), ("search",)),
        (("search",), ("search",)),
    ]
    assert len(fake_validator.published) == 2
    assert fake_validator.published[-1][0] is not None
    assert fake_validator.published[-1][1].config_revision == 2
    second_row = await _stored_row(pg_engine)
    assert second_row is not None
    assert second_row.updated_at == first_updated_at


async def test_same_config_refresh_commits_when_discovery_changes(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200
    fake_validator.refresh = ("search",)
    fake_validator.source_catalog = SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name="search",
                tools=[
                    SourceMcpTool(
                        raw_name="lookup",
                        input_schema={"type": "object", "properties": {}},
                    )
                ],
            )
        ],
    )

    refreshed = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 2,
            "mcp_servers": [_remote_config(authorization="<redacted>")],
        },
    )

    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["config_revision"] == 3
    assert refreshed.json()["mcp_discovered"]["search"]["tools"][0][
        "final_name"
    ] == "mcp_search_lookup"
    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value["config_revision"] == 3


async def test_same_config_refresh_rechecks_db_authority_before_publish(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200
    fake_validator.refresh = ("search",)

    async def advance_authority() -> None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            row = await db.scalar(
                select(SystemConfig)
                .where(SystemConfig.key == server_mcp.SERVER_MCP_CONFIG_KEY)
                .with_for_update()
            )
            assert row is not None
            value = deepcopy(row.value)
            value["config_revision"] = 3
            row.value = value
            await db.commit()

    fake_validator.before_return = advance_authority
    refreshed = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 2,
            "mcp_servers": [_remote_config(authorization="<redacted>")],
        },
    )

    assert refreshed.status_code == 409
    assert refreshed.json()["code"] == ErrorCode.SERVER_MCP_CONFIG_CONFLICT
    assert len(fake_validator.discarded) == 1
    assert len(fake_validator.published) == 1


async def test_redaction_marker_cannot_cross_a_remote_sink(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200

    rejected = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 2,
            "mcp_servers": [
                _remote_config(
                    url="https://other.example.test/mcp",
                    authorization="<redacted>",
                )
            ],
        },
    )

    assert rejected.status_code == 422
    assert rejected.json()["code"] == ErrorCode.CONFIG_VALIDATION_FAILED
    assert fake_validator.calls == [(('search',), ('search',))]
    assert len(fake_validator.published) == 1
    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value["config_revision"] == 2
    assert row.value["mcp_servers"][0]["url"] == "https://mcp.example.test/mcp"


async def test_server_provider_capacity_is_validated_before_commit(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "openctopus_server.mcp.routes.PROVIDER_CAPABILITY_MAX",
        0,
    )

    rejected = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )

    assert rejected.status_code == 409
    assert rejected.json()["code"] == ErrorCode.MCP_SERVER_SCHEMA_LIMIT
    assert len(fake_validator.discarded) == 1
    assert await _stored_row(pg_engine) is None


def test_stored_envelope_rechecks_server_provider_capacity(monkeypatch) -> None:
    source = _source_catalog(("search",))
    configs = parse_server_mcp_configs([_remote_config()])
    envelope = server_mcp.build_candidate_envelope(
        empty_server_mcp_envelope(),
        configs,
        validate_servers=("search",),
        source_catalog=source,
    )
    monkeypatch.setattr(
        "openctopus_server.mcp.routes.PROVIDER_CAPABILITY_MAX",
        0,
    )

    with pytest.raises(server_mcp.ServerMcpStorageCorruptionError):
        server_mcp.parse_stored_envelope(server_mcp_envelope_storage(envelope))


async def test_deleting_all_servers_retains_the_row_and_bumps_revision(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200

    deleted = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 2, "mcp_servers": []},
    )

    assert deleted.status_code == 200
    assert deleted.json()["config_revision"] == 3
    assert deleted.json()["mcp_servers"] == []
    assert deleted.json()["mcp_discovered"] == {}
    assert fake_validator.calls == [(("search",), ("search",)), ((), ())]
    assert fake_validator.changed_calls[-1] == ("search",)
    assert len(fake_validator.published) == 2
    assert fake_validator.published[-1][0] is not None
    assert fake_validator.published[-1][1].config_revision == 3
    assert fake_validator.preflight_calls[-2:] == [
        ((), ("search",)),
        ((), ("search",)),
    ]
    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value == {
        "version": 1,
        "config_revision": 3,
        "mcp_servers": [],
        "mcp_catalog": {
            "version": 1,
            "digest": (
                "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf"
            ),
            "servers": [],
        },
    }


async def test_stale_initial_cas_rejects_before_validation(
    admin_client,
    fake_validator: FakeValidator,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200
    fake_validator.calls.clear()
    fake_validator.published.clear()

    stale = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 1,
            "mcp_servers": [
                _remote_config(url="https://new.example.test/mcp", authorization="new")
            ],
        },
    )

    assert stale.status_code == 409
    assert stale.json()["code"] == ErrorCode.SERVER_MCP_CONFIG_CONFLICT
    assert fake_validator.calls == []
    assert fake_validator.discarded == []


async def test_final_cas_rejects_a_revision_changed_during_validation(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    created = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )
    assert created.status_code == 200
    fake_validator.calls.clear()
    fake_validator.published.clear()

    async def advance_authority() -> None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            row = await db.scalar(
                select(SystemConfig)
                .where(SystemConfig.key == server_mcp.SERVER_MCP_CONFIG_KEY)
                .with_for_update()
            )
            assert row is not None
            value = deepcopy(row.value)
            value["config_revision"] = 3
            row.value = value
            await db.commit()

    fake_validator.before_return = advance_authority
    stale = await admin_client.put(
        "/api/admin/server-mcp",
        json={
            "base_config_revision": 2,
            "mcp_servers": [
                _remote_config(url="https://new.example.test/mcp", authorization="new")
            ],
        },
    )

    assert stale.status_code == 409
    assert stale.json()["code"] == ErrorCode.SERVER_MCP_CONFIG_CONFLICT
    assert len(fake_validator.discarded) == 1
    assert fake_validator.published == []
    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value["config_revision"] == 3
    assert row.value["mcp_servers"][0]["url"] == "https://mcp.example.test/mcp"


async def test_cancellation_during_validation_does_not_write(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    entered = asyncio.Event()
    blocker = asyncio.Event()

    async def block_validation() -> None:
        entered.set()
        await blocker.wait()

    fake_validator.before_return = block_validation
    request = asyncio.create_task(
        admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert await _stored_row(pg_engine) is None


async def test_cancellation_after_transition_starts_waits_for_commit(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
    monkeypatch,
) -> None:
    committed = asyncio.Event()
    release = asyncio.Event()
    original = server_mcp.commit_candidate

    async def commit_then_pause(*args: Any, **kwargs: Any):
        envelope = await original(*args, **kwargs)
        committed.set()
        await release.wait()
        return envelope

    monkeypatch.setattr(server_mcp, "commit_candidate", commit_then_pause)
    request = asyncio.create_task(
        admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
        )
    )
    await asyncio.wait_for(committed.wait(), timeout=1)
    request.cancel()
    await asyncio.sleep(0)
    assert not request.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await request

    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value["config_revision"] == 2
    assert len(fake_validator.published) == 1


async def test_publish_failure_reconciles_the_committed_authority(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
) -> None:
    fake_validator.publish_error = RuntimeError("publish failed")

    with pytest.raises(RuntimeError, match="publish failed"):
        await admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
        )

    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value["config_revision"] == 2
    assert len(fake_validator.discarded) == 1
    assert [envelope.config_revision for envelope in fake_validator.reconciled] == [2]


async def test_commit_ack_loss_publishes_the_durable_candidate(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
    test_app,
    monkeypatch,
) -> None:
    original = server_mcp.commit_candidate

    async def commit_then_lose_ack(*args: Any, **kwargs: Any):
        await original(*args, **kwargs)
        raise server_mcp.ServerMcpCommitOutcomeUnknownError(OSError("commit ack lost"))

    monkeypatch.setattr(server_mcp, "commit_candidate", commit_then_lose_ack)

    response = await admin_client.put(
        "/api/admin/server-mcp",
        json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
    )

    assert response.status_code == 200
    assert response.json()["config_revision"] == 2
    assert fake_validator.discarded == []
    assert [envelope.config_revision for _, envelope in fake_validator.published] == [2]
    assert test_app.state.server_mcp_authority.snapshot.config_revision == 2


async def test_commit_ack_recovery_waits_for_in_flight_first_writer(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
    test_app,
    monkeypatch,
) -> None:
    writer_locked = asyncio.Event()
    release_commit = asyncio.Event()
    recovery_started = asyncio.Event()
    writer_task: asyncio.Task[None] | None = None
    original_recovery_load = server_mcp.load_envelope_after_commit_boundary

    async def delayed_commit(*args: Any, **kwargs: Any):
        nonlocal writer_task
        candidate = kwargs["candidate"]
        assert isinstance(candidate, ServerMcpEnvelope)

        async def write_candidate() -> None:
            async with AsyncSession(pg_engine, expire_on_commit=False) as writer:
                await lock_global_mcp_catalog_write(writer)
                await lock_server_mcp_config_write(writer)
                writer.add(
                    SystemConfig(
                        key=server_mcp.SERVER_MCP_CONFIG_KEY,
                        value=server_mcp_envelope_storage(candidate),
                    )
                )
                await writer.flush()
                writer_locked.set()
                await release_commit.wait()
                await writer.commit()

        writer_task = asyncio.create_task(write_candidate())
        await writer_locked.wait()
        raise server_mcp.ServerMcpCommitOutcomeUnknownError(OSError("commit ack lost"))

    async def observe_recovery(db: AsyncSession) -> ServerMcpEnvelope:
        recovery_started.set()
        return await original_recovery_load(db)

    monkeypatch.setattr(server_mcp, "commit_candidate", delayed_commit)
    monkeypatch.setattr(
        server_mcp,
        "load_envelope_after_commit_boundary",
        observe_recovery,
    )
    request = asyncio.create_task(
        admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
        )
    )
    try:
        await asyncio.wait_for(recovery_started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not request.done()

        release_commit.set()
        response = await asyncio.wait_for(request, timeout=2)
        assert response.status_code == 200, response.text
        assert response.json()["config_revision"] == 2
        assert fake_validator.discarded == []
        assert [value.config_revision for _, value in fake_validator.published] == [2]
        assert test_app.state.server_mcp_authority.snapshot.config_revision == 2
    finally:
        release_commit.set()
        if writer_task is not None:
            await writer_task


async def test_commit_boundary_cancellation_converges_before_propagating(
    admin_client,
    fake_validator: FakeValidator,
    pg_engine,
    monkeypatch,
) -> None:
    original = server_mcp.commit_candidate

    async def commit_then_cancel(*args: Any, **kwargs: Any):
        await original(*args, **kwargs)
        raise server_mcp.ServerMcpCommitOutcomeUnknownError(asyncio.CancelledError())

    monkeypatch.setattr(server_mcp, "commit_candidate", commit_then_cancel)

    with pytest.raises(asyncio.CancelledError):
        await admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
        )

    row = await _stored_row(pg_engine)
    assert row is not None
    assert row.value["config_revision"] == 2
    assert len(fake_validator.published) == 1
    assert fake_validator.discarded == []


async def test_failed_commit_reconciles_the_unchanged_durable_authority(
    admin_client,
    fake_validator: FakeValidator,
    test_app,
    monkeypatch,
) -> None:
    secret = "commit-secret-sentinel"

    async def fail_before_commit(*args: Any, **kwargs: Any):
        del args, kwargs
        raise server_mcp.ServerMcpCommitOutcomeUnknownError(OSError(secret))

    monkeypatch.setattr(server_mcp, "commit_candidate", fail_before_commit)

    with pytest.raises(
        server_mcp.ServerMcpCommitFailedError,
        match="Server MCP configuration commit failed",
    ) as captured:
        await admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
        )

    assert secret not in "".join(traceback.format_exception(captured.value))
    assert len(fake_validator.discarded) == 1
    assert [envelope.config_revision for envelope in fake_validator.reconciled] == [1]
    snapshot = test_app.state.server_mcp_authority.snapshot
    assert snapshot.config_revision == 1
    assert snapshot.valid is True


async def test_unreadable_commit_outcome_invalidates_the_local_issue_fence(
    admin_client,
    fake_validator: FakeValidator,
    test_app,
    monkeypatch,
) -> None:
    original_load = server_mcp.load_envelope
    load_count = 0
    commit_secret = "commit-secret-sentinel"
    recovery_secret = "recovery-secret-sentinel"

    async def fail_before_commit(*args: Any, **kwargs: Any):
        del args, kwargs
        raise server_mcp.ServerMcpCommitOutcomeUnknownError(OSError(commit_secret))

    async def fail_recovery_read(*args: Any, **kwargs: Any):
        nonlocal load_count
        load_count += 1
        if load_count > 1:
            raise OSError(recovery_secret)
        return await original_load(*args, **kwargs)

    monkeypatch.setattr(server_mcp, "commit_candidate", fail_before_commit)
    monkeypatch.setattr(server_mcp, "load_envelope", fail_recovery_read)

    with pytest.raises(
        server_mcp.ServerMcpCommitFailedError,
        match="Server MCP configuration commit failed",
    ) as captured:
        await admin_client.put(
            "/api/admin/server-mcp",
            json={"base_config_revision": 1, "mcp_servers": [_remote_config()]},
        )

    rendered = "".join(traceback.format_exception(captured.value))
    assert commit_secret not in rendered
    assert recovery_secret not in rendered
    assert len(fake_validator.discarded) == 1
    assert test_app.state.server_mcp_authority.snapshot.valid is False
