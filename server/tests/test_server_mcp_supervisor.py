from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from mcp import types

from openctopus_server.devices.mcp_catalog import wrapped_capability_name
from openctopus_server.devices.mcp_models import (
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
)
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.catalog import build_server_persisted_catalog
from openctopus_server.mcp.models import ServerMcpEnvelope, parse_server_mcp_configs
from openctopus_server.mcp.routes import FrozenServerMcpEntryRoute
from openctopus_server.mcp.scheduler import AdmissionClock
from openctopus_server.mcp.supervisor import ServerMcpSupervisor

USER_A = UUID(int=1)
USER_B = UUID(int=2)
SESSION_A = UUID(int=3)
SESSION_B = UUID(int=4)


class FakeClock(AdmissionClock):
    def __init__(self) -> None:
        self.current = 0.0
        self.sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def now(self) -> float:
        return self.current

    async def sleep_until(self, deadline: float) -> None:
        if deadline <= self.current:
            return
        future = asyncio.get_running_loop().create_future()
        item = (deadline, future)
        self.sleepers.append(item)
        try:
            await future
        finally:
            self.sleepers.remove(item)

    def advance(self, seconds: float) -> None:
        self.current += seconds
        for deadline, future in tuple(self.sleepers):
            if deadline <= self.current and not future.done():
                future.set_result(None)


class FakeSession:
    def __init__(self, result: str) -> None:
        self.result = result
        self.calls = 0
        self.started = asyncio.Event()
        self.pending: asyncio.Future[types.CallToolResult] | None = None

    async def send_request(self, request: object, result_type: object) -> types.CallToolResult:
        del request, result_type
        self.calls += 1
        self.started.set()
        if self.pending is not None:
            return await self.pending
        return types.CallToolResult(content=[types.TextContent(type="text", text=self.result)])


class FakeClient:
    def __init__(
        self,
        label: str,
        *,
        enter: Callable[[], Awaitable[None]] | None = None,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.session = FakeSession(label)
        self.transport = object()
        self.closed = False
        self.enter = enter
        self.on_close = close
        self.handler: object | None = None

    async def __aenter__(self) -> FakeClient:
        if self.enter:
            await self.enter()
        return self

    async def close(self) -> None:
        if self.on_close is not None:
            await self.on_close()
        self.closed = True


def config(name: str = "search", *, transport: str = "streamable_http"):
    details = (
        {"command": "fake-mcp"} if transport == "stdio" else {"url": f"https://{name}.example/mcp"}
    )
    return parse_server_mcp_configs(
        [
            {
                "name": name,
                "transport": transport,
                "enabled_capabilities": [],
                **details,
            }
        ]
    )[0]


def source(name: str = "search", *, schema_type: str = "string") -> SourceMcpServerCatalog:
    return SourceMcpServerCatalog(
        name=name,
        tools=[
            SourceMcpTool(
                raw_name="lookup",
                description="Lookup",
                input_schema={"type": "object", "properties": {"query": {"type": schema_type}}},
                output_schema=None,
            )
        ],
        resources=[],
        resource_templates=[],
        prompts=[],
    )


def envelope(*configs) -> ServerMcpEnvelope:
    catalog = build_server_persisted_catalog(
        configs,
        SourceMcpCatalog(version=1, servers=[source(item.name) for item in configs]),
        entry_id_factory=new_uuid7,
    )
    return ServerMcpEnvelope(
        version=1, config_revision=2, mcp_servers=list(configs), mcp_catalog=catalog
    )


def route(
    private: ServerMcpEnvelope, generations, name: str = "search"
) -> FrozenServerMcpEntryRoute:
    entry = next(server for server in private.mcp_catalog.servers if server.name == name).entries[0]
    return FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=private.config_revision,
        catalog_digest=private.mcp_catalog.digest,
        runtime_generation=generations[name],
        server=name,
        surface="tool",
        raw_name="lookup",
        invocation_identity="lookup",
        final_name=entry.final_name,
    )


def supervisor(
    *,
    clock: FakeClock | None = None,
    max_clients: int = 256,
    max_starting: int | None = None,
    discoverer=None,
    enter=None,
    close=None,
):
    clients: list[FakeClient] = []

    def factory(_config, **_kwargs):
        client = FakeClient(f"client-{len(clients)}", enter=enter, close=close)
        client.handler = _kwargs.get("message_handler")
        clients.append(client)
        return client

    async def default_discover(name, _session):
        return source(name)

    value = ServerMcpSupervisor(
        client_factory=factory,
        discoverer=discoverer or default_discover,
        clock=clock,
        max_clients=max_clients,
        max_stdio_clients=max_clients,
        max_starting=max_starting or max_clients,
        idle_seconds=600,
    )
    return value, clients


async def test_private_clients_are_reused_only_within_the_same_conversation() -> None:
    service, clients = supervisor()
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        first, first_generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        again, again_generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        assert first_generations == again_generations
        assert first.mcp_catalog.digest == again.mcp_catalog.digest
        result = await service.dispatch_server_mcp(
            route=route(first, first_generations),
            user_id=USER_A,
            session_id=SESSION_A,
            name=route(first, first_generations).final_name,
            args={"query": "a"},
        )
        assert not result.is_error
        assert len(clients) == 1
    async with service.run(user_id=USER_B, session_id=SESSION_A):
        other, other_generations = await service.prepare(
            user_id=USER_B, session_id=SESSION_A, envelope=saved
        )
        assert other_generations["search"] != first_generations["search"]
        denied = await service.dispatch_server_mcp(
            route=route(first, first_generations),
            user_id=USER_B,
            session_id=SESSION_A,
            name=route(first, first_generations).final_name,
            args={},
        )
        assert denied.code == ErrorCode.TOOL_MCP_UNAVAILABLE
        assert other.mcp_catalog.digest == first.mcp_catalog.digest
    assert len(clients) == 2
    await service.shutdown()
    assert all(client.closed for client in clients)


async def test_each_prepare_discovers_new_schema_on_same_client() -> None:
    calls = 0

    async def discover(name, _session):
        nonlocal calls
        calls += 1
        return source(name, schema_type="string" if calls == 1 else "integer")

    service, clients = supervisor(discoverer=discover)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        first, generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        updated, updated_generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        assert len(clients) == 1
        assert generations == updated_generations
        assert first.mcp_catalog.digest != updated.mcp_catalog.digest
        assert route(first, generations).entry_id == route(updated, updated_generations).entry_id
        denied = await service.dispatch_server_mcp(
            route=route(first, generations),
            user_id=USER_A,
            session_id=SESSION_A,
            name=route(first, generations).final_name,
            args={},
        )
        assert denied.code == ErrorCode.TOOL_MCP_UNAVAILABLE
    await service.shutdown()


async def test_idle_expiry_and_lru_eviction_never_evict_an_active_run() -> None:
    clock = FakeClock()
    service, clients = supervisor(clock=clock, max_clients=1)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        async with service.run(user_id=USER_B, session_id=SESSION_B):
            with pytest.raises(ConfigError) as exc:
                await service.prepare(user_id=USER_B, session_id=SESSION_B, envelope=saved)
            assert exc.value.code == ErrorCode.TOOL_MCP_BUSY
    async with service.run(user_id=USER_B, session_id=SESSION_B):
        await service.prepare(user_id=USER_B, session_id=SESSION_B, envelope=saved)
        assert clients[0].closed
        assert len(clients) == 2
    clock.advance(599)
    await asyncio.sleep(0)
    assert not clients[1].closed
    clock.advance(2)
    for _ in range(100):
        if not service._entries:
            break
        await asyncio.sleep(0)
    assert clients[1].closed
    assert not service._entries
    assert not service._catalogs
    assert not service._session_locks
    assert not service._forgotten
    await service.shutdown()


async def test_admin_validation_is_temporary_and_counts_toward_capacity() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def enter() -> None:
        started.set()
        await release.wait()

    service, clients = supervisor(max_clients=1, enter=enter)
    saved = envelope(config())
    await service.start(saved)
    task = asyncio.create_task(
        service.validate(
            configs=tuple(saved.mcp_servers),
            changed_names=("search",),
            validate_servers=("search",),
        )
    )
    await started.wait()
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        with pytest.raises(ConfigError) as exc:
            await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        assert exc.value.code == ErrorCode.TOOL_MCP_BUSY
    release.set()
    candidate = await task
    assert candidate.source_catalog.servers[0].name == "search"
    assert clients[0].closed
    await service.publish(candidate, saved)
    assert service.runtime_snapshot(saved)["search"].idle_sessions == 0
    await service.shutdown()


async def test_startup_saturation_does_not_evict_idle_client() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    enters = 0

    async def enter() -> None:
        nonlocal enters
        enters += 1
        if enters == 2:
            started.set()
            await release.wait()

    service, clients = supervisor(max_clients=3, max_starting=1, enter=enter)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
    async with service.run(user_id=USER_B, session_id=SESSION_B):
        opening = asyncio.create_task(
            service.prepare(user_id=USER_B, session_id=SESSION_B, envelope=saved)
        )
        await started.wait()
        async with service.run(user_id=USER_A, session_id=SESSION_B):
            with pytest.raises(ConfigError) as exc:
                await service.prepare(user_id=USER_A, session_id=SESSION_B, envelope=saved)
            assert exc.value.code == ErrorCode.TOOL_MCP_BUSY
        assert not clients[0].closed
        release.set()
        await opening
    await service.shutdown()


async def test_forget_session_defers_close_until_active_run_exits() -> None:
    service, clients = supervisor()
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        await service.forget_session(USER_A, SESSION_A)
        assert not clients[0].closed
    assert clients[0].closed
    await service.shutdown()


async def test_removed_selected_capability_is_suppressed_without_changing_admin_config() -> None:
    async def discover(name, _session):
        return SourceMcpServerCatalog(
            name=name, tools=[], resources=[], resource_templates=[], prompts=[]
        )

    selected = wrapped_capability_name("search", "lookup")
    configured = parse_server_mcp_configs(
        [
            {
                "name": "search",
                "transport": "streamable_http",
                "url": "https://search.example/mcp",
                "enabled_capabilities": [selected],
            }
        ]
    )[0]
    saved = envelope(configured)
    service, _ = supervisor(discoverer=discover)
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        private, _ = await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        assert private.mcp_servers[0].enabled_capabilities == [selected]
        assert private.mcp_catalog.servers[0].entries == []
    await service.shutdown()


async def test_list_change_invalidates_issued_catalog_until_next_prepare() -> None:
    service, clients = supervisor()
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        private, generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        first_route = route(private, generations)
        handler = clients[0].handler
        assert handler is not None
        handler._emit("list_changed")
        for _ in range(100):
            if service._entries[(USER_A, SESSION_A, "search")].stale:
                break
            await asyncio.sleep(0)
        assert service._entries[(USER_A, SESSION_A, "search")].stale
        denied = await service.dispatch_server_mcp(
            route=first_route,
            user_id=USER_A,
            session_id=SESSION_A,
            name=first_route.final_name,
            args={},
        )
        assert denied.code == ErrorCode.TOOL_MCP_UNAVAILABLE
        refreshed, same_generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        assert same_generations == generations
        assert refreshed.mcp_catalog.digest == private.mcp_catalog.digest
    await service.shutdown()


async def test_cancelled_open_closes_transport_and_releases_capacity() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def enter() -> None:
        started.set()
        await release.wait()

    service, clients = supervisor(max_clients=1, enter=enter)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        preparation = asyncio.create_task(
            service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        )
        await started.wait()
        preparation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await preparation
    assert clients[0].closed
    release.set()
    async with service.run(user_id=USER_B, session_id=SESSION_B):
        await service.prepare(user_id=USER_B, session_id=SESSION_B, envelope=saved)
    await service.shutdown()


async def test_cancelled_rediscovery_closes_unavailable_client_on_run_exit() -> None:
    started = asyncio.Event()
    calls = 0

    async def discover(name, _session):
        nonlocal calls
        calls += 1
        if calls == 2:
            started.set()
            await asyncio.Event().wait()
        return source(name)

    service, clients = supervisor(max_clients=1, discoverer=discover)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        preparation = asyncio.create_task(
            service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        )
        await started.wait()
        preparation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await preparation
    assert clients[0].closed
    assert service.runtime_snapshot(saved)["search"].idle_sessions == 0
    await service.shutdown()


async def test_failed_cleanup_retains_capacity_until_retry_completes() -> None:
    close_attempts = 0

    async def close() -> None:
        nonlocal close_attempts
        close_attempts += 1
        if close_attempts == 1:
            raise RuntimeError("cleanup failed")

    service, clients = supervisor(max_clients=1, close=close)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
    await service.forget_session(USER_A, SESSION_A)
    assert service.runtime_snapshot(saved)["search"].closing_sessions == 1
    assert not clients[0].closed
    async with service.run(user_id=USER_B, session_id=SESSION_B):
        await service.prepare(user_id=USER_B, session_id=SESSION_B, envelope=saved)
    assert clients[0].closed
    assert close_attempts >= 2
    await service.shutdown()


async def test_admin_validation_rejects_incomplete_cleanup() -> None:
    async def close() -> None:
        raise RuntimeError("cleanup failed")

    service, _ = supervisor(max_clients=1, close=close)
    saved = envelope(config())
    await service.start(saved)
    with pytest.raises(ConfigError) as exc:
        await service.validate(
            configs=tuple(saved.mcp_servers),
            changed_names=("search",),
            validate_servers=("search",),
        )
    assert exc.value.code == ErrorCode.TOOL_MCP_BUSY
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        with pytest.raises(ConfigError) as busy:
            await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
        assert busy.value.code == ErrorCode.TOOL_MCP_BUSY
    await service.shutdown()


async def test_admin_validation_after_shutdown_does_not_open_client() -> None:
    service, clients = supervisor()
    saved = envelope(config())
    await service.start(saved)
    await service.begin_shutdown()
    with pytest.raises(ConfigError) as exc:
        await service.validate(
            configs=tuple(saved.mcp_servers),
            changed_names=("search",),
            validate_servers=("search",),
        )
    assert exc.value.code == ErrorCode.SERVER_MCP_CONFIG_CONFLICT
    assert clients == []
    await service.shutdown()


async def test_forget_user_marks_run_without_open_clients() -> None:
    service, _ = supervisor()
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        await service.forget_user(USER_A)
        with pytest.raises(ConfigError):
            await service.prepare(user_id=USER_A, session_id=SESSION_A, envelope=saved)
    assert not service._forgotten
    await service.shutdown()


async def test_remote_timeout_keeps_issued_slot_until_late_result_without_replay() -> None:
    clock = FakeClock()
    service, clients = supervisor(clock=clock)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        private, generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        selected = route(private, generations)
        client = clients[0]
        client.session.pending = asyncio.get_running_loop().create_future()
        call = asyncio.create_task(
            service.dispatch_server_mcp(
                route=selected,
                user_id=USER_A,
                session_id=SESSION_A,
                name=selected.final_name,
                args={},
            )
        )
        await client.session.started.wait()
        assert service.coordinator_snapshot().reserved == 1
        for _ in range(100):
            if any(deadline == 60 for deadline, _ in clock.sleepers):
                break
            await asyncio.sleep(0)
        assert any(deadline == 60 for deadline, _ in clock.sleepers)
        clock.advance(61)
        result = await call
        assert result.code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN
        assert service.coordinator_snapshot().draining == 1
        assert client.session.calls == 1
        client.session.pending.set_result(
            types.CallToolResult(content=[types.TextContent(type="text", text="late")])
        )
        await service.wait_background()
        assert service.coordinator_snapshot().reserved == 0
        assert client.session.calls == 1
    await service.shutdown()
    assert service.coordinator_snapshot().reserved == 0


async def test_remote_cancellation_drains_late_result_without_replay() -> None:
    service, clients = supervisor()
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        private, generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        selected = route(private, generations)
        client = clients[0]
        client.session.pending = asyncio.get_running_loop().create_future()
        call = asyncio.create_task(
            service.dispatch_server_mcp(
                route=selected,
                user_id=USER_A,
                session_id=SESSION_A,
                name=selected.final_name,
                args={},
            )
        )
        await client.session.started.wait()
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        assert service.coordinator_snapshot().draining == 1
        assert client.session.calls == 1
        client.session.pending.set_result(
            types.CallToolResult(content=[types.TextContent(type="text", text="late")])
        )
        await service.wait_background()
        assert service.coordinator_snapshot().reserved == 0
        assert client.session.calls == 1
    await service.shutdown()
    assert service.coordinator_snapshot().reserved == 0


async def test_remote_late_drain_deadline_closes_unresolved_client() -> None:
    clock = FakeClock()
    service, clients = supervisor(clock=clock)
    saved = envelope(config())
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        private, generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        selected = route(private, generations)
        client = clients[0]
        client.session.pending = asyncio.get_running_loop().create_future()
        call = asyncio.create_task(
            service.dispatch_server_mcp(
                route=selected,
                user_id=USER_A,
                session_id=SESSION_A,
                name=selected.final_name,
                args={},
            )
        )
        await client.session.started.wait()
        for _ in range(100):
            if any(deadline == 60 for deadline, _ in clock.sleepers):
                break
            await asyncio.sleep(0)
        clock.advance(61)
        result = await call
        assert result.code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN
        assert service.coordinator_snapshot().draining == 1
        for _ in range(100):
            if any(deadline == 121 for deadline, _ in clock.sleepers):
                break
            await asyncio.sleep(0)
        assert any(deadline == 121 for deadline, _ in clock.sleepers)
        clock.advance(61)
        await service.wait_background()
        assert client.closed
        assert client.session.calls == 1
        assert service.coordinator_snapshot().reserved == 0
    await service.shutdown()
    assert service.coordinator_snapshot().reserved == 0


async def test_stdio_cancellation_closes_private_process_and_releases_slot() -> None:
    service, clients = supervisor()
    saved = envelope(config(transport="stdio"))
    await service.start(saved)
    async with service.run(user_id=USER_A, session_id=SESSION_A):
        private, generations = await service.prepare(
            user_id=USER_A, session_id=SESSION_A, envelope=saved
        )
        selected = route(private, generations)
        client = clients[0]
        client.session.pending = asyncio.get_running_loop().create_future()
        call = asyncio.create_task(
            service.dispatch_server_mcp(
                route=selected,
                user_id=USER_A,
                session_id=SESSION_A,
                name=selected.final_name,
                args={},
            )
        )
        await client.session.started.wait()
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        assert client.closed
        assert client.session.calls == 1
        assert service.coordinator_snapshot().reserved == 0
        denied = await service.dispatch_server_mcp(
            route=selected,
            user_id=USER_A,
            session_id=SESSION_A,
            name=selected.final_name,
            args={},
        )
        assert denied.code == ErrorCode.TOOL_MCP_UNAVAILABLE
    await service.shutdown()
    assert service.coordinator_snapshot().reserved == 0
