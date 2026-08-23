from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from types import MappingProxyType
from uuid import UUID

import pytest
from mcp import types
from mcp.shared.exceptions import McpError

from openctopus_server.devices.mcp_models import SourceMcpCatalog, SourceMcpServerCatalog
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.catalog import build_server_persisted_catalog
from openctopus_server.mcp.models import ServerMcpEnvelope, parse_server_mcp_configs
from openctopus_server.mcp.routes import FrozenServerMcpEntryRoute
from openctopus_server.mcp.runtime import ServerMcpMessageHandler
from openctopus_server.mcp.scheduler import AdmissionClock
from openctopus_server.mcp.supervisor import (
    PROCESS_RESERVED_NAME_MAX,
    ServerMcpSupervisor,
    retry_backoff_seconds,
)
from openctopus_server.mcp.transport import (
    McpMessageTooLargeError,
    McpTransportFailureSignal,
)

_USER_ID = UUID("01890f7c-bb80-7000-8000-000000000041")
_ENTRY_ID = UUID("01890f7c-bb80-7000-8000-000000000042")


def test_retry_backoff_is_safe_for_long_lived_failure_counts() -> None:
    assert retry_backoff_seconds(10_000, jitter=0.5) == 60


async def test_process_reserved_name_capacity_accepts_sixteen_and_rejects_seventeen() -> (
    None
):
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(),
        discoverer=_discover,
    )
    names = tuple(f"server_{index:02d}" for index in range(PROCESS_RESERVED_NAME_MAX + 1))
    configs = tuple(_config(name) for name in names)

    await supervisor.preflight(
        configs=configs[:PROCESS_RESERVED_NAME_MAX],
        changed_names=names[:PROCESS_RESERVED_NAME_MAX],
    )
    with pytest.raises(ConfigError, match="capacity is exhausted"):
        await supervisor.preflight(configs=configs, changed_names=names)
    await supervisor.shutdown()


class FakeClock(AdmissionClock):
    def __init__(self) -> None:
        self.current = 0.0
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def now(self) -> float:
        return self.current

    async def sleep_until(self, deadline: float) -> None:
        if deadline <= self.current:
            return
        future = asyncio.get_running_loop().create_future()
        item = (deadline, future)
        self._sleepers.append(item)
        try:
            await future
        finally:
            if item in self._sleepers:
                self._sleepers.remove(item)

    def advance(self, seconds: float) -> None:
        self.current += seconds
        for deadline, future in tuple(self._sleepers):
            if deadline <= self.current and not future.done():
                future.set_result(None)


class FakeSession:
    def __init__(self, result: asyncio.Future[types.CallToolResult] | None = None) -> None:
        self.result = result
        self.started = asyncio.Event()
        self.error: BaseException | None = None
        self.call_count = 0

    async def send_request(
        self,
        request: types.ClientRequest,
        result_type: type[types.CallToolResult],
    ) -> types.CallToolResult:
        del request, result_type
        self.call_count += 1
        self.started.set()
        if self.error is not None:
            raise self.error
        if self.result is not None:
            return await self.result
        return types.CallToolResult(content=[types.TextContent(type="text", text="found")])


class FakeClient:
    def __init__(
        self,
        session: FakeSession | None = None,
        *,
        enter: Callable[[], Awaitable[None]] | None = None,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.session = session or FakeSession()
        self.transport = object()
        self._enter = enter
        self._close = close
        self.closed = False

    async def __aenter__(self) -> object:
        if self._enter is not None:
            await self._enter()
        return self

    async def close(self) -> None:
        self.closed = True
        if self._close is not None:
            await self._close()


def _config(
    name: str,
    *,
    transport: str = "streamable_http",
    max_concurrent_calls: int | None = None,
):
    if transport == "stdio":
        payload: dict[str, object] = {
            "name": name,
            "transport": "stdio",
            "command": "fake-mcp",
            "enabled_capabilities": [],
        }
    else:
        payload = {
            "name": name,
            "transport": transport,
            "url": f"https://{name}.example/mcp",
            "enabled_capabilities": [],
        }
    if max_concurrent_calls is not None:
        payload["max_concurrent_calls"] = max_concurrent_calls
    return parse_server_mcp_configs([payload])[0]


def _source(name: str) -> SourceMcpServerCatalog:
    from openctopus_server.devices.mcp_models import SourceMcpTool

    return SourceMcpServerCatalog(
        name=name,
        tools=[
            SourceMcpTool(
                raw_name="search",
                description="Search",
                input_schema={
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                    "additionalProperties": False,
                },
                output_schema=None,
            )
        ],
        resources=[],
        resource_templates=[],
        prompts=[],
    )


async def _discover(name: str, _session: object) -> SourceMcpServerCatalog:
    return _source(name)


def _envelope(configs, sources, *, revision: int = 2) -> ServerMcpEnvelope:
    next_id = iter(UUID(int=0x01890F7CBB8070008000000000001000 + index) for index in range(32))
    catalog = build_server_persisted_catalog(
        configs,
        SourceMcpCatalog(version=1, servers=list(sources)),
        entry_id_factory=lambda: next(next_id),
    )
    return ServerMcpEnvelope(
        version=1,
        config_revision=revision,
        mcp_servers=list(configs),
        mcp_catalog=catalog,
    )


def _route(
    supervisor: ServerMcpSupervisor,
    envelope: ServerMcpEnvelope,
    *,
    server: str = "search",
) -> FrozenServerMcpEntryRoute:
    persisted = next(item for item in envelope.mcp_catalog.servers if item.name == server)
    entry = persisted.entries[0]
    return FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.ready_generations(envelope)[server],
        server=server,
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )


async def test_candidate_validation_is_isolated_and_parallelism_is_four() -> None:
    gate = asyncio.Event()
    four_started = asyncio.Event()
    active = 0
    peak = 0

    async def enter() -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 4:
            four_started.set()
        await gate.wait()
        active -= 1

    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(enter=enter),
        discoverer=_discover,
    )
    configs = tuple(_config(f"s{index}") for index in range(5))
    task = asyncio.create_task(
        supervisor.validate(
            configs=configs,
            changed_names=tuple(config.name for config in configs),
            validate_servers=tuple(config.name for config in configs),
        )
    )

    await four_started.wait()
    await asyncio.sleep(0)
    assert peak == 4
    assert supervisor.runtime_generations() == {}

    gate.set()
    candidate = await task
    assert candidate.source_catalog.servers == [_source(config.name) for config in configs]
    assert supervisor.runtime_generations() == {}
    await supervisor.discard(candidate)
    await supervisor.shutdown()


async def test_shutdown_owns_candidate_while_cancelled_open_cleanup_is_blocked() -> None:
    entered = asyncio.Event()
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def blocked_enter() -> None:
        entered.set()
        await asyncio.Event().wait()

    async def blocked_close() -> None:
        close_started.set()
        await release_close.wait()

    client = FakeClient(enter=blocked_enter, close=blocked_close)
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    validation = asyncio.create_task(
        supervisor.validate(
            configs=(_config("search"),),
            changed_names=("search",),
            validate_servers=("search",),
        )
    )
    await entered.wait()

    validation.cancel()
    await close_started.wait()
    shutdown = asyncio.create_task(supervisor.shutdown())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert shutdown.done() is False

    release_close.set()
    with pytest.raises(asyncio.CancelledError):
        await validation
    await shutdown
    assert client.closed is True


async def test_shutdown_owns_successful_candidate_until_publish_or_discard() -> None:
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def blocked_close() -> None:
        close_started.set()
        await release_close.wait()

    client = FakeClient(close=blocked_close)
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    candidate = await supervisor.validate(
        configs=(_config("search"),),
        changed_names=("search",),
        validate_servers=("search",),
    )

    shutdown = asyncio.create_task(supervisor.shutdown())
    await close_started.wait()
    assert shutdown.done() is False
    release_close.set()
    await shutdown

    envelope = _envelope((_config("search"),), candidate.source_catalog.servers)
    with pytest.raises(ConfigError, match="shutting down"):
        await supervisor.publish(candidate, envelope)


async def test_start_returns_while_optional_endpoint_is_still_connecting() -> None:
    entered = asyncio.Event()
    gate = asyncio.Event()

    async def blocked_enter() -> None:
        entered.set()
        await gate.wait()

    config = _config("search")
    envelope = _envelope((config,), (_source("search"),))
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(enter=blocked_enter),
        discoverer=_discover,
    )

    await supervisor.start(envelope)
    await entered.wait()
    status = supervisor.runtime_snapshot(envelope)["search"].active
    assert status is not None and status.state == "starting"
    assert status.config_revision == envelope.config_revision
    assert status.catalog_digest == envelope.mcp_catalog.digest
    assert supervisor.ready_generations(envelope) == {"search": None}

    gate.set()
    for _ in range(32):
        if supervisor.ready_generations(envelope)["search"] is not None:
            break
        await asyncio.sleep(0)
    assert supervisor.ready_generations(envelope)["search"] is not None
    await supervisor.shutdown()


async def test_permanent_start_failure_stays_degraded_without_retry() -> None:
    attempted = asyncio.Event()

    async def missing_executable() -> None:
        attempted.set()
        raise FileNotFoundError("/secret/admin/path/search-mcp")

    config = _config("search", transport="stdio")
    envelope = _envelope((config,), (_source("search"),))
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(enter=missing_executable),
        discoverer=_discover,
    )

    await supervisor.start(envelope)
    await attempted.wait()
    for _ in range(8):
        status = supervisor.runtime_snapshot(envelope)["search"].active
        if status is not None and status.state == "unavailable":
            break
        await asyncio.sleep(0)
    assert status is not None and status.state == "unavailable"
    assert status.last_error is not None
    assert status.last_error.code == "mcp_spawn_failed"
    assert "/secret/" not in status.last_error.message
    assert status.restart_attempt == 0
    await supervisor.shutdown()


async def test_transient_start_failure_uses_fake_clock_backoff_then_recovers() -> None:
    clock = FakeClock()
    attempted = asyncio.Event()
    first = True

    async def flaky_enter() -> None:
        nonlocal first
        if first:
            first = False
            attempted.set()
            raise OSError("temporary network loss")

    config = _config("search")
    envelope = _envelope((config,), (_source("search"),))
    supervisor = ServerMcpSupervisor(
        clock=clock,
        jitter=lambda: 0.5,
        client_factory=lambda _config, **_kwargs: FakeClient(enter=flaky_enter),
        discoverer=_discover,
    )

    await supervisor.start(envelope)
    await attempted.wait()
    for _ in range(32):
        status = supervisor.runtime_snapshot(envelope)["search"].active
        if status is not None and status.state == "backoff":
            break
        await asyncio.sleep(0)
    assert status is not None and status.state == "backoff"
    assert status.restart_attempt == 1

    clock.advance(1.0)
    for _ in range(32):
        if supervisor.ready_generations(envelope)["search"] is not None:
            break
        await asyncio.sleep(0)
    assert supervisor.ready_generations(envelope)["search"] is not None
    recovered = supervisor.runtime_snapshot(envelope)["search"].active
    assert recovered is not None
    assert recovered.restart_attempt == 0
    await supervisor.shutdown()


async def test_successful_recovery_resets_the_next_backoff_sequence() -> None:
    clock = FakeClock()
    attempts = 0
    handlers: list[ServerMcpMessageHandler] = []

    async def flaky_enter() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("temporary network loss")

    def factory(_config, **kwargs):
        handler = kwargs["message_handler"]
        assert isinstance(handler, ServerMcpMessageHandler)
        handlers.append(handler)
        return FakeClient(enter=flaky_enter)

    config = _config("search")
    envelope = _envelope((config,), (_source("search"),))
    supervisor = ServerMcpSupervisor(
        clock=clock,
        jitter=lambda: 0.5,
        client_factory=factory,
        discoverer=_discover,
    )

    await supervisor.start(envelope)
    for _ in range(32):
        status = supervisor.runtime_snapshot(envelope)["search"].active
        if status is not None and status.state == "backoff":
            break
        await asyncio.sleep(0)
    clock.advance(1)
    for _ in range(32):
        if supervisor.ready_generations(envelope)["search"] is not None:
            break
        await asyncio.sleep(0)

    await handlers[1].on_exception(OSError("later network loss"))
    for _ in range(32):
        status = supervisor.runtime_snapshot(envelope)["search"].active
        if status is not None and status.state == "backoff":
            break
        await asyncio.sleep(0)
    for _ in range(8):
        if any(deadline == clock.current + 1 for deadline, _future in clock._sleepers):
            break
        await asyncio.sleep(0)
    clock.advance(1)
    for _ in range(16):
        if attempts == 3:
            break
        await asyncio.sleep(0)

    assert attempts == 3
    await supervisor.shutdown()


async def test_publish_promotes_candidate_and_dispatch_checks_frozen_generation() -> None:
    client = FakeClient()
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)

    await supervisor.publish(candidate, envelope)
    generation = supervisor.runtime_generations()["search"]
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=generation,
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )

    result = await supervisor.dispatch_server_mcp(
        route=route,
        user_id=_USER_ID,
        name=entry.final_name,
        args={"query": "octopus"},
    )
    assert result.is_error is False
    assert result.content[-1]["text"] == "found"

    fenced = await supervisor.dispatch_server_mcp(
        route=route,
        user_id=_USER_ID,
        name=entry.final_name,
        args={"query": "octopus"},
        issue_guard=lambda: False,
    )
    assert fenced.code is not None
    assert fenced.code.value == "tool_mcp_unavailable"
    assert client.session.call_count == 1

    stale = replace(route, runtime_generation=UUID(int=1))
    unavailable = await supervisor.dispatch_server_mcp(
        route=stale,
        user_id=_USER_ID,
        name=entry.final_name,
        args={"query": "octopus"},
    )
    assert unavailable.code is not None
    assert unavailable.code.value == "tool_mcp_unavailable"
    await supervisor.shutdown()


async def test_begin_shutdown_rejects_new_admission_before_transport_cleanup() -> None:
    client = FakeClient()
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.runtime_generations()["search"],
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )

    await supervisor.begin_shutdown()
    await supervisor.begin_shutdown()
    assert client.closed is False
    rejected = await supervisor.dispatch_server_mcp(
        route=route,
        user_id=_USER_ID,
        name=entry.final_name,
        args={"query": "octopus"},
    )
    assert rejected.code is not None
    assert rejected.code.value == "tool_mcp_unavailable"

    await supervisor.shutdown()
    await supervisor.shutdown()
    assert client.closed is True


async def test_shutdown_releases_lease_retained_by_late_remote_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result_future: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    release_close = asyncio.Event()

    async def blocked_close() -> None:
        await release_close.wait()

    session = FakeSession(result_future)
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(
            session,
            close=blocked_close,
        ),
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    runtime = supervisor._slots["search"].active
    assert runtime is not None
    runtime._cleanup_timeout = 0.01
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.runtime_generations()["search"],
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )
    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=entry.final_name,
            args={"query": "octopus"},
        )
    )
    await session.started.wait()
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call

    wait_background_entered = asyncio.Event()
    release_wait_background = asyncio.Event()
    original_wait_background = supervisor.wait_background

    async def gated_wait_background() -> None:
        wait_background_entered.set()
        await release_wait_background.wait()
        await original_wait_background()

    monkeypatch.setattr(supervisor, "wait_background", gated_wait_background)
    shutdown = asyncio.create_task(supervisor.shutdown())
    await wait_background_entered.wait()
    result_future.set_result(
        types.CallToolResult(content=[types.TextContent(type="text", text="late")])
    )
    release_wait_background.set()
    await shutdown

    assert supervisor.coordinator_snapshot().reserved == 0
    assert supervisor._retained_leases == {}


async def test_replacement_snapshot_keeps_new_active_and_old_draining_separate() -> None:
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def delayed_close() -> None:
        close_started.set()
        await release_close.wait()

    old_client = FakeClient()
    old_client.close = delayed_close  # type: ignore[method-assign]
    new_client = FakeClient()
    clients = iter((old_client, new_client))
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: next(clients),
        discoverer=_discover,
    )
    first_config = _config("search")
    first = await supervisor.validate(
        configs=(first_config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    first_envelope = _envelope((first_config,), first.source_catalog.servers, revision=2)
    await supervisor.publish(first, first_envelope)
    first_generation = supervisor.runtime_generations()["search"]

    second_payload = first_config.storage_dict()
    second_payload["max_concurrent_calls"] = 9
    second_config = parse_server_mcp_configs([second_payload])[0]
    second = await supervisor.validate(
        configs=(second_config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    second_envelope = _envelope((second_config,), second.source_catalog.servers, revision=3)
    await supervisor.publish(second, second_envelope)
    await close_started.wait()

    snapshot = supervisor.runtime_snapshot(second_envelope)["search"]
    assert snapshot.active is not None
    assert snapshot.active.state == "ready"
    assert snapshot.active.config_revision == 3
    assert snapshot.draining is not None
    assert snapshot.draining.state == "draining"
    assert snapshot.draining.config_revision == 2
    assert snapshot.draining.runtime_generation == first_generation

    release_close.set()
    await supervisor.wait_background()
    assert supervisor.runtime_snapshot(second_envelope)["search"].draining is None
    await supervisor.shutdown()


async def test_replacement_name_credits_include_old_and_recover_after_cleanup() -> None:
    close_started_count = 0
    all_closes_started = asyncio.Event()
    release_close = asyncio.Event()

    async def delayed_close() -> None:
        nonlocal close_started_count
        close_started_count += 1
        if close_started_count == PROCESS_RESERVED_NAME_MAX:
            all_closes_started.set()
        await release_close.wait()

    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(close=delayed_close),
        discoverer=_discover,
    )
    old_names = tuple(f"old_{index:02d}" for index in range(PROCESS_RESERVED_NAME_MAX))
    new_names = tuple(f"new_{index:02d}" for index in range(PROCESS_RESERVED_NAME_MAX))
    old_configs = tuple(_config(name) for name in old_names)
    new_configs = tuple(_config(name) for name in new_names)
    try:
        current_candidate = await supervisor.validate(
            configs=old_configs,
            changed_names=old_names,
            validate_servers=old_names,
        )
        current = _envelope(
            old_configs,
            tuple(_source(name) for name in old_names),
        )
        await supervisor.publish(current_candidate, current)

        with pytest.raises(ConfigError, match="capacity is exhausted"):
            await supervisor.preflight(
                configs=new_configs,
                changed_names=old_names + new_names,
            )

        deleting = await supervisor.validate(
            configs=(),
            changed_names=old_names,
            validate_servers=(),
        )
        empty = _envelope((), (), revision=3)
        await supervisor.publish(deleting, empty)
        await all_closes_started.wait()

        with pytest.raises(ConfigError, match="capacity is exhausted"):
            await supervisor.preflight(
                configs=(new_configs[0],),
                changed_names=(new_names[0],),
            )

        release_close.set()
        await supervisor.wait_background()
        await supervisor.preflight(
            configs=(new_configs[0],),
            changed_names=(new_names[0],),
        )
        assert supervisor.snapshot() == {}
    finally:
        release_close.set()
        await supervisor.shutdown()


async def test_same_catalog_refresh_replaces_runtime_without_revision_bump() -> None:
    clients = iter((FakeClient(), FakeClient()))
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: next(clients),
        discoverer=_discover,
    )
    config = _config("search")
    first = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), first.source_catalog.servers, revision=7)
    await supervisor.publish(first, envelope)
    first_generation = supervisor.ready_generations(envelope)["search"]
    assert supervisor.refresh_names(envelope) == ()

    refresh = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    # This is the final pre-commit check used by the no-DB-write refresh path.
    await supervisor.preflight(configs=(config,), changed_names=("search",))
    await supervisor.publish(refresh, envelope)
    second_generation = supervisor.ready_generations(envelope)["search"]

    assert first_generation is not None
    assert second_generation is not None
    assert second_generation != first_generation
    active = supervisor.runtime_snapshot(envelope)["search"].active
    assert active is not None and active.config_revision == 7
    await supervisor.wait_background()
    await supervisor.shutdown()


async def test_list_changed_rediscovery_marks_drift_without_accepting_schema() -> None:
    handler: ServerMcpMessageHandler | None = None
    discoveries = 0

    def factory(_config, **kwargs):
        nonlocal handler
        value = kwargs["message_handler"]
        assert isinstance(value, ServerMcpMessageHandler)
        handler = value
        return FakeClient()

    async def changing_discovery(name: str, _session: object) -> SourceMcpServerCatalog:
        nonlocal discoveries
        discoveries += 1
        source = _source(name)
        if discoveries > 1:
            source.tools[0].description = "changed schema"
        return source

    supervisor = ServerMcpSupervisor(
        client_factory=factory,
        discoverer=changing_discovery,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    assert handler is not None

    await handler.on_tool_list_changed(types.ToolListChangedNotification())
    for _ in range(32):
        active = supervisor.runtime_snapshot(envelope)["search"].active
        if active is not None and active.state == "drifted":
            break
        await asyncio.sleep(0)
    assert active is not None and active.state == "drifted"
    assert supervisor.ready_generations(envelope) == {"search": None}
    assert supervisor.refresh_names(envelope) == ("search",)
    assert "changed schema" not in (envelope.mcp_catalog.servers[0].entries[0].provider_description)
    await supervisor.shutdown()


async def test_list_changed_rediscovery_keeps_newer_global_authority() -> None:
    handler: ServerMcpMessageHandler | None = None
    rediscovery_started = asyncio.Event()
    release_rediscovery = asyncio.Event()
    discoveries = 0

    def factory(_config, **kwargs):
        nonlocal handler
        value = kwargs["message_handler"]
        assert isinstance(value, ServerMcpMessageHandler)
        handler = value
        return FakeClient()

    async def blocked_discovery(name: str, _session: object) -> SourceMcpServerCatalog:
        nonlocal discoveries
        discoveries += 1
        if discoveries > 1:
            rediscovery_started.set()
            await release_rediscovery.wait()
        return _source(name)

    supervisor = ServerMcpSupervisor(
        client_factory=factory,
        discoverer=blocked_discovery,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers, revision=2)
    await supervisor.publish(candidate, envelope)
    generation = supervisor.ready_generations(envelope)["search"]
    assert handler is not None

    await handler.on_tool_list_changed(types.ToolListChangedNotification())
    await rediscovery_started.wait()
    newer = envelope.model_copy(update={"config_revision": 3})
    await supervisor.reconcile(newer)
    release_rediscovery.set()
    for _ in range(12):
        if supervisor.ready_generations(newer)["search"] is not None:
            break
        await asyncio.sleep(0)

    assert supervisor.ready_generations(newer)["search"] == generation
    assert supervisor.refresh_names(newer) == ()
    await supervisor.shutdown()


async def test_failed_candidate_cleanup_is_visible_and_blocks_same_name() -> None:
    current_client = FakeClient()
    candidate_client = FakeClient()

    async def fail_close() -> None:
        candidate_client.closed = True
        raise RuntimeError("secret third-party close failure")

    candidate_client.close = fail_close  # type: ignore[method-assign]
    clients = iter((current_client, candidate_client))
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: next(clients),
        discoverer=_discover,
    )
    current_config = _config("search")
    current = await supervisor.validate(
        configs=(current_config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((current_config,), current.source_catalog.servers)
    await supervisor.publish(current, envelope)

    changed_payload = current_config.storage_dict()
    changed_payload["max_concurrent_calls"] = 9
    changed_config = parse_server_mcp_configs([changed_payload])[0]
    candidate = await supervisor.validate(
        configs=(changed_config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    await supervisor.discard(candidate)

    snapshot = supervisor.runtime_snapshot(envelope)["search"]
    assert snapshot.active is not None and snapshot.active.state == "ready"
    assert snapshot.draining is not None
    assert snapshot.draining.state == "cleanup_blocked"
    assert snapshot.draining.origin == "candidate"
    assert snapshot.draining.config_revision is None
    assert snapshot.draining.catalog_digest is None
    assert snapshot.draining.last_error is not None
    assert snapshot.draining.last_error.code == "mcp_cleanup_incomplete"
    assert "secret" not in snapshot.draining.last_error.message
    with pytest.raises(ConfigError):
        await supervisor.preflight(configs=(changed_config,), changed_names=("search",))
    await supervisor.shutdown()


async def test_cleanup_retry_releases_candidate_name_credit() -> None:
    clock = FakeClock()
    current_client = FakeClient()
    candidate_client = FakeClient()
    close_attempts = 0

    async def flaky_close() -> None:
        nonlocal close_attempts
        close_attempts += 1
        if close_attempts == 1:
            raise RuntimeError("first close failed")
        candidate_client.closed = True

    candidate_client.close = flaky_close  # type: ignore[method-assign]
    clients = iter((current_client, candidate_client))
    supervisor = ServerMcpSupervisor(
        clock=clock,
        jitter=lambda: 0.5,
        client_factory=lambda _config, **_kwargs: next(clients),
        discoverer=_discover,
    )
    current_config = _config("search")
    current = await supervisor.validate(
        configs=(current_config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((current_config,), current.source_catalog.servers)
    await supervisor.publish(current, envelope)

    changed_payload = current_config.storage_dict()
    changed_payload["max_concurrent_calls"] = 9
    changed_config = parse_server_mcp_configs([changed_payload])[0]
    candidate = await supervisor.validate(
        configs=(changed_config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    await supervisor.discard(candidate)
    assert supervisor.runtime_snapshot(envelope)["search"].draining is not None

    await asyncio.sleep(0)
    clock.advance(1.0)
    for _ in range(12):
        if supervisor.runtime_snapshot(envelope)["search"].draining is None:
            break
        await asyncio.sleep(0)
    assert close_attempts == 2
    assert supervisor.runtime_snapshot(envelope)["search"].draining is None
    await supervisor.preflight(configs=(changed_config,), changed_names=("search",))
    await supervisor.shutdown()


async def test_remote_public_timeout_keeps_permit_until_late_result_is_consumed() -> None:
    clock = FakeClock()
    result_future: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(result_future)
    supervisor = ServerMcpSupervisor(
        clock=clock,
        client_factory=lambda _config, **_kwargs: FakeClient(session),
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.runtime_generations()["search"],
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )

    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=entry.final_name,
            args={"query": "octopus"},
        )
    )
    await session.started.wait()
    clock.advance(60)
    timed_out = await call
    assert timed_out.code is not None
    assert timed_out.code.value == "tool_execution_outcome_unknown"
    assert supervisor.coordinator_snapshot().draining == 1

    result_future.set_result(
        types.CallToolResult(content=[types.TextContent(type="text", text="late")])
    )
    await supervisor.wait_background()
    assert session.call_count == 1
    assert supervisor.coordinator_snapshot().reserved == 0
    await supervisor.shutdown()


async def test_cancellation_at_issue_boundary_transfers_remote_call_to_drain() -> None:
    result_future: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(result_future)
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(session),
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.runtime_generations()["search"],
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )
    call: asyncio.Task[object] | None = None

    def cancel_at_issue() -> None:
        assert call is not None
        call.cancel()

    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=entry.final_name,
            args={"query": "octopus"},
            on_issued=cancel_at_issue,
        )
    )
    with pytest.raises(asyncio.CancelledError):
        await call
    assert supervisor.coordinator_snapshot().draining == 1

    result_future.set_result(
        types.CallToolResult(content=[types.TextContent(type="text", text="late")])
    )
    await supervisor.wait_background()
    assert supervisor.coordinator_snapshot().reserved == 0
    await supervisor.shutdown()


async def test_repeated_cancellation_cannot_interrupt_remote_drain_handoff() -> None:
    result_future: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(result_future)
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(session),
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.runtime_generations()["search"],
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )
    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=entry.final_name,
            args={"query": "octopus"},
        )
    )
    await session.started.wait()

    await supervisor._coordinator._lock.acquire()
    try:
        call.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        call.cancel()
        await asyncio.sleep(0)
    finally:
        supervisor._coordinator._lock.release()

    with pytest.raises(asyncio.CancelledError):
        await call
    assert supervisor.coordinator_snapshot().draining == 1
    result_future.set_result(
        types.CallToolResult(content=[types.TextContent(type="text", text="late")])
    )
    await supervisor.wait_background()
    assert session.call_count == 1
    assert supervisor.coordinator_snapshot().reserved == 0
    await supervisor.shutdown()


async def test_remote_hard_drain_expiry_retires_generation() -> None:
    clock = FakeClock()
    result_future: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(result_future)
    client = FakeClient(session)
    supervisor = ServerMcpSupervisor(
        clock=clock,
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.runtime_generations()["search"],
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )
    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=entry.final_name,
            args={"query": "octopus"},
        )
    )
    await session.started.wait()
    clock.advance(60)
    timed_out = await call
    assert timed_out.code is not None
    await asyncio.sleep(0)

    clock.advance(60)
    await supervisor.wait_background()
    assert client.closed is True
    assert supervisor.runtime_generations()["search"] is None
    assert supervisor.coordinator_snapshot().reserved == 0
    await supervisor.shutdown()


async def test_stdio_public_timeout_retires_generation_without_remote_drain() -> None:
    clock = FakeClock()
    result_future: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(result_future)
    client = FakeClient(session)
    supervisor = ServerMcpSupervisor(
        clock=clock,
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    config = _config("search", transport="stdio")
    candidate = await supervisor.validate(
        configs=(config,), changed_names=("search",), validate_servers=("search",)
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    entry = envelope.mcp_catalog.servers[0].entries[0]
    route = FrozenServerMcpEntryRoute(
        entry_id=entry.entry_id,
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        runtime_generation=supervisor.runtime_generations()["search"],
        server="search",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=entry.final_name,
    )

    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=entry.final_name,
            args={"query": "octopus"},
        )
    )
    await session.started.wait()
    clock.advance(60)
    timed_out = await call

    assert timed_out.code is not None
    assert timed_out.code.value == "tool_execution_outcome_unknown"
    assert client.closed is True
    assert supervisor.runtime_generations()["search"] is None
    assert supervisor.coordinator_snapshot().reserved == 0
    await supervisor.shutdown()


async def test_pure_delete_candidate_claims_name_without_connecting_and_removes_empty_slot() -> (
    None
):
    attempted = asyncio.Event()

    async def missing_executable() -> None:
        attempted.set()
        raise FileNotFoundError("missing-mcp")

    config = _config("search", transport="stdio")
    current = _envelope((config,), (_source("search"),), revision=2)
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(enter=missing_executable),
        discoverer=_discover,
    )
    await supervisor.start(current)
    await attempted.wait()
    for _ in range(8):
        status = supervisor.runtime_snapshot(current)["search"].active
        if (
            status is not None
            and status.state == "unavailable"
            and status.runtime_generation is None
        ):
            break
        await asyncio.sleep(0)

    candidate = await supervisor.validate(
        configs=(),
        changed_names=("search",),
        validate_servers=(),
    )
    assert candidate.runtimes == {}
    assert candidate.source_catalog.servers == []
    empty = _envelope((), (), revision=3)
    await supervisor.publish(candidate, empty)

    assert supervisor.snapshot() == {}
    assert supervisor._slots == {}  # noqa: SLF001 - verifies bounded slot ownership
    await supervisor.shutdown()


async def test_discarded_pure_delete_resumes_recovery_blocked_by_its_lease() -> None:
    entered = asyncio.Event()

    async def enter() -> None:
        entered.set()

    config = _config("search")
    envelope = _envelope((config,), (_source("search"),))
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(enter=enter),
        discoverer=_discover,
    )
    await supervisor.start(envelope)
    candidate = await supervisor.validate(
        configs=(),
        changed_names=("search",),
        validate_servers=(),
    )
    await asyncio.sleep(0)
    assert entered.is_set() is False

    await supervisor.discard(candidate)
    await entered.wait()
    for _ in range(32):
        if supervisor.ready_generations(envelope)["search"] is not None:
            break
        await asyncio.sleep(0)
    assert supervisor.ready_generations(envelope)["search"] is not None
    await supervisor.shutdown()


async def test_candidate_lease_defers_active_failure_until_publish_or_discard() -> None:
    handlers: list[ServerMcpMessageHandler] = []
    candidate_close_started = asyncio.Event()
    release_candidate_close = asyncio.Event()

    async def blocked_candidate_close() -> None:
        candidate_close_started.set()
        await release_candidate_close.wait()

    current_client = FakeClient()
    candidate_client = FakeClient(close=blocked_candidate_close)
    recovery_client = FakeClient()
    clients = iter((current_client, candidate_client, recovery_client))

    def factory(_config, **kwargs):
        handler = kwargs["message_handler"]
        assert isinstance(handler, ServerMcpMessageHandler)
        handlers.append(handler)
        return next(clients)

    supervisor = ServerMcpSupervisor(client_factory=factory, discoverer=_discover)
    config = _config("search")
    current = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), current.source_catalog.servers)
    await supervisor.publish(current, envelope)

    payload = config.storage_dict()
    payload["max_concurrent_calls"] = 9
    changed = parse_server_mcp_configs([payload])[0]
    candidate = await supervisor.validate(
        configs=(changed,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    await handlers[0].on_exception(OSError("connection lost"))
    for _ in range(12):
        active = supervisor.runtime_snapshot(envelope)["search"].active
        if active is not None and active.state == "unavailable":
            break
        await asyncio.sleep(0)

    snapshot = supervisor.runtime_snapshot(envelope)["search"]
    assert snapshot.active is not None and snapshot.active.state == "unavailable"
    assert snapshot.draining is None
    assert current_client.closed is False

    discard = asyncio.create_task(supervisor.discard(candidate))
    await candidate_close_started.wait()
    snapshot = supervisor.runtime_snapshot(envelope)["search"]
    assert snapshot.active is not None and snapshot.active.state == "unavailable"
    assert snapshot.draining is not None
    assert snapshot.draining.origin == "candidate"

    release_candidate_close.set()
    await discard
    for _ in range(16):
        if current_client.closed:
            break
        await asyncio.sleep(0)
    assert current_client.closed is True
    await supervisor.shutdown()


async def test_republishing_consumed_candidate_does_not_release_newer_lease() -> None:
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(),
        discoverer=_discover,
    )
    config = _config("search")
    first = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), first.source_catalog.servers)
    await supervisor.publish(first, envelope)

    second = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    await supervisor.publish(first, envelope)
    with pytest.raises(ConfigError):
        await supervisor.validate(
            configs=(config,),
            changed_names=("search",),
            validate_servers=("search",),
        )

    await supervisor.discard(second)
    await supervisor.shutdown()


async def test_queued_call_rechecks_frozen_route_at_actual_issue_boundary() -> None:
    handler: ServerMcpMessageHandler | None = None
    rediscovery_started = asyncio.Event()
    release_rediscovery = asyncio.Event()
    discoveries = 0
    first_result: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(first_result)

    def factory(_config, **kwargs):
        nonlocal handler
        value = kwargs["message_handler"]
        assert isinstance(value, ServerMcpMessageHandler)
        handler = value
        return FakeClient(session)

    async def discover(name: str, _session: object) -> SourceMcpServerCatalog:
        nonlocal discoveries
        discoveries += 1
        if discoveries > 1:
            rediscovery_started.set()
            await release_rediscovery.wait()
        return _source(name)

    supervisor = ServerMcpSupervisor(client_factory=factory, discoverer=discover)
    config = _config("search", max_concurrent_calls=1)
    candidate = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    route = _route(supervisor, envelope)

    first = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=route.final_name,
            args={"query": "first"},
        )
    )
    await session.started.wait()
    second = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=UUID(int=_USER_ID.int + 1),
            name=route.final_name,
            args={"query": "second"},
        )
    )
    for _ in range(8):
        active = supervisor.runtime_snapshot(envelope)["search"].active
        if active is not None and active.waiting_calls == 1:
            break
        await asyncio.sleep(0)
    assert handler is not None
    await handler.on_tool_list_changed(types.ToolListChangedNotification())
    await rediscovery_started.wait()

    first_result.set_result(
        types.CallToolResult(content=[types.TextContent(type="text", text="first")])
    )
    assert (await first).is_error is False
    rejected = await second
    assert rejected.code is not None
    assert rejected.code.value == "tool_mcp_unavailable"
    assert session.call_count == 1

    release_rediscovery.set()
    await supervisor.shutdown()


async def test_cleanup_blocked_keeps_remote_permit_until_retry_converges() -> None:
    clock = FakeClock()
    result: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(result)
    close_attempts = 0

    async def flaky_close() -> None:
        nonlocal close_attempts
        close_attempts += 1
        if close_attempts == 1:
            raise RuntimeError("close failed")

    client = FakeClient(session, close=flaky_close)
    supervisor = ServerMcpSupervisor(
        clock=clock,
        jitter=lambda: 0.5,
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    route = _route(supervisor, envelope)

    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=route.final_name,
            args={"query": "octopus"},
        )
    )
    await session.started.wait()
    clock.advance(60)
    assert (await call).code is not None
    await asyncio.sleep(0)
    clock.advance(60)
    for _ in range(16):
        snapshot = supervisor.runtime_snapshot(envelope)["search"]
        if snapshot.draining is not None and snapshot.draining.state == "cleanup_blocked":
            break
        await asyncio.sleep(0)

    assert snapshot.draining is not None
    assert snapshot.draining.state == "cleanup_blocked"
    assert supervisor.coordinator_snapshot().reserved == 1
    await asyncio.wait_for(supervisor.wait_background(), timeout=0.1)

    for _ in range(8):
        if any(deadline == 121 for deadline, _future in clock._sleepers):
            break
        await asyncio.sleep(0)
    clock.advance(1)
    for _ in range(16):
        if supervisor.coordinator_snapshot().reserved == 0:
            break
        await asyncio.sleep(0)
    assert close_attempts == 2
    assert supervisor.coordinator_snapshot().reserved == 0
    await supervisor.shutdown()


async def test_reconcile_does_not_duplicate_a_backoff_recovery() -> None:
    clock = FakeClock()
    attempts = 0
    first_failed = asyncio.Event()

    async def enter() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            first_failed.set()
            raise OSError("temporary")

    config = _config("search")
    envelope = _envelope((config,), (_source("search"),))
    supervisor = ServerMcpSupervisor(
        clock=clock,
        jitter=lambda: 0.5,
        client_factory=lambda _config, **_kwargs: FakeClient(enter=enter),
        discoverer=_discover,
    )
    await supervisor.start(envelope)
    await first_failed.wait()
    for _ in range(8):
        status = supervisor.runtime_snapshot(envelope)["search"].active
        if status is not None and status.state == "backoff":
            break
        await asyncio.sleep(0)

    await supervisor.reconcile(envelope)
    await asyncio.sleep(0)
    assert attempts == 1
    clock.advance(1)
    for _ in range(16):
        if supervisor.ready_generations(envelope)["search"] is not None:
            break
        await asyncio.sleep(0)
    assert attempts == 2
    await supervisor.shutdown()


async def test_reconcile_retires_mismatched_sink_and_recovers_durable_config() -> None:
    clients = [FakeClient(), FakeClient()]
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: clients.pop(0),
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    first_envelope = _envelope((config,), candidate.source_catalog.servers, revision=2)
    await supervisor.publish(candidate, first_envelope)
    first_generation = supervisor.ready_generations(first_envelope)["search"]

    payload = config.storage_dict()
    payload["max_concurrent_calls"] = 9
    changed = parse_server_mcp_configs([payload])[0]
    second_envelope = _envelope((changed,), (_source("search"),), revision=3)
    await supervisor.reconcile(second_envelope)
    for _ in range(32):
        generation = supervisor.ready_generations(second_envelope)["search"]
        if generation is not None:
            break
        await asyncio.sleep(0)

    assert generation is not None and generation != first_generation
    active = supervisor.runtime_snapshot(second_envelope)["search"].active
    assert active is not None and active.max_concurrent_calls == 9
    await supervisor.shutdown()


async def test_publish_activation_failure_isolated_and_recovered_per_name() -> None:
    clock = FakeClock()
    clients: dict[str, list[FakeClient]] = {"bad": [], "good": []}

    def factory(config, **_kwargs):
        client = FakeClient()
        clients[config.name].append(client)
        return client

    supervisor = ServerMcpSupervisor(
        clock=clock,
        jitter=lambda: 0.5,
        client_factory=factory,
        discoverer=_discover,
    )
    configs = (_config("bad"), _config("good"))
    candidate = await supervisor.validate(
        configs=configs,
        changed_names=("bad", "good"),
        validate_servers=("bad", "good"),
    )

    def fail_bind(*_args, **_kwargs):
        raise RuntimeError("activation failed")

    candidate.runtimes["bad"].bind_authority = fail_bind  # type: ignore[method-assign]
    envelope = _envelope(configs, candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    assert supervisor.ready_generations(envelope)["good"] is not None
    for _ in range(8):
        active = supervisor.runtime_snapshot(envelope)["bad"].active
        if active is not None and active.state == "backoff":
            break
        await asyncio.sleep(0)
    await asyncio.sleep(0)
    clock.advance(1)
    for _ in range(16):
        if supervisor.ready_generations(envelope)["bad"] is not None:
            break
        await asyncio.sleep(0)

    assert supervisor.ready_generations(envelope)["bad"] is not None
    assert clients["bad"][0].closed is True
    await supervisor.shutdown()


async def test_message_limit_returns_stable_code_and_suspends_generation() -> None:
    session = FakeSession()
    session.error = McpMessageTooLargeError("secret raw detail")
    client = FakeClient(session)
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    route = _route(supervisor, envelope)

    output = await supervisor.dispatch_server_mcp(
        route=route,
        user_id=_USER_ID,
        name=route.final_name,
        args={"query": "octopus"},
    )

    assert output.code is not None
    assert output.code.value == "tool_mcp_message_too_large"
    assert "secret" not in str(output.content)
    assert supervisor.ready_generations(envelope)["search"] is None
    await supervisor.shutdown()


async def test_connection_closed_returns_outcome_unknown_and_retires_generation() -> None:
    session = FakeSession()
    session.error = McpError(
        types.ErrorData(code=types.CONNECTION_CLOSED, message="Connection closed")
    )
    client = FakeClient(session)
    recovery_gate = asyncio.Event()

    async def block_recovery() -> None:
        await recovery_gate.wait()

    clients = iter((client, FakeClient(enter=block_recovery)))
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: next(clients),
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    route = _route(supervisor, envelope)

    output = await supervisor.dispatch_server_mcp(
        route=route,
        user_id=_USER_ID,
        name=route.final_name,
        args={"query": "octopus"},
    )

    assert output.code is not None
    assert output.code.value == "tool_execution_outcome_unknown"
    assert client.closed is True
    await supervisor.shutdown()


@pytest.mark.parametrize("after_public_timeout", [False, True])
async def test_protocol_violation_returns_outcome_unknown_and_stays_suspended(
    after_public_timeout: bool,
) -> None:
    clock = FakeClock()
    result_future: asyncio.Future[types.CallToolResult] = asyncio.get_running_loop().create_future()
    session = FakeSession(result_future)
    client = FakeClient(session)
    signals: list[McpTransportFailureSignal] = []
    factory_calls = 0

    def factory(_config, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal factory_calls
        factory_calls += 1
        signal = kwargs.get("transport_failure_signal")
        assert isinstance(signal, McpTransportFailureSignal)
        signals.append(signal)
        return client

    supervisor = ServerMcpSupervisor(
        clock=clock,
        client_factory=factory,
        discoverer=_discover,
    )
    config = _config("search")
    candidate = await supervisor.validate(
        configs=(config,),
        changed_names=("search",),
        validate_servers=("search",),
    )
    envelope = _envelope((config,), candidate.source_catalog.servers)
    await supervisor.publish(candidate, envelope)
    route = _route(supervisor, envelope)

    call = asyncio.create_task(
        supervisor.dispatch_server_mcp(
            route=route,
            user_id=_USER_ID,
            name=route.final_name,
            args={"query": "octopus"},
        )
    )
    await session.started.wait()
    if after_public_timeout:
        clock.advance(60)
        output = await call
        signals[0].report("unsupported_content_encoding")
    else:
        signals[0].report("unsupported_content_encoding")
        output = await call

    assert output.code is not None
    assert output.code.value == "tool_execution_outcome_unknown"
    for _ in range(32):
        status = supervisor.runtime_snapshot(envelope)["search"].active
        if status is not None and status.state == "unavailable":
            break
        await asyncio.sleep(0)
    assert status is not None and status.state == "unavailable"
    assert status.last_error is not None
    assert status.last_error.code == "config_validation_failed"
    assert status.restart_attempt == 0
    clock.advance(60)
    for _ in range(32):
        await asyncio.sleep(0)
    assert factory_calls == 1
    await supervisor.shutdown()


async def test_runtime_snapshot_is_immutable_and_sanitized() -> None:
    supervisor = ServerMcpSupervisor(
        client_factory=lambda _config, **_kwargs: FakeClient(),
        discoverer=_discover,
    )
    snapshot = supervisor.snapshot()
    assert isinstance(snapshot, MappingProxyType)
    with pytest.raises(TypeError):
        snapshot["x"] = object()  # type: ignore[index]
    await supervisor.shutdown()
