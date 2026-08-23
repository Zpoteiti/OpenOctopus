from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from mcp import types
from mcp.shared.exceptions import McpError
from pydantic import ValidationError

from openctopus_server.devices.mcp_models import SourceMcpServerCatalog, SourceMcpTool
from openctopus_server.mcp.catalog import build_server_persisted_catalog
from openctopus_server.mcp.models import parse_server_mcp_configs
from openctopus_server.mcp.runtime import (
    RuntimeGeneration,
    RuntimeMessageTooLargeError,
    RuntimeOpenError,
    RuntimeState,
    RuntimeTransportError,
    ServerMcpMessageHandler,
)
from openctopus_server.mcp.scheduler import ServerMcpCoordinator
from openctopus_server.mcp.transport import McpMessageTooLargeError

_ENTRY_ID = UUID("01890f7c-bb80-7000-8000-000000000031")


def _config(*, transport: str = "streamable_http"):
    if transport == "stdio":
        payload: dict[str, object] = {
            "name": "search",
            "transport": "stdio",
            "command": "search-mcp",
            "enabled_capabilities": [],
        }
    else:
        payload = {
            "name": "search",
            "transport": transport,
            "url": "https://mcp.example/mcp",
            "enabled_capabilities": [],
        }
    return parse_server_mcp_configs([payload])[0]


def _source(name: str = "search") -> SourceMcpServerCatalog:
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


class FakeSession:
    def __init__(self) -> None:
        self.call_started = asyncio.Event()
        self.call_result: asyncio.Future[types.CallToolResult] | None = None
        self.call_error: BaseException | None = None

    async def send_request(
        self,
        request: types.ClientRequest,
        result_type: type[types.CallToolResult],
    ) -> types.CallToolResult:
        del request, result_type
        self.call_started.set()
        if self.call_error is not None:
            raise self.call_error
        if self.call_result is None:
            return types.CallToolResult(content=[types.TextContent(type="text", text="found")])
        return await self.call_result


class FakeClient:
    def __init__(
        self,
        *,
        session: FakeSession | None = None,
        enter: Callable[[], Awaitable[None]] | None = None,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.session = session or FakeSession()
        self.transport = object()
        self._enter = enter
        self._close = close
        self.entered = False
        self.closed = False

    async def __aenter__(self) -> object:
        self.entered = True
        if self._enter is not None:
            await self._enter()
        return self

    async def close(self) -> None:
        self.closed = True
        if self._close is not None:
            await self._close()


class FakeTransport:
    def __init__(self, terminal_error: BaseException | None = None) -> None:
        self.terminal_error = terminal_error


async def _discover(
    name: str,
    _session: object,
) -> SourceMcpServerCatalog:
    return _source(name)


async def test_runtime_opens_once_binds_authority_and_invokes_tool() -> None:
    config = _config()
    client = FakeClient()
    runtime = RuntimeGeneration(
        config,
        coordinator=ServerMcpCoordinator(),
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )

    source = await runtime.open()
    assert source == _source()
    assert runtime.generation is not None
    assert runtime.state is RuntimeState.DISCOVERING

    from openctopus_server.devices.mcp_models import SourceMcpCatalog

    catalog = build_server_persisted_catalog(
        [config],
        SourceMcpCatalog(version=1, servers=[source]),
        entry_id_factory=lambda: _ENTRY_ID,
    )
    runtime.bind_authority(
        catalog.servers[0],
        config_revision=7,
        catalog_digest=catalog.digest,
    )

    assert runtime.state is RuntimeState.READY
    result = await runtime.invoke(_ENTRY_ID, {"query": "octopus"})
    assert result.is_error is False
    assert result.content[-1]["text"] == "found"

    await runtime.close()
    assert client.closed is True


async def _ready_runtime(
    session: FakeSession,
    *,
    terminal_error: BaseException | None = None,
) -> RuntimeGeneration:
    config = _config()
    client = FakeClient(session=session)
    client.transport = FakeTransport(terminal_error)
    runtime = RuntimeGeneration(
        config,
        coordinator=ServerMcpCoordinator(),
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    source = await runtime.open()

    from openctopus_server.devices.mcp_models import SourceMcpCatalog

    catalog = build_server_persisted_catalog(
        [config],
        SourceMcpCatalog(version=1, servers=[source]),
        entry_id_factory=lambda: _ENTRY_ID,
    )
    runtime.bind_authority(
        catalog.servers[0],
        config_revision=7,
        catalog_digest=catalog.digest,
    )
    return runtime


async def test_invalid_typed_result_does_not_retire_generation() -> None:
    session = FakeSession()
    with pytest.raises(ValidationError) as captured:
        types.CallToolResult.model_validate({"content": "invalid"}, strict=True)
    session.call_error = captured.value
    runtime = await _ready_runtime(session)

    result = await runtime.invoke(_ENTRY_ID, {"query": "octopus"})

    assert result.code is not None
    assert result.code.value == "tool_mcp_invalid_result"
    assert runtime.state is RuntimeState.READY
    await runtime.close()


async def test_sdk_connection_closed_loses_result_boundary() -> None:
    session = FakeSession()
    session.call_error = McpError(
        types.ErrorData(code=types.CONNECTION_CLOSED, message="Connection closed")
    )
    runtime = await _ready_runtime(session)

    with pytest.raises(RuntimeTransportError):
        await runtime.invoke(_ENTRY_ID, {"query": "octopus"})

    await runtime.close()


async def test_raw_message_limit_has_distinct_runtime_failure() -> None:
    session = FakeSession()
    session.call_error = McpMessageTooLargeError("secret raw detail")
    runtime = await _ready_runtime(session)

    with pytest.raises(RuntimeMessageTooLargeError, match="message limit"):
        await runtime.invoke(_ENTRY_ID, {"query": "octopus"})

    await runtime.close()


async def test_grouped_raw_message_limit_has_distinct_runtime_failure() -> None:
    session = FakeSession()
    session.call_error = ExceptionGroup(
        "MCP reader failed",
        [McpMessageTooLargeError("secret grouped raw detail")],
    )
    runtime = await _ready_runtime(session)

    with pytest.raises(RuntimeMessageTooLargeError, match="message limit"):
        await runtime.invoke(_ENTRY_ID, {"query": "octopus"})

    await runtime.close()


async def test_message_handler_marks_wrapped_raw_message_overflow() -> None:
    handler = ServerMcpMessageHandler()
    wrapped = RuntimeError("FastMCP reader failed")
    wrapped.__cause__ = ExceptionGroup(
        "transport task failed",
        [McpMessageTooLargeError("secret grouped raw detail")],
    )

    await handler.on_exception(wrapped)

    assert handler.message_too_large is True
    assert await handler.next_event() == "transport_failed"


async def test_idle_runtime_observes_transport_message_limit_signal() -> None:
    runtime = await _ready_runtime(FakeSession())

    runtime._transport_failure_signal.report(  # noqa: SLF001 - transport side-channel
        "message_too_large"
    )

    assert await runtime.next_event() == "transport_failed"
    failure = runtime.transport_failure()
    assert failure.code == "mcp_message_too_large"
    assert failure.permanent is True
    await runtime.close()


async def test_idle_runtime_observes_unsupported_encoding_signal() -> None:
    runtime = await _ready_runtime(FakeSession())

    runtime._transport_failure_signal.report(  # noqa: SLF001 - transport side-channel
        "unsupported_content_encoding"
    )

    assert await runtime.next_event() == "transport_failed"
    failure = runtime.transport_failure()
    assert failure.code == "config_validation_failed"
    assert failure.permanent is True
    await runtime.close()


async def test_idle_runtime_classifies_malformed_stdio_record_as_permanent() -> None:
    with pytest.raises(ValidationError) as captured:
        types.JSONRPCMessage.model_validate_json(b"not-json")
    runtime = await _ready_runtime(FakeSession(), terminal_error=captured.value)

    failure = runtime.transport_failure()

    assert failure.code == "config_validation_failed"
    assert failure.permanent is True
    await runtime.close()


async def test_wrapped_missing_stdio_executable_is_a_permanent_spawn_failure() -> None:
    config = parse_server_mcp_configs(
        [
            {
                "name": "missing",
                "transport": "stdio",
                "command": "definitely-missing-openoctopus-mcp",
                "enabled_capabilities": [],
            }
        ]
    )[0]
    runtime = RuntimeGeneration(config, coordinator=ServerMcpCoordinator())

    with pytest.raises(RuntimeOpenError) as captured:
        await runtime.open()

    assert captured.value.failure.code == "mcp_spawn_failed"
    assert captured.value.failure.permanent is True
    await runtime.close()


async def test_runtime_marks_schema_drift_instead_of_binding_changed_schema() -> None:
    config = _config()
    runtime = RuntimeGeneration(
        config,
        coordinator=ServerMcpCoordinator(),
        client_factory=lambda _config, **_kwargs: FakeClient(),
        discoverer=_discover,
    )
    source = await runtime.open()
    changed = source.model_copy(deep=True)
    changed.tools[0].description = "stale persisted description"

    from openctopus_server.devices.mcp_models import SourceMcpCatalog

    catalog = build_server_persisted_catalog(
        [config],
        SourceMcpCatalog(version=1, servers=[changed]),
        entry_id_factory=lambda: _ENTRY_ID,
    )

    assert (
        runtime.bind_authority(
            catalog.servers[0],
            config_revision=7,
            catalog_digest=catalog.digest,
        )
        is False
    )
    assert runtime.state is RuntimeState.DRIFTED
    assert runtime.routes == {}
    await runtime.close()


async def test_runtime_close_finishes_before_propagating_cancellation() -> None:
    release = asyncio.Event()

    async def delayed_close() -> None:
        await release.wait()

    client = FakeClient(close=delayed_close)
    runtime = RuntimeGeneration(
        _config(),
        coordinator=ServerMcpCoordinator(),
        client_factory=lambda _config, **_kwargs: client,
        discoverer=_discover,
    )
    await runtime.open()

    close_task = asyncio.create_task(runtime.close())
    await asyncio.sleep(0)
    close_task.cancel()
    await asyncio.sleep(0)
    assert close_task.done() is False

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await close_task
    assert client.closed is True
