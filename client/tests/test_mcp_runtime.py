from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp import types
from mcp.shared.exceptions import McpError
from pydantic import AnyUrl, SecretStr

from openoctopus_client.mcp.models import (
    McpServerConfig,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    SseMcpServerConfig,
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
)
from openoctopus_client.mcp.runtime import (
    McpRuntimeError,
    McpRuntimeEvent,
    McpRuntimeState,
    McpServerRuntime,
    build_runtime_client,
    retry_backoff_seconds,
    validate_candidate,
)
from openoctopus_client.mcp.transport import (
    BoundedStdioTransport,
    McpMessageTooLargeError,
)
from openoctopus_client.protocol import new_uuid7


class _DiscoveryCounter:
    def __init__(self) -> None:
        self.current = 0
        self.peak = 0

    async def pause(self) -> None:
        self.current += 1
        self.peak = max(self.peak, self.current)
        try:
            await asyncio.sleep(0.02)
        finally:
            self.current -= 1


class _FakeSession:
    def __init__(
        self,
        *,
        counter: _DiscoveryCounter | None = None,
        discovery_error: BaseException | None = None,
    ) -> None:
        self.counter = counter
        self.discovery_error = discovery_error
        self.tools = [
            types.Tool(
                name="calculate",
                description="Calculate",
                inputSchema={"type": "object", "properties": {"value": {"type": "integer"}}},
                outputSchema={"type": "object", "required": ["answer"]},
            )
        ]
        self.resources = [
            types.Resource(
                name="manual",
                uri=AnyUrl("file:///manual.txt"),
                description="Manual",
                mimeType="text/plain",
            )
        ]
        self.templates = [
            types.ResourceTemplate(
                name="issue",
                uriTemplate="https://example.test/issues/{id}",
                description="Issue",
            )
        ]
        self.prompts = [
            types.Prompt(
                name="explain",
                description="Explain",
                arguments=[types.PromptArgument(name="topic", required=True)],
            )
        ]
        self.sent_requests: list[types.ClientRequest] = []
        self.read_uris: list[str] = []
        self.prompt_calls: list[tuple[str, dict[str, str] | None]] = []
        self.call_tool_used = False
        self.send_error: Exception | None = None

    def get_server_capabilities(self) -> types.ServerCapabilities:
        return types.ServerCapabilities(
            tools=types.ToolsCapability(),
            resources=types.ResourcesCapability(),
            prompts=types.PromptsCapability(),
        )

    async def _before_discovery(self) -> None:
        if self.counter is not None:
            await self.counter.pause()
        if self.discovery_error is not None:
            raise self.discovery_error

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult:
        assert cursor is None
        await self._before_discovery()
        return types.ListToolsResult(tools=self.tools)

    async def list_resources(self, cursor: str | None = None) -> types.ListResourcesResult:
        assert cursor is None
        return types.ListResourcesResult(resources=self.resources)

    async def list_resource_templates(
        self, cursor: str | None = None
    ) -> types.ListResourceTemplatesResult:
        assert cursor is None
        return types.ListResourceTemplatesResult(resourceTemplates=self.templates)

    async def list_prompts(self, cursor: str | None = None) -> types.ListPromptsResult:
        assert cursor is None
        return types.ListPromptsResult(prompts=self.prompts)

    async def send_request(
        self,
        request: types.ClientRequest,
        result_type: type[types.CallToolResult],
    ) -> types.CallToolResult:
        assert result_type is types.CallToolResult
        self.sent_requests.append(request)
        if self.send_error is not None:
            raise self.send_error
        return types.CallToolResult(
            content=[types.TextContent(type="text", text="tool result")],
            structuredContent={"wrong": "outputSchema is intentionally ignored"},
        )

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult:
        del name, arguments
        self.call_tool_used = True
        raise AssertionError("runtime must use raw send_request")

    async def read_resource(self, uri: Any) -> types.ReadResourceResult:
        self.read_uris.append(str(uri))
        return types.ReadResourceResult(
            contents=[types.TextResourceContents(uri=uri, text="resource result")]
        )

    async def get_prompt(
        self, name: str, arguments: dict[str, str] | None = None
    ) -> types.GetPromptResult:
        self.prompt_calls.append((name, arguments))
        return types.GetPromptResult(
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(type="text", text="prompt result"),
                )
            ]
        )


class _FakeClient:
    def __init__(
        self,
        session: _FakeSession,
        *,
        enter_error: BaseException | None = None,
        enter_delay: float = 0,
        close_delay: float = 0,
        close_error: Exception | None = None,
    ) -> None:
        self.session = session
        self.transport = SimpleNamespace(cleanup_incomplete=False)
        self.enter_error = enter_error
        self.enter_delay = enter_delay
        self.close_delay = close_delay
        self.close_error = close_error
        self.entered = False
        self.closed = False

    async def __aenter__(self) -> _FakeClient:
        if self.enter_delay:
            await asyncio.sleep(self.enter_delay)
        if self.enter_error is not None:
            raise self.enter_error
        self.entered = True
        return self

    async def close(self) -> None:
        if self.close_delay:
            await asyncio.sleep(self.close_delay)
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class _FakeBuilder:
    def __init__(self, factory: Callable[[str], _FakeClient] | None = None) -> None:
        self.factory = factory or (lambda _name: _FakeClient(_FakeSession()))
        self.clients: dict[str, _FakeClient] = {}

    def __call__(self, config: McpServerConfig) -> _FakeClient:
        client = self.factory(config.name)
        self.clients[config.name] = client
        return client


def _config(name: str = "local") -> StdioMcpServerConfig:
    return StdioMcpServerConfig(
        name=name,
        transport="stdio",
        command=sys.executable,
        args=[],
        cwd=None,
        env={},
    )


def _remote_config(name: str = "remote") -> StreamableHttpMcpServerConfig:
    return StreamableHttpMcpServerConfig(
        name=name,
        transport="streamable_http",
        url="https://mcp.invalid/mcp",
        headers={},
    )


def test_runtime_public_message_handler_emits_bounded_change_and_failure_events() -> None:
    async def exercise() -> list[McpRuntimeEvent]:
        runtime = McpServerRuntime(_config(), client_factory=_FakeBuilder())
        handler = runtime.message_handler
        await handler.on_tool_list_changed(types.ToolListChangedNotification())
        await handler.on_tool_list_changed(types.ToolListChangedNotification())
        await handler.on_resource_list_changed(types.ResourceListChangedNotification())
        await handler.on_prompt_list_changed(types.PromptListChangedNotification())
        await handler.on_exception(RuntimeError("secret must not be retained"))
        return [await runtime.next_event() for _index in range(4)]

    assert [event.kind for event in asyncio.run(exercise())] == [
        "tools_changed",
        "resources_changed",
        "prompts_changed",
        "transport_failed",
    ]


@pytest.mark.asyncio
async def test_idle_http_message_limit_failure_keeps_specific_code_and_retries() -> None:
    client = _FakeClient(_FakeSession())
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
    )
    await runtime.start()
    await runtime.message_handler.on_exception(
        McpMessageTooLargeError("secret raw detail")
    )

    await runtime.mark_transport_unavailable()

    assert runtime.state is McpRuntimeState.UNAVAILABLE
    assert runtime.code == "tool_mcp_message_too_large"
    assert runtime.enter_backoff(jitter=0.5) == 1


@pytest.mark.asyncio
async def test_discovery_sdk_close_preserves_handler_message_limit_failure() -> None:
    session = _FakeSession(
        discovery_error=McpError(
            types.ErrorData(
                code=types.CONNECTION_CLOSED,
                message="Connection closed",
            )
        )
    )
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: _FakeClient(session)),
    )
    await runtime.message_handler.on_exception(
        McpMessageTooLargeError("secret raw detail")
    )

    with pytest.raises(McpRuntimeError) as caught:
        await runtime.start()

    assert caught.value.failure.code == "mcp_message_too_large"
    assert "secret raw detail" not in caught.value.failure.message


def _entry(
    *,
    surface: str,
    raw_name: str,
    identity: str,
    final_name: str,
) -> PersistedMcpCatalogEntry:
    return PersistedMcpCatalogEntry(
        entry_id=new_uuid7(),
        server="local",
        surface=cast(Any, surface),
        raw_name=raw_name,
        invocation_identity=identity,
        final_name=final_name,
        provider_description="test",
        input_schema={"type": "object"},
        enabled=True,
    )


def _persisted_server() -> PersistedMcpServerCatalog:
    return PersistedMcpServerCatalog(
        name="local",
        entries=[
            _entry(
                surface="tool",
                raw_name="calculate",
                identity="calculate",
                final_name="mcp_local_calculate",
            ),
            _entry(
                surface="resource",
                raw_name="manual",
                identity="file:///manual.txt",
                final_name="mcp_local_manual",
            ),
            _entry(
                surface="resource_template",
                raw_name="issue",
                identity="https://example.test/issues/{id}",
                final_name="mcp_local_issue",
            ),
            _entry(
                surface="prompt",
                raw_name="explain",
                identity="explain",
                final_name="mcp_local_explain",
            ),
        ],
    )


def test_build_runtime_client_uses_only_explicit_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENOCTOPUS_DEVICE_TOKEN", "never-forward")
    stdio = build_runtime_client(
        StdioMcpServerConfig(
            name="local",
            transport="stdio",
            command=sys.executable,
            env={"MCP_SENTINEL": SecretStr("allowed")},
        )
    )
    streamable = build_runtime_client(
        StreamableHttpMcpServerConfig(
            name="remote",
            transport="streamable_http",
            url="https://example.test/mcp?visible=config",
            headers={"Authorization": SecretStr("Bearer secret")},
        )
    )
    sse = build_runtime_client(
        SseMcpServerConfig(
            name="legacy",
            transport="sse",
            url="https://example.test/sse",
            headers={},
        )
    )

    assert isinstance(stdio.transport, BoundedStdioTransport)
    assert stdio.transport.cwd == Path.home()
    assert stdio.transport.environment["MCP_SENTINEL"] == "allowed"
    assert "OPENOCTOPUS_DEVICE_TOKEN" not in stdio.transport.environment
    assert isinstance(streamable.transport, StreamableHttpTransport)
    assert streamable.transport.url == "https://example.test/mcp?visible=config"
    assert streamable.transport.headers == {"authorization": "Bearer secret"}
    assert streamable.transport.httpx_client_factory is not None
    http_client = cast(
        Callable[..., httpx.AsyncClient],
        streamable.transport.httpx_client_factory,
    )(follow_redirects=True)
    assert http_client.follow_redirects is False
    asyncio.run(http_client.aclose())
    assert isinstance(sse.transport, SSETransport)
    assert sse.transport.httpx_client_factory is not None


@pytest.mark.asyncio
async def test_candidate_validation_is_canonical_bounded_and_promotable() -> None:
    counter = _DiscoveryCounter()
    builder = _FakeBuilder(lambda _name: _FakeClient(_FakeSession(counter=counter)))
    configs = [_config(f"server_{index}") for index in reversed(range(6))]

    outcome = await validate_candidate(
        configs,
        client_factory=builder,
        max_parallel=4,
        candidate_timeout=2,
    )

    assert outcome.ok is True
    assert outcome.failures == ()
    assert outcome.source_catalog is not None
    assert [server.name for server in outcome.source_catalog.servers] == [
        f"server_{index}" for index in range(6)
    ]
    assert counter.peak == 4
    assert set(outcome.runtimes) == {config.name for config in configs}
    assert all(
        runtime.state is McpRuntimeState.AWAITING_ACK
        for runtime in outcome.runtimes.values()
    )

    await outcome.close()
    assert all(client.closed for client in builder.clients.values())


@pytest.mark.asyncio
async def test_candidate_failure_is_secret_safe_and_closes_every_runtime() -> None:
    sentinel = "authorization-secret-from-third-party"
    builder = _FakeBuilder(
        lambda name: _FakeClient(
            _FakeSession(),
            enter_error=RuntimeError(f"connection included {sentinel} for {name}"),
        )
    )

    outcome = await validate_candidate([_config()], client_factory=builder)

    assert outcome.ok is False
    assert outcome.source_catalog is None
    assert len(outcome.failures) == 1
    failure = outcome.failures[0]
    assert (failure.server, failure.stage, failure.code) == (
        "local",
        "connect",
        "config_validation_failed",
    )
    assert sentinel not in failure.message
    assert sentinel not in repr(failure)
    assert builder.clients["local"].closed is True
    assert outcome.runtimes == {}


@pytest.mark.asyncio
async def test_candidate_cleanup_failure_cannot_leak_third_party_exception() -> None:
    sentinel = "secret-from-close-exception"
    retained: list[McpServerRuntime] = []
    builder = _FakeBuilder(
        lambda _name: _FakeClient(
            _FakeSession(),
            enter_error=RuntimeError("connect failed"),
            close_error=RuntimeError(sentinel),
        )
    )

    outcome = await validate_candidate(
        [_config()],
        client_factory=builder,
        cleanup_sink=retained.extend,
    )

    assert outcome.ok is False
    assert sentinel not in repr(outcome.failures)
    assert builder.clients["local"].closed is True
    assert retained == []


@pytest.mark.asyncio
async def test_candidate_failure_reports_cleanup_blocked_runtime_to_owner() -> None:
    retained: list[McpServerRuntime] = []
    client = _FakeClient(_FakeSession(), enter_error=RuntimeError("connect failed"))
    client.transport.cleanup_incomplete = True
    builder = _FakeBuilder(lambda _name: client)

    outcome = await validate_candidate(
        [_config()],
        client_factory=builder,
        cleanup_sink=retained.extend,
    )

    assert not outcome.ok
    assert len(retained) == 1
    assert retained[0].state is McpRuntimeState.CLEANUP_BLOCKED


@pytest.mark.asyncio
async def test_cancelled_candidate_reports_cleanup_blocked_runtime_to_owner() -> None:
    retained: list[McpServerRuntime] = []
    client = _FakeClient(_FakeSession(), enter_delay=60)
    client.transport.cleanup_incomplete = True
    builder = _FakeBuilder(lambda _name: client)
    task = asyncio.create_task(
        validate_candidate(
            [_config()],
            client_factory=builder,
            cleanup_sink=retained.extend,
        )
    )
    for _attempt in range(20):
        if builder.clients:
            break
        await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(retained) == 1
    assert retained[0].state is McpRuntimeState.CLEANUP_BLOCKED


@pytest.mark.asyncio
async def test_connect_and_discovery_have_independent_outer_deadlines() -> None:
    connect_builder = _FakeBuilder(
        lambda _name: _FakeClient(_FakeSession(), enter_delay=0.05)
    )
    connect = await validate_candidate(
        [_config()],
        client_factory=connect_builder,
        connect_timeout=0.005,
    )

    discovery_builder = _FakeBuilder(
        lambda _name: _FakeClient(
            _FakeSession(counter=_DiscoveryCounter()),
        )
    )
    discovery = await validate_candidate(
        [_config()],
        client_factory=discovery_builder,
        discovery_timeout=0.005,
    )

    assert connect.failures[0].stage == "connect"
    assert discovery.failures[0].stage == "discovery"
    assert connect_builder.clients["local"].closed is True
    assert discovery_builder.clients["local"].closed is True


@pytest.mark.asyncio
async def test_runtime_binds_exact_entries_and_invokes_all_four_surfaces() -> None:
    session = _FakeSession()
    builder = _FakeBuilder(lambda _name: _FakeClient(session))
    runtime = McpServerRuntime(_config(), client_factory=builder)
    generation = runtime.generation

    source = await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(generation)

    assert source.name == "local"
    assert runtime.state is McpRuntimeState.READY
    by_surface = {route.surface: entry_id for entry_id, route in routes.items()}

    tool = await runtime.invoke(
        by_surface["tool"],
        {"value": 3},
        runtime_generation=generation,
    )
    resource = await runtime.invoke(
        by_surface["resource"],
        {},
        runtime_generation=generation,
    )
    template = await runtime.invoke(
        by_surface["resource_template"],
        {"id": "42"},
        runtime_generation=generation,
    )
    prompt = await runtime.invoke(
        by_surface["prompt"],
        {"topic": "MCP"},
        runtime_generation=generation,
    )

    assert tool.is_error is False
    assert resource.is_error is False
    assert template.is_error is False
    assert prompt.is_error is False
    request = session.sent_requests[0].root
    assert isinstance(request, types.CallToolRequest)
    assert request.params.name == "calculate"
    assert request.params.arguments == {"value": 3}
    assert session.call_tool_used is False
    assert session.read_uris == [
        "file:///manual.txt",
        "https://example.test/issues/42",
    ]
    assert session.prompt_calls == [("explain", {"topic": "MCP"})]


@pytest.mark.asyncio
async def test_runtime_rejects_stale_unknown_and_disabled_routes_before_send() -> None:
    session = _FakeSession()
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: _FakeClient(session)),
    )
    await runtime.start()
    persisted = _persisted_server()
    routes = runtime.bind_persisted(persisted)
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")

    stale = await runtime.invoke(tool_id, {}, runtime_generation=new_uuid7())
    unknown = await runtime.invoke(new_uuid7(), {}, runtime_generation=runtime.generation)

    assert stale.code == "tool_mcp_unavailable"
    assert unknown.code == "tool_mcp_unavailable"
    assert session.sent_requests == []


@pytest.mark.asyncio
async def test_transport_failure_closes_attempt_before_backoff() -> None:
    session = _FakeSession()
    session.send_error = ConnectionError("third-party transport detail")
    client = _FakeClient(session)
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
    )
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")

    output = await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)

    assert output.code == "tool_execution_outcome_unknown"
    assert "third-party transport detail" not in str(output.content)
    assert client.closed is True
    assert runtime.state is McpRuntimeState.UNAVAILABLE
    assert runtime.enter_backoff(jitter=0.5) == 1


@pytest.mark.asyncio
async def test_invocation_timeout_closes_ambiguous_runtime_before_backoff() -> None:
    class HangingSession(_FakeSession):
        async def send_request(
            self,
            request: types.ClientRequest,
            result_type: type[types.CallToolResult],
        ) -> types.CallToolResult:
            assert result_type is types.CallToolResult
            self.sent_requests.append(request)
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    session = HangingSession()
    client = _FakeClient(session)
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
        invocation_timeout=0.01,
    )
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")

    output = await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)

    assert output.code == "tool_execution_outcome_unknown"
    assert client.closed
    assert runtime.state is McpRuntimeState.UNAVAILABLE
    assert runtime.enter_backoff(jitter=0.5) == 1


@pytest.mark.asyncio
async def test_sdk_connection_closed_is_outcome_unknown_and_unavailable() -> None:
    session = _FakeSession()
    session.send_error = McpError(
        types.ErrorData(code=types.CONNECTION_CLOSED, message="Connection closed")
    )
    client = _FakeClient(session)
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
    )
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")

    output = await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)

    assert output.code == "tool_execution_outcome_unknown"
    assert runtime.state is McpRuntimeState.UNAVAILABLE
    assert client.closed
    assert runtime.enter_backoff(jitter=0.5) == 1


@pytest.mark.asyncio
async def test_third_party_minus_32000_is_not_misclassified_as_disconnect() -> None:
    session = _FakeSession()
    session.send_error = McpError(
        types.ErrorData(
            code=types.CONNECTION_CLOSED,
            message="application-specific failure",
        )
    )
    client = _FakeClient(session)
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
    )
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")

    output = await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)

    assert output.code == "tool_mcp_error"
    assert runtime.state is McpRuntimeState.READY
    assert not client.closed


@pytest.mark.asyncio
async def test_stdio_message_limit_survives_sdk_connection_closed_translation() -> None:
    session = _FakeSession()
    session.send_error = McpError(
        types.ErrorData(code=types.CONNECTION_CLOSED, message="Connection closed")
    )
    client = _FakeClient(session)
    client.transport.terminal_error = McpMessageTooLargeError("secret raw detail")
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
    )
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")

    output = await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)

    assert output.code == "tool_mcp_message_too_large"
    assert "secret raw detail" not in str(output.content)
    assert runtime.state is McpRuntimeState.UNAVAILABLE


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403])
async def test_invocation_auth_failure_suspends_without_backoff(status_code: int) -> None:
    request = httpx.Request("POST", "https://mcp.invalid/mcp")
    response = httpx.Response(status_code, request=request)
    session = _FakeSession()
    session.send_error = httpx.HTTPStatusError(
        "secret upstream response",
        request=request,
        response=response,
    )
    client = _FakeClient(session)
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
    )
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")

    output = await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)

    assert output.code == "tool_execution_outcome_unknown"
    assert "secret upstream response" not in str(output.content)
    assert runtime.state is McpRuntimeState.UNAVAILABLE
    assert runtime.permanent_failure
    assert runtime.enter_backoff(jitter=0.5) is None


@pytest.mark.asyncio
async def test_ready_registration_resets_retry_backoff_history() -> None:
    sessions = [_FakeSession(), _FakeSession()]
    clients = [_FakeClient(session) for session in sessions]
    created = 0

    def client_factory(_name: str) -> _FakeClient:
        nonlocal created
        client = clients[created]
        created += 1
        return client

    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(client_factory),
    )
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")
    sessions[0].send_error = ConnectionError("first drop")
    await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)
    assert runtime.enter_backoff(jitter=0.5) == 1

    runtime.begin_retry()
    await runtime.start()
    routes = runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)
    tool_id = next(entry_id for entry_id, route in routes.items() if route.surface == "tool")
    sessions[1].send_error = ConnectionError("later drop")
    await runtime.invoke(tool_id, {}, runtime_generation=runtime.generation)

    assert runtime.enter_backoff(jitter=0.5) == 1


@pytest.mark.asyncio
async def test_runtime_close_retries_cleanup_blocked_client() -> None:
    session = _FakeSession()

    class RetryCleanupClient(_FakeClient):
        def __init__(self) -> None:
            super().__init__(session)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            self.transport.cleanup_incomplete = self.close_calls == 1
            self.closed = True

    client = RetryCleanupClient()
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: client),
    )
    await runtime.start()

    await runtime.close()
    assert runtime.state.value == "cleanup_blocked"
    await runtime.close()

    assert client.close_calls == 2
    assert runtime.state is McpRuntimeState.ABSENT


@pytest.mark.asyncio
async def test_remote_close_deadline_does_not_block_replacement() -> None:
    release = asyncio.Event()

    class HangingCloseClient(_FakeClient):
        def __init__(self) -> None:
            super().__init__(_FakeSession())
            self.close_calls = 0
            self.close_cancelled = False

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    self.close_cancelled = True
                    raise
            self.closed = True

    client = HangingCloseClient()
    runtime = McpServerRuntime(
        _remote_config(),
        client_factory=_FakeBuilder(lambda _name: client),
        cleanup_timeout=0.01,
    )
    await runtime.start()
    close_task = asyncio.create_task(runtime.close())
    completed = False
    first_state: McpRuntimeState | None = None
    try:
        done, _pending = await asyncio.wait({close_task}, timeout=0.1)
        completed = close_task in done
        if completed:
            await close_task
            first_state = runtime.state
    finally:
        release.set()
        if not close_task.done():
            await close_task
        await runtime.close()

    assert completed
    assert first_state is McpRuntimeState.ABSENT
    assert client.close_cancelled
    assert runtime.state is McpRuntimeState.ABSENT


@pytest.mark.asyncio
async def test_fresh_discovery_marks_schema_drift_without_rebinding_old_routes() -> None:
    session = _FakeSession()
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: _FakeClient(session)),
    )
    await runtime.start()
    runtime.bind_persisted(_persisted_server())
    runtime.mark_ready(runtime.generation)

    unchanged = await runtime.refresh()
    session.tools[0] = types.Tool(
        name="calculate",
        description="Changed schema",
        inputSchema={"type": "object", "properties": {}},
    )
    changed = await runtime.refresh()

    assert unchanged is True
    assert changed is False
    assert runtime.state is McpRuntimeState.DRIFTED
    assert runtime.code == "mcp_schema_drift"


@pytest.mark.asyncio
async def test_close_finishes_under_caller_cancellation() -> None:
    fake = _FakeClient(_FakeSession(), close_delay=0.02)
    runtime = McpServerRuntime(
        _config(),
        client_factory=_FakeBuilder(lambda _name: fake),
    )
    await runtime.start()
    task = asyncio.create_task(runtime.close())
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert fake.closed is True
    assert runtime.state is McpRuntimeState.ABSENT


def test_backoff_is_capped_and_jittered_deterministically() -> None:
    assert retry_backoff_seconds(0, jitter=0.5) == 1
    assert retry_backoff_seconds(1, jitter=0.5) == 2
    assert retry_backoff_seconds(8, jitter=0.5) == 60
    assert retry_backoff_seconds(0, jitter=0) == pytest.approx(0.8)
    assert retry_backoff_seconds(0, jitter=1) == pytest.approx(1.2)
