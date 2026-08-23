from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from functools import partial
from pathlib import Path
from typing import cast
from uuid import UUID

import httpx
import pytest
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp import types
from mcp.shared.exceptions import McpError

from openctopus_server.devices.mcp_models import SourceMcpCatalog
from openctopus_server.mcp import transport as transport_module
from openctopus_server.mcp.catalog import build_server_persisted_catalog
from openctopus_server.mcp.models import ServerStreamableHttpMcpServerConfig
from openctopus_server.mcp.runtime import (
    RuntimeGeneration,
    RuntimeMessageTooLargeError,
    RuntimeOpenError,
    RuntimeTransportError,
)
from openctopus_server.mcp.scheduler import ServerMcpCoordinator
from openctopus_server.mcp.transport import (
    BoundedHttpTransport,
    BoundedStdioTransport,
    McpMessageTooLargeError,
    McpTransportFailureSignal,
    UnsupportedMcpContentEncodingError,
    build_mcp_environment,
    build_runtime_client,
    create_fastmcp_client,
    create_mcp_http_client,
    install_mcp_log_discard_boundary,
)


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class RaisingCloseStream(ChunkStream):
    async def aclose(self) -> None:
        raise RuntimeError("close failed")


class ResponseTransport(httpx.AsyncBaseTransport):
    def __init__(self, chunks: list[bytes], headers: dict[str, str] | None = None) -> None:
        self.chunks = chunks
        self.headers = headers or {}
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, headers=self.headers, stream=ChunkStream(self.chunks))


class RaisingCloseTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(200, stream=RaisingCloseStream([b"123456789"]))


class RaisingCloseEncodingTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=RaisingCloseStream([b"unused"]),
        )


def _json_response(payload: object, *, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers={"Content-Type": "application/json"},
        stream=ChunkStream([json.dumps(payload).encode("utf-8")]),
    )


class ConcurrentStreamableTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.active_calls = 0
        self.peak_calls = 0
        self.both_started = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        payload = cast(dict[str, object], json.loads(await request.aread()))
        if "id" not in payload:
            return _json_response({}, status_code=202)
        request_id = payload["id"]
        method = payload["method"]
        if method == "initialize":
            params = cast(dict[str, object], payload["params"])
            return _json_response(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": params["protocolVersion"],
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "concurrent", "version": "1"},
                    },
                }
            )
        if method == "tools/call":
            params = cast(dict[str, object], payload["params"])
            arguments = cast(dict[str, object], params["arguments"])
            value = cast(str, arguments["value"])
            self.active_calls += 1
            self.peak_calls = max(self.peak_calls, self.active_calls)
            if self.active_calls == 2:
                self.both_started.set()
            try:
                await asyncio.wait_for(self.both_started.wait(), 1)
            finally:
                self.active_calls -= 1
            return _json_response(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"content": [{"type": "text", "text": value}]},
                }
            )
        raise AssertionError(f"unexpected method: {method}")


class RuntimeOverflowTransport(httpx.AsyncBaseTransport):
    def __init__(self, stage: str, *, include_content_length: bool = True) -> None:
        self.stage = stage
        self.include_content_length = include_content_length

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        payload = cast(dict[str, object], json.loads(await request.aread()))
        if "id" not in payload:
            return _json_response({}, status_code=202)
        method = payload["method"]
        if method == "initialize":
            params = cast(dict[str, object], payload["params"])
            result: dict[str, object] = {
                "protocolVersion": params["protocolVersion"],
                "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                "serverInfo": {"name": "overflow", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "echo",
                        "description": (
                            "x" * 2_000 if self.stage == "discovery" else "Echo text."
                        ),
                        "inputSchema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    }
                ]
            }
        elif method == "resources/list":
            result = {"resources": []}
        elif method == "resources/templates/list":
            result = {"resourceTemplates": []}
        elif method == "prompts/list":
            result = {"prompts": []}
        elif method == "tools/call":
            result = {
                "content": [
                    {
                        "type": "text",
                        "text": "x" * 2_000,
                    }
                ]
            }
        else:
            raise AssertionError(f"unexpected method: {method}")
        body = json.dumps(
            {"jsonrpc": "2.0", "id": payload["id"], "result": result}
        ).encode()
        headers = {"Content-Type": "application/json"}
        if self.stage == "encoding" or (
            self.stage == "invocation_encoding" and method == "tools/call"
        ):
            headers["Content-Encoding"] = "gzip"
        if self.include_content_length:
            headers["Content-Length"] = str(len(body))
        return httpx.Response(200, headers=headers, stream=ChunkStream([body]))


def _runtime_overflow_client_factory(backend: httpx.AsyncBaseTransport):
    def client_factory(_config, **kwargs):  # type: ignore[no-untyped-def]
        transport = StreamableHttpTransport(
            "https://mcp.invalid/mcp",
            httpx_client_factory=partial(
                create_mcp_http_client,
                _transport=backend,
                transport_failure_signal=kwargs.get("transport_failure_signal"),
            ),
        )
        return create_fastmcp_client(
            transport,
            message_handler=kwargs.get("message_handler"),
        )

    return client_factory


class QueueSseStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue()

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        while (chunk := await self.queue.get()) is not None:
            yield chunk

    async def aclose(self) -> None:
        self.queue.put_nowait(None)


class LegacySseTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.stream = QueueSseStream()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            self.stream.queue.put_nowait(
                b"event: endpoint\ndata: https://mcp.invalid/messages\n\n"
            )
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=self.stream,
            )
        payload = cast(dict[str, object], json.loads(await request.aread()))
        if "id" in payload:
            params = cast(dict[str, object], payload.get("params", {}))
            if payload["method"] == "initialize":
                result: dict[str, object] = {
                    "protocolVersion": params["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "legacy", "version": "1"},
                }
            elif payload["method"] == "tools/list":
                result = {
                    "tools": [
                        {
                            "name": "search",
                            "inputSchema": {"type": "object", "properties": {}},
                        }
                    ]
                }
            else:
                raise AssertionError(f"unexpected method: {payload['method']}")
            response = json.dumps(
                {"jsonrpc": "2.0", "id": payload["id"], "result": result},
                separators=(",", ":"),
            ).encode("utf-8")
            self.stream.queue.put_nowait(b"event: message\ndata: " + response + b"\n\n")
        return _json_response({}, status_code=202)


def test_stdio_environment_is_posix_baseline_plus_overlay_without_server_secrets() -> None:
    result = build_mcp_environment(
        {
            "HOME": "/safe-home",
            "PATH": "/safe-bin",
            "DATABASE_URL": "must-not-pass",
            "OPENOCTOPUS_ADMIN_TOKEN": "must-not-pass",
        },
        {"CUSTOM": "allowed", "HOME": "/configured", "openoctopus_x": "blocked"},
    )

    assert result == {"HOME": "/configured", "PATH": "/safe-bin", "CUSTOM": "allowed"}


def test_mcp_loggers_are_discarded_before_application_handlers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_mcp_log_discard_boundary()
    with caplog.at_level(1):
        logging.getLogger("fastmcp.client").critical("secret-sentinel")
        logging.getLogger("openctopus_server.keep").warning("visible")
    assert "secret-sentinel" not in caplog.text
    assert "visible" in caplog.text


@pytest.mark.asyncio
async def test_http_entity_cap_and_content_encoding_are_predecode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openctopus_server.mcp.transport.MCP_MESSAGE_BYTES_MAX", 8)
    message_limit = McpTransportFailureSignal()
    overflow = ResponseTransport([b"1234", b"56789"])
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            overflow,
            transport_failure_signal=message_limit,
        )
    ) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid")
    assert message_limit.kind == "message_too_large"

    close_failure_limit = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            RaisingCloseTransport(),
            transport_failure_signal=close_failure_limit,
        )
    ) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid")
    assert close_failure_limit.kind == "message_too_large"


@pytest.mark.asyncio
async def test_sse_cap_is_per_complete_event(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("openctopus_server.mcp.transport.MCP_MESSAGE_BYTES_MAX", 10)
    accepted = ResponseTransport(
        [b"data:x\n\ndata:y\n\n"], {"Content-Type": "text/event-stream"}
    )
    async with httpx.AsyncClient(transport=BoundedHttpTransport(accepted)) as client:
        assert await (await client.get("https://mcp.invalid")).aread() == (
            b"data:x\n\ndata:y\n\n"
        )

    rejected = ResponseTransport(
        [b"data:12345\n\n"], {"Content-Type": "text/event-stream"}
    )
    message_limit = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            rejected,
            transport_failure_signal=message_limit,
        )
    ) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid")
    assert message_limit.kind == "message_too_large"

    compressed = ResponseTransport([b"unused"], {"Content-Encoding": "gzip"})
    encoding_failure = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            compressed,
            transport_failure_signal=encoding_failure,
        )
    ) as client:
        with pytest.raises(UnsupportedMcpContentEncodingError):
            await client.get("https://mcp.invalid")
    assert encoding_failure.kind == "unsupported_content_encoding"

    close_failure = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            RaisingCloseEncodingTransport(),
            transport_failure_signal=close_failure,
        )
    ) as client:
        with pytest.raises(UnsupportedMcpContentEncodingError):
            await client.get("https://mcp.invalid")
    assert close_failure.kind == "unsupported_content_encoding"


@pytest.mark.asyncio
async def test_http_factory_forces_identity_no_redirects_and_no_proxy() -> None:
    inner = ResponseTransport([])
    client = create_mcp_http_client(
        headers={"Accept-Encoding": "gzip"},
        follow_redirects=True,
        _transport=inner,
    )
    async with client:
        await client.get("https://mcp.invalid")
    assert inner.requests[0].headers["accept-encoding"] == "identity"
    assert client.follow_redirects is False
    assert client._trust_env is False


@pytest.mark.asyncio
async def test_real_stdio_initializes_four_surfaces_with_direct_argv() -> None:
    fixture = (
        Path(__file__).parents[2] / "client" / "tests" / "fixtures" / "fake_mcp_surfaces_stdio.py"
    )
    transport = BoundedStdioTransport(sys.executable, (str(fixture),))
    client = create_fastmcp_client(transport)

    async with client:
        assert [item.name for item in (await client.session.list_tools()).tools] == ["echo"]
        assert [item.name for item in (await client.session.list_resources()).resources] == [
            "manual"
        ]
        assert [
            item.name for item in (await client.session.list_resource_templates()).resourceTemplates
        ] == ["issue"]
        assert [item.name for item in (await client.session.list_prompts()).prompts] == [
            "explain"
        ]

    assert transport.process is not None and transport.process.returncode is not None
    assert transport.cleanup_incomplete is False


@pytest.mark.asyncio
async def test_stdio_limit_is_enforced_before_json_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openctopus_server.mcp.transport.MCP_MESSAGE_BYTES_MAX", 8)
    transport = BoundedStdioTransport(
        sys.executable,
        ("-c", "import sys;sys.stdout.buffer.write(b'x'*9);sys.stdout.buffer.flush()"),
    )
    client = create_fastmcp_client(transport)

    with pytest.raises(McpError, match="Connection closed"):
        async with client:
            pass
    assert isinstance(transport.terminal_error, McpMessageTooLargeError)


@pytest.mark.asyncio
async def test_stdio_close_is_cancellation_shielded_through_force_kill(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("openctopus_server.mcp.transport._STDIN_EOF_SECONDS", 0.02)
    monkeypatch.setattr("openctopus_server.mcp.transport._TERMINATE_SECONDS", 0.02)
    monkeypatch.setattr("openctopus_server.mcp.transport._FORCE_KILL_SECONDS", 0.2)
    child_pid_path = tmp_path / "stdio-child.pid"
    code = (
        "import pathlib,signal,subprocess,sys,time;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);"
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid));"
        "\nwhile child.poll() is None: time.sleep(0.005)\n"
        "time.sleep(60)"
    )
    transport = BoundedStdioTransport(
        sys.executable,
        ("-c", code, str(child_pid_path)),
    )
    await transport._start()
    try:
        for _ in range(100):
            if child_pid_path.exists():
                break
            await asyncio.sleep(0.01)
        assert child_pid_path.exists()
        assert transport.process is not None
        child_pid = int(child_pid_path.read_text())
        assert os.getpgid(child_pid) == transport.process.pid

        close_task = asyncio.create_task(transport.close())
        await asyncio.sleep(0)
        close_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await close_task
        assert transport.process.returncode is not None
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
        with pytest.raises(ProcessLookupError):
            os.killpg(transport.process.pid, 0)
        assert transport.cleanup_incomplete is False
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_one_streamable_http_session_multiplexes_two_requests_by_id() -> None:
    backend = ConcurrentStreamableTransport()
    transport = StreamableHttpTransport(
        "https://mcp.invalid/mcp",
        httpx_client_factory=partial(create_mcp_http_client, _transport=backend),
    )
    client = create_fastmcp_client(transport)

    async def call(value: str) -> types.CallToolResult:
        return await client.session.send_request(
            types.ClientRequest(
                types.CallToolRequest(
                    params=types.CallToolRequestParams(
                        name="echo",
                        arguments={"value": value},
                    )
                )
            ),
            types.CallToolResult,
        )

    async with client:
        first, second = await asyncio.gather(call("one"), call("two"))

    assert backend.peak_calls == 2
    assert first.content == [types.TextContent(type="text", text="one")]
    assert second.content == [types.TextContent(type="text", text="two")]


@pytest.mark.asyncio
async def test_real_fastmcp_connect_classifies_wrapped_message_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = RuntimeOverflowTransport("connect")

    async def unused_discovery(_name: str, _session: object):  # type: ignore[no-untyped-def]
        raise AssertionError("overflow must fail before discovery")

    runtime = RuntimeGeneration(
        ServerStreamableHttpMcpServerConfig(
            name="overflow",
            transport="streamable_http",
            url="https://mcp.invalid/mcp",
            enabled_capabilities=[],
        ),
        coordinator=ServerMcpCoordinator(),
        client_factory=_runtime_overflow_client_factory(backend),
        discoverer=unused_discovery,
        connect_timeout=2,
        cleanup_timeout=0.2,
    )
    monkeypatch.setattr(transport_module, "MCP_MESSAGE_BYTES_MAX", 64)

    try:
        with pytest.raises(RuntimeOpenError) as captured:
            await runtime.open()

        assert captured.value.failure.code == "mcp_message_too_large"
        assert captured.value.failure.permanent is True
    finally:
        await runtime.cancel_pending_cleanup()


@pytest.mark.asyncio
async def test_real_fastmcp_rejects_content_encoding_without_retry() -> None:
    backend = RuntimeOverflowTransport("encoding")

    async def unused_discovery(_name: str, _session: object):  # type: ignore[no-untyped-def]
        raise AssertionError("encoding rejection must fail before discovery")

    runtime = RuntimeGeneration(
        ServerStreamableHttpMcpServerConfig(
            name="encoding",
            transport="streamable_http",
            url="https://mcp.invalid/mcp",
            enabled_capabilities=[],
        ),
        coordinator=ServerMcpCoordinator(),
        client_factory=_runtime_overflow_client_factory(backend),
        discoverer=unused_discovery,
        connect_timeout=2,
        cleanup_timeout=0.2,
    )

    try:
        with pytest.raises(RuntimeOpenError) as captured:
            await runtime.open()

        assert captured.value.failure.code == "config_validation_failed"
        assert captured.value.failure.permanent is True
    finally:
        await runtime.cancel_pending_cleanup()


@pytest.mark.asyncio
async def test_real_fastmcp_discovery_observes_transport_overflow_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = RuntimeOverflowTransport("discovery")
    config = ServerStreamableHttpMcpServerConfig(
        name="overflow",
        transport="streamable_http",
        url="https://mcp.invalid/mcp",
        enabled_capabilities=[],
    )
    runtime = RuntimeGeneration(
        config,
        coordinator=ServerMcpCoordinator(),
        client_factory=_runtime_overflow_client_factory(backend),
        connect_timeout=2,
        discovery_timeout=0.5,
        cleanup_timeout=0.2,
    )
    monkeypatch.setattr(transport_module, "MCP_MESSAGE_BYTES_MAX", 1_024)

    try:
        with pytest.raises(RuntimeOpenError) as captured:
            await runtime.open()

        assert captured.value.failure.code == "mcp_message_too_large"
        assert captured.value.failure.permanent is True
    finally:
        await runtime.cancel_pending_cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("include_content_length", [True, False])
async def test_real_fastmcp_invocation_observes_transport_overflow_signal(
    monkeypatch: pytest.MonkeyPatch,
    include_content_length: bool,
) -> None:
    backend = RuntimeOverflowTransport(
        "invocation",
        include_content_length=include_content_length,
    )
    config = ServerStreamableHttpMcpServerConfig(
        name="overflow",
        transport="streamable_http",
        url="https://mcp.invalid/mcp",
        enabled_capabilities=[],
    )
    runtime = RuntimeGeneration(
        config,
        coordinator=ServerMcpCoordinator(),
        client_factory=_runtime_overflow_client_factory(backend),
        connect_timeout=2,
        discovery_timeout=2,
        cleanup_timeout=0.2,
    )
    monkeypatch.setattr(transport_module, "MCP_MESSAGE_BYTES_MAX", 1_024)

    try:
        source = await runtime.open()
        entry_id = UUID("01890f7c-bb80-7000-8000-000000000041")
        catalog = build_server_persisted_catalog(
            [config],
            SourceMcpCatalog(version=1, servers=[source]),
            entry_id_factory=lambda: entry_id,
        )
        runtime.bind_authority(
            catalog.servers[0],
            config_revision=2,
            catalog_digest=catalog.digest,
        )

        with pytest.raises(RuntimeMessageTooLargeError):
            async with asyncio.timeout(2):
                await runtime.invoke(entry_id, {"text": "hello"})
    finally:
        await runtime.close()
        await runtime.cancel_pending_cleanup()


@pytest.mark.asyncio
async def test_real_fastmcp_invocation_observes_unsupported_encoding_signal() -> None:
    backend = RuntimeOverflowTransport("invocation_encoding")
    config = ServerStreamableHttpMcpServerConfig(
        name="encoding",
        transport="streamable_http",
        url="https://mcp.invalid/mcp",
        enabled_capabilities=[],
    )
    runtime = RuntimeGeneration(
        config,
        coordinator=ServerMcpCoordinator(),
        client_factory=_runtime_overflow_client_factory(backend),
        connect_timeout=2,
        discovery_timeout=2,
        cleanup_timeout=0.2,
    )

    try:
        source = await runtime.open()
        entry_id = UUID("01890f7c-bb80-7000-8000-000000000042")
        catalog = build_server_persisted_catalog(
            [config],
            SourceMcpCatalog(version=1, servers=[source]),
            entry_id_factory=lambda: entry_id,
        )
        runtime.bind_authority(
            catalog.servers[0],
            config_revision=2,
            catalog_digest=catalog.digest,
        )

        with pytest.raises(RuntimeTransportError):
            async with asyncio.timeout(2):
                await runtime.invoke(entry_id, {"text": "hello"})
        failure = runtime.transport_failure()
        assert failure.code == "config_validation_failed"
        assert failure.permanent is True
    finally:
        await runtime.close()
        await runtime.cancel_pending_cleanup()


@pytest.mark.asyncio
async def test_real_legacy_sse_transport_initializes_explicitly() -> None:
    backend = LegacySseTransport()
    transport = SSETransport(
        "https://mcp.invalid/sse",
        httpx_client_factory=partial(create_mcp_http_client, _transport=backend),
    )
    client = create_fastmcp_client(transport)

    async with client:
        tools = await client.session.list_tools()

    assert [tool.name for tool in tools.tools] == ["search"]


def test_runtime_client_builder_exposes_session_transport_and_close() -> None:
    client = build_runtime_client(
        ServerStreamableHttpMcpServerConfig(
            name="search",
            transport="streamable_http",
            url="https://mcp.invalid/mcp",
        )
    )

    assert isinstance(client.transport, StreamableHttpTransport)
    assert callable(client.close)
    assert hasattr(type(client), "session")
