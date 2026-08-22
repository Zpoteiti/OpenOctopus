from __future__ import annotations

import asyncio
import json
import logging
import sys
from functools import partial
from pathlib import Path
from typing import cast

import httpx
import pytest
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp import types
from mcp.shared.exceptions import McpError

from openoctopus_client.mcp.transport import (
    MCP_MESSAGE_BYTES_MAX,
    BoundedHttpTransport,
    BoundedStdioTransport,
    McpMessageTooLargeError,
    McpTransportClosingError,
    McpTransportError,
    UnsupportedMcpContentEncodingError,
    build_mcp_environment,
    create_fastmcp_client,
    create_mcp_http_client,
    install_mcp_log_discard_boundary,
)


def test_stdio_environment_uses_safe_baseline_and_redacts_client_secrets() -> None:
    parent = {
        "HOME": "/safe-home",
        "PATH": "/safe-bin",
        "UNRELATED": "must-not-pass",
        "OPENOCTOPUS_DEVICE_TOKEN": "parent-secret",
    }

    result = build_mcp_environment(
        parent,
        {
            "CUSTOM": "allowed",
            "HOME": "/candidate-home",
            "OpenOctopus_Injected": "candidate-secret",
        },
        windows=False,
    )

    assert result == {
        "HOME": "/candidate-home",
        "PATH": "/safe-bin",
        "CUSTOM": "allowed",
    }


def test_stdio_environment_windows_overlay_is_case_insensitive() -> None:
    result = build_mcp_environment(
        {"Path": r"C:\Windows", "PATHEXT": ".EXE;.CMD", "USERNAME": "alice"},
        {"PATH": r"C:\Tools", "username": "bob", "OPENOCTOPUS_X": "secret"},
        windows=True,
    )

    assert result == {"PATH": r"C:\Tools", "PATHEXT": ".EXE;.CMD", "username": "bob"}


def test_mcp_loggers_do_not_propagate_payloads(caplog: pytest.LogCaptureFixture) -> None:
    sentinel = "mcp-secret-sentinel"
    install_mcp_log_discard_boundary()
    with caplog.at_level(1):
        logging.getLogger("fastmcp.client.transport").log(1, sentinel)
        logging.getLogger("mcp.client.sse").critical(sentinel)
        logging.getLogger("httpx").warning(sentinel)
        logging.getLogger("httpcore.connection").error(sentinel)
        logging.getLogger("openoctopus_client.keep").warning("application-visible")

    assert sentinel not in caplog.text
    assert "application-visible" in caplog.text


class _ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _ResponseTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        chunks: list[bytes],
        *,
        headers: dict[str, str] | None = None,
        status_code: int = 200,
    ) -> None:
        self.chunks = chunks
        self.headers = headers or {}
        self.status_code = status_code
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(
            self.status_code,
            headers=self.headers,
            stream=_ChunkStream(self.chunks),
        )


def _json_response(payload: object, *, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers={"Content-Type": "application/json"},
        stream=_ChunkStream([json.dumps(payload).encode()]),
    )


def _mcp_response(request: dict[str, object]) -> dict[str, object]:
    request_id = request["id"]
    method = request["method"]
    if method == "initialize":
        params = cast(dict[str, object], request["params"])
        result: dict[str, object] = {
            "protocolVersion": params["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "remote-fake", "version": "1"},
        }
    elif method == "tools/list":
        result = {
            "tools": [
                {
                    "name": "remote_tool",
                    "description": "remote fake",
                    "inputSchema": {"type": "object"},
                }
            ]
        }
    else:  # pragma: no cover - the assertion reports an unexpected SDK request
        raise AssertionError(f"unexpected MCP request: {method}")
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


class _StreamableMcpTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        payload = json.loads(await request.aread())
        if "id" not in payload:
            return _json_response({}, status_code=202)
        return _json_response(_mcp_response(payload))


class _QueueSseStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.closed = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        while (chunk := await self.queue.get()) is not None:
            yield chunk

    async def aclose(self) -> None:
        if not self.closed:
            self.closed = True
            self.queue.put_nowait(None)


class _SseMcpTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.stream = _QueueSseStream()
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.method == "GET":
            self.stream.queue.put_nowait(
                b"event: endpoint\ndata: https://mcp.invalid/messages\n\n"
            )
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=self.stream,
            )
        payload = json.loads(await request.aread())
        if "id" in payload:
            response = json.dumps(_mcp_response(payload), separators=(",", ":")).encode()
            self.stream.queue.put_nowait(b"event: message\ndata: " + response + b"\n\n")
        return _json_response({}, status_code=202)


@pytest.mark.asyncio
async def test_http_entity_limit_is_enforced_before_decode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport.MCP_MESSAGE_BYTES_MAX", 8)
    inner = _ResponseTransport([b"1234", b"5678"])
    async with httpx.AsyncClient(transport=BoundedHttpTransport(inner)) as client:
        response = await client.get("https://mcp.invalid/messages")
        assert await response.aread() == b"12345678"

    overflow = _ResponseTransport([b"1234", b"56789"])
    async with httpx.AsyncClient(transport=BoundedHttpTransport(overflow)) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid/messages")


@pytest.mark.asyncio
async def test_http_rejects_length_and_encoding_before_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport.MCP_MESSAGE_BYTES_MAX", 8)
    too_long = _ResponseTransport([b"not-read"], headers={"Content-Length": "9"})
    async with httpx.AsyncClient(transport=BoundedHttpTransport(too_long)) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid/messages")

    compressed = _ResponseTransport([b"not-read"], headers={"Content-Encoding": "gzip"})
    async with httpx.AsyncClient(transport=BoundedHttpTransport(compressed)) as client:
        with pytest.raises(UnsupportedMcpContentEncodingError):
            await client.get("https://mcp.invalid/messages")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chunks",
    [
        [b"data:1234\n", b"data:5\n\n"],
        [b"id:1234\r\nid:5\r", b"\n\r\n"],
        [b":1234\r:5678\r\r"],
    ],
)
async def test_sse_limit_counts_each_raw_event_across_delimiters(
    monkeypatch: pytest.MonkeyPatch,
    chunks: list[bytes],
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport.MCP_MESSAGE_BYTES_MAX", 12)
    inner = _ResponseTransport(chunks, headers={"Content-Type": "text/event-stream"})
    async with httpx.AsyncClient(transport=BoundedHttpTransport(inner)) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid/sse")


@pytest.mark.asyncio
async def test_sse_limit_resets_after_each_complete_event(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport.MCP_MESSAGE_BYTES_MAX", 10)
    inner = _ResponseTransport(
        [b"data:x\n\n", b"data:y\r\n\r\n", b"data:z\r\r"],
        headers={"Content-Type": "text/event-stream"},
    )
    async with httpx.AsyncClient(transport=BoundedHttpTransport(inner)) as client:
        response = await client.get("https://mcp.invalid/sse")
        assert await response.aread() == b"data:x\n\ndata:y\r\n\r\ndata:z\r\r"


@pytest.mark.asyncio
async def test_sse_content_length_may_exceed_per_event_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport.MCP_MESSAGE_BYTES_MAX", 9)
    body = b"data:x\n\ndata:y\n\n"
    inner = _ResponseTransport(
        [body],
        headers={
            "Content-Type": "text/event-stream",
            "Content-Length": str(len(body)),
        },
    )
    async with httpx.AsyncClient(transport=BoundedHttpTransport(inner)) as client:
        response = await client.get("https://mcp.invalid/sse")
        assert await response.aread() == body


@pytest.mark.asyncio
async def test_http_client_factory_forces_identity_and_does_not_follow_redirects() -> None:
    inner = _ResponseTransport(
        [], headers={"Location": "https://mcp.invalid/redirected"}, status_code=307
    )
    client = create_mcp_http_client(
        headers={"Authorization": "Bearer secret", "Accept-Encoding": "gzip"},
        follow_redirects=True,
        _transport=inner,
    )
    async with client:
        response = await client.get("https://mcp.invalid/start")

    assert response.status_code == 307
    assert len(inner.requests) == 1
    assert inner.requests[0].headers["accept-encoding"] == "identity"


@pytest.mark.asyncio
async def test_real_streamable_http_transport_initializes_explicitly() -> None:
    fake = _StreamableMcpTransport()
    transport = StreamableHttpTransport(
        "https://mcp.invalid/mcp",
        headers={"X-MCP-Test": "present"},
        httpx_client_factory=partial(create_mcp_http_client, _transport=fake),
    )
    client = create_fastmcp_client(transport)

    async with client:
        tools = await client.session.list_tools()

    assert [tool.name for tool in tools.tools] == ["remote_tool"]
    assert fake.requests
    assert all(request.headers["accept-encoding"] == "identity" for request in fake.requests)
    assert all(request.headers["x-mcp-test"] == "present" for request in fake.requests)


@pytest.mark.asyncio
async def test_real_legacy_sse_transport_initializes_explicitly() -> None:
    fake = _SseMcpTransport()
    transport = SSETransport(
        "https://mcp.invalid/sse",
        headers={"X-MCP-Test": "present"},
        httpx_client_factory=partial(create_mcp_http_client, _transport=fake),
    )
    client = create_fastmcp_client(transport)

    async with client:
        tools = await client.session.list_tools()

    assert [tool.name for tool in tools.tools] == ["remote_tool"]
    assert any(request.method == "GET" for request in fake.requests)
    assert any(request.method == "POST" for request in fake.requests)
    assert all(request.headers["accept-encoding"] == "identity" for request in fake.requests)


@pytest.mark.asyncio
async def test_legacy_sse_clean_eof_reports_idle_transport_failure() -> None:
    fake = _SseMcpTransport()
    messages: asyncio.Queue[object] = asyncio.Queue()

    async def handler(message: object) -> None:
        messages.put_nowait(message)

    transport = SSETransport(
        "https://mcp.invalid/sse",
        httpx_client_factory=partial(create_mcp_http_client, _transport=fake),
    )
    client = create_fastmcp_client(transport, message_handler=handler)

    async with client:
        await client.session.list_tools()
        fake.stream.queue.put_nowait(None)
        message = await asyncio.wait_for(messages.get(), timeout=2)
        assert isinstance(message, McpTransportError)


@pytest.mark.asyncio
async def test_real_stdio_initialize_and_raw_session_apis() -> None:
    fixture = Path(__file__).parent / "fixtures" / "fake_mcp_stdio.py"
    transport = BoundedStdioTransport(
        command=sys.executable,
        args=(str(fixture),),
        env={"MCP_SENTINEL": "visible", "OPENOCTOPUS_SECRET": "must-not-pass"},
    )
    client = create_fastmcp_client(transport)

    async with client:
        assert [tool.name for tool in (await client.session.list_tools()).tools] == ["environment"]
        assert (await client.session.list_resources()).resources == []
        assert (await client.session.list_resource_templates()).resourceTemplates == []
        assert (await client.session.list_prompts()).prompts == []
        result = await client.session.send_request(
            types.ClientRequest(
                types.CallToolRequest(
                    params=types.CallToolRequestParams(
                        name="environment",
                        arguments={"keys": ["MCP_SENTINEL", "OPENOCTOPUS_SECRET"]},
                    )
                )
            ),
            types.CallToolResult,
        )

    content = cast(types.TextContent, result.content[0])
    assert json.loads(content.text) == {
        "MCP_SENTINEL": "visible",
        "OPENOCTOPUS_SECRET": None,
    }
    assert result.structuredContent == {"unexpected": True}
    assert transport.cleanup_incomplete is False
    assert transport.terminal_error is None
    assert transport.process is not None and transport.process.returncode is not None


@pytest.mark.asyncio
async def test_clean_stdio_eof_reports_idle_failure_but_normal_close_does_not() -> None:
    code = """
import json
import sys

request = json.loads(sys.stdin.readline())
response = {
    "jsonrpc": "2.0",
    "id": request["id"],
    "result": {
        "protocolVersion": request["params"]["protocolVersion"],
        "capabilities": {},
        "serverInfo": {"name": "exit-after-init", "version": "1"},
    },
}
sys.stdout.write(json.dumps(response) + "\\n")
sys.stdout.flush()
sys.stdin.readline()
"""
    messages: asyncio.Queue[object] = asyncio.Queue()

    async def handler(message: object) -> None:
        messages.put_nowait(message)

    transport = BoundedStdioTransport(command=sys.executable, args=("-c", code))
    client = create_fastmcp_client(transport, message_handler=handler)

    async with client:
        message = await asyncio.wait_for(messages.get(), timeout=2)
        assert isinstance(message, McpTransportError)
    await asyncio.sleep(0)

    assert messages.empty()
    assert isinstance(transport.terminal_error, McpTransportError)


@pytest.mark.asyncio
async def test_stdio_limit_rejects_oversize_record_before_lf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport.MCP_MESSAGE_BYTES_MAX", 8)
    code = (
        "import sys;"
        "sys.stdout.buffer.write(b'x'*9);"
        "sys.stdout.buffer.flush();"
        "sys.stdin.buffer.read()"
    )
    transport = BoundedStdioTransport(command=sys.executable, args=("-c", code))
    client = create_fastmcp_client(transport)

    with pytest.raises(McpError, match="Connection closed"):
        async with client:
            pass
    assert isinstance(transport.terminal_error, McpMessageTooLargeError)
    assert transport.process is not None and transport.process.returncode is not None


@pytest.mark.asyncio
async def test_stdio_close_is_cancellation_shielded_and_force_kills(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport._STDIN_EOF_SECONDS", 0.02)
    monkeypatch.setattr("openoctopus_client.mcp.transport._TERMINATE_SECONDS", 0.02)
    monkeypatch.setattr("openoctopus_client.mcp.transport._FORCE_KILL_SECONDS", 0.2)
    if sys.platform == "win32":
        code = "import time; time.sleep(60)"
    else:
        code = (
            "import signal,time;"
            "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
            "time.sleep(60)"
        )
    transport = BoundedStdioTransport(command=sys.executable, args=("-c", code))
    await transport._start()
    close_task = asyncio.create_task(transport.close())
    await asyncio.sleep(0)
    close_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await close_task
    assert transport.cleanup_incomplete is False
    assert transport.process is not None and transport.process.returncode is not None
    await transport.close()


@pytest.mark.asyncio
async def test_incomplete_stdio_cleanup_blocks_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport._STDIN_EOF_SECONDS", 0.01)
    monkeypatch.setattr("openoctopus_client.mcp.transport._TERMINATE_SECONDS", 0.01)
    monkeypatch.setattr("openoctopus_client.mcp.transport._FORCE_KILL_SECONDS", 0.01)
    transport = BoundedStdioTransport(
        command=sys.executable, args=("-c", "import time; time.sleep(60)")
    )
    await transport._start()

    async def never_converged() -> bool:
        return False

    monkeypatch.setattr(transport, "_tree_converged", never_converged)
    await transport.close()

    assert transport.cleanup_incomplete is True
    with pytest.raises(McpTransportClosingError):
        await transport._start()


@pytest.mark.asyncio
async def test_second_stdio_close_retries_incomplete_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = BoundedStdioTransport(command=sys.executable, args=("-c", "pass"))
    cleanup_calls = 0

    async def cleanup() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        transport.cleanup_incomplete = cleanup_calls == 1
        transport._cleanup_blocked = transport.cleanup_incomplete

    monkeypatch.setattr(transport, "_cleanup", cleanup)

    await transport.close()
    assert transport.cleanup_incomplete
    await transport.close()

    assert cleanup_calls == 2
    assert not transport.cleanup_incomplete


def test_fastmcp_client_has_disabled_sdk_timeouts() -> None:
    transport = BoundedStdioTransport(command=sys.executable, args=("-c", "pass"))
    client = create_fastmcp_client(transport)

    assert client._init_timeout is None
    assert client._session_kwargs["read_timeout_seconds"] is None
    assert MCP_MESSAGE_BYTES_MAX == 12 * 1024 * 1024
    assert isinstance(StreamableHttpTransport("https://example.invalid"), StreamableHttpTransport)
    assert isinstance(SSETransport("https://example.invalid"), SSETransport)
