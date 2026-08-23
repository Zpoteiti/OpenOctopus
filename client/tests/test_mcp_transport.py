from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp import types
from mcp.shared.exceptions import McpError

from openoctopus_client.mcp import runtime as runtime_module
from openoctopus_client.mcp import transport as transport_module
from openoctopus_client.mcp.models import (
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    StreamableHttpMcpServerConfig,
)
from openoctopus_client.mcp.runtime import McpRuntimeError, McpServerRuntime
from openoctopus_client.mcp.transport import (
    MCP_MESSAGE_BYTES_MAX,
    BoundedHttpTransport,
    BoundedStdioTransport,
    McpMessageTooLargeError,
    McpTransportClosingError,
    McpTransportError,
    McpTransportFailureSignal,
    UnsupportedMcpContentEncodingError,
    _spawn_stdio_process,
    _stdio_argv,
    _stdio_launch,
    _ThreadedStdioProcess,
    _windows_batch_arg,
    _windows_batch_command_line,
    _windows_batch_process_command_line,
    build_mcp_environment,
    create_fastmcp_client,
    create_mcp_http_client,
    install_mcp_log_discard_boundary,
)
from openoctopus_client.protocol import new_uuid7


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


@pytest.mark.parametrize(
    ("argument", "encoded"),
    [
        ("", '""'),
        ("plain", "plain"),
        ("two words", '"two words"'),
        ('say "hi"', '"say ""hi"""'),
        ("C:\\path\\", '"C:\\path\\\\"'),
        ('slash\\"quote', '"slash\\\\""quote"'),
        ("a&b|c<d>e^f", '"a&b|c<d>e^f"'),
        ("%PATH%", '"%%cd:~,%PATH%%cd:~,%"'),
        ("wow!", '"wow!"'),
    ],
)
def test_windows_batch_arguments_are_encoded_as_literal_values(
    argument: str,
    encoded: str,
) -> None:
    assert _windows_batch_arg(argument) == encoded


def test_windows_batch_command_line_uses_fixed_literal_wrapper() -> None:
    assert _windows_batch_command_line(
        r"C:\Program Files\MCP\server.cmd",
        ("", "two words", "a&b", "%PATH%", "wow!"),
    ) == (
        '""C:\\Program Files\\MCP\\server.cmd" "" "two words" "a&b" '
        '"%%cd:~,%PATH%%cd:~,%" "wow!""'
    )


def test_windows_batch_launcher_disables_delayed_expansion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolved = r"C:\Program Files\MCP\server.cmd"
    monkeypatch.setattr("openoctopus_client.mcp.transport.os.name", "nt")
    monkeypatch.setattr(
        "openoctopus_client.mcp.transport._resolve_windows_command",
        lambda *_args, **_kwargs: resolved,
    )
    monkeypatch.setattr(
        "openoctopus_client.mcp.transport.os.path.isfile", lambda _path: True
    )

    argv = _stdio_argv("server", ("wow!",), {"PATHEXT": ".CMD"})

    assert argv == (
        r"C:\Windows\System32\cmd.exe",
        "/E:ON",
        "/V:OFF",
        "/D",
        "/C",
        _windows_batch_command_line(resolved, ("wow!",)),
    )


def test_windows_batch_launcher_builds_one_raw_createprocess_command_line() -> None:
    comspec = r"C:\Windows\System32\cmd.exe"
    command = _windows_batch_command_line(
        r"C:\Program Files\MCP\server.cmd",
        ("", "two words", "a&b", "%PATH%", "wow!"),
    )
    assert _windows_batch_process_command_line(comspec, command) == (
        'C:\\Windows\\System32\\cmd.exe /E:ON /V:OFF /D /C '
        '""C:\\Program Files\\MCP\\server.cmd" "" "two words" "a&b" '
        '"%%cd:~,%PATH%%cd:~,%" "wow!""'
    )


@pytest.mark.asyncio
async def test_windows_batch_spawn_uses_raw_line_without_blocking_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    release = threading.Event()
    calls: list[tuple[object, dict[str, object]]] = []

    class SlowWriter(io.BytesIO):
        def write(self, payload: Any) -> int:
            started.set()
            if not release.wait(1):
                raise TimeoutError
            return super().write(payload)

    class FakePopen:
        def __init__(self) -> None:
            self.stdin = SlowWriter()
            self.stdout = io.BytesIO(b"response")
            self.pid = 123
            self._returncode: int | None = None

        def poll(self) -> int | None:
            return self._returncode

        def wait(self) -> int:
            self._returncode = 0
            return 0

        def kill(self) -> None:
            self._returncode = 1

    def popen(command: object, **kwargs: object) -> FakePopen:
        calls.append((command, kwargs))
        return FakePopen()

    resolved = r"C:\Program Files\MCP\server.cmd"
    monkeypatch.setattr("openoctopus_client.mcp.transport.os.name", "nt")
    monkeypatch.setattr(
        "openoctopus_client.mcp.transport._resolve_windows_command",
        lambda *_args, **_kwargs: resolved,
    )
    monkeypatch.setattr(
        "openoctopus_client.mcp.transport.os.path.isfile", lambda _path: True
    )
    monkeypatch.setattr("openoctopus_client.mcp.transport.subprocess.Popen", popen)
    launch = _stdio_launch("server", ("a&b",), {"PATHEXT": ".CMD"})

    process = await _spawn_stdio_process(
        launch,
        stderr_sink=io.BytesIO(),
        cwd=None,
        environment={},
    )
    assert isinstance(process, _ThreadedStdioProcess)
    assert calls == [
        (
            launch.raw_command_line,
            {
                "executable": launch.executable,
                "stdin": subprocess.PIPE,
                "stdout": subprocess.PIPE,
                "stderr": calls[0][1]["stderr"],
                "bufsize": 0,
                "cwd": None,
                "env": {},
                "creationflags": 0x00000200,
            },
        )
    ]
    assert process.stdin is not None
    process.stdin.write(b"payload")
    timer = threading.Timer(0.2, release.set)
    timer.start()
    drain_task = asyncio.create_task(process.stdin.drain())
    assert await asyncio.to_thread(started.wait, 1)
    await asyncio.sleep(0)
    assert not drain_task.done()
    release.set()
    await drain_task
    timer.cancel()
    timer.join()
    assert await process.stdout.read() == b"response"
    assert await process.stdout.read() == b""
    assert process.returncode is None
    assert await process.wait() == 0
    assert process.returncode == 0
    await process.close_pipes()
    assert process.stdin._stream.closed
    assert process.stdout._stream.closed


@pytest.mark.asyncio
async def test_threaded_stdio_process_does_not_depend_on_default_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    default_executor = ThreadPoolExecutor(max_workers=1)
    replacement_executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(default_executor)
    blocker_started = threading.Event()
    release_blocker = threading.Event()

    def occupy_default_executor() -> None:
        blocker_started.set()
        release_blocker.wait()

    blocker = loop.run_in_executor(None, occupy_default_executor)
    while not blocker_started.is_set():
        await asyncio.sleep(0)

    original_to_thread = asyncio.to_thread

    async def forbidden_to_thread(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("raw stdio must not use the default executor")

    monkeypatch.setattr(asyncio, "to_thread", forbidden_to_thread)
    raw_process = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-c",
            (
                "import sys;"
                "data=sys.stdin.buffer.readline();"
                "sys.stdout.buffer.write(b'got:'+data);"
                "sys.stdout.buffer.flush()"
            ),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=0,
    )
    process = _ThreadedStdioProcess(raw_process)
    try:
        assert process.stdin is not None
        assert process.stdout is not None
        read_task = asyncio.create_task(process.stdout.read(64 * 1024))
        await asyncio.sleep(0)
        process.stdin.write(b"ping\n")
        await asyncio.wait_for(process.stdin.drain(), 1)
        assert await asyncio.wait_for(read_task, 1) == b"got:ping\n"
        assert await asyncio.wait_for(process.wait(), 1) == 0
        await asyncio.wait_for(process.close_pipes(), 1)
        await process.close_pipes()
    finally:
        release_blocker.set()
        await blocker
        monkeypatch.setattr(asyncio, "to_thread", original_to_thread)
        if raw_process.poll() is None:
            raw_process.kill()
            raw_process.wait()
        with contextlib.suppress(Exception):
            await process.close_pipes()
        loop.set_default_executor(replacement_executor)
        default_executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_threaded_stdio_process_close_is_shared_and_cancellation_safe() -> None:
    close_started = threading.Event()
    release_close = threading.Event()

    class BlockingClose(io.BytesIO):
        def close(self) -> None:
            close_started.set()
            if not release_close.wait(1):
                raise TimeoutError
            super().close()

    class ExitedPopen:
        def __init__(self) -> None:
            self.stdin = BlockingClose()
            self.stdout = io.BytesIO()
            self.pid = 123

        def poll(self) -> int:
            return 0

        def wait(self) -> int:
            return 0

        def kill(self) -> None:
            raise AssertionError("exited process must not be killed")

    raw_process = ExitedPopen()
    process = _ThreadedStdioProcess(cast(Any, raw_process))
    first_close = asyncio.create_task(process.close_pipes())
    try:
        while not close_started.is_set():
            await asyncio.sleep(0)
        second_close = asyncio.create_task(process.close_pipes())
        first_close.cancel()
        await asyncio.sleep(0)
        assert not first_close.done()
        release_close.set()
        with pytest.raises(asyncio.CancelledError):
            await first_close
        await second_close
        await process.close_pipes()
        assert raw_process.stdin.closed
        assert raw_process.stdout.closed
    finally:
        release_close.set()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await first_close
        with contextlib.suppress(Exception):
            await process.close_pipes()


@pytest.mark.asyncio
async def test_stopped_raw_process_retires_io_after_job_assignment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExitedPopen:
        def __init__(self) -> None:
            self.stdin = io.BytesIO()
            self.stdout = io.BytesIO()
            self.pid = 123

        def poll(self) -> int:
            return 0

        def wait(self) -> int:
            return 0

        def kill(self) -> None:
            raise AssertionError("exited process must not be killed")

    raw_process = ExitedPopen()
    process = _ThreadedStdioProcess(cast(Any, raw_process))
    transport = BoundedStdioTransport(command=sys.executable)
    transport.process = process
    transport._job_assignment_failed = True

    async def tree_stopped(_timeout: float) -> bool:
        return True

    monkeypatch.setattr(transport, "_wait_for_tree", tree_stopped)
    existing_workers = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("oo-mcp-batch")
    }
    try:
        await transport.close()

        assert transport.cleanup_incomplete
        assert transport._cleanup_blocked
        assert raw_process.stdin.closed
        assert raw_process.stdout.closed
        assert {
            thread.ident
            for thread in threading.enumerate()
            if thread.name.startswith("oo-mcp-batch")
        } <= existing_workers

        await transport.close()
        assert transport.cleanup_incomplete
        assert raw_process.stdin.closed
        assert raw_process.stdout.closed
    finally:
        transport._job_assignment_failed = False
        with contextlib.suppress(Exception):
            await transport.close()


@pytest.mark.skipif(os.name != "nt", reason="requires native cmd.exe")
def test_windows_batch_launcher_preserves_literal_arguments(tmp_path: Path) -> None:
    capture = tmp_path / "capture.py"
    capture.write_text(
        "import json, sys\nprint(json.dumps(sys.argv[1:]))\n",
        encoding="utf-8",
    )
    launcher = tmp_path / "capture.cmd"
    launcher.write_text(
        '@echo off\n"%OO_TEST_PYTHON%" "%OO_TEST_CAPTURE%" %*\n',
        encoding="utf-8",
    )
    arguments = (
        "",
        "two words",
        'say "hi"',
        "trailing\\",
        'slash\\"quote',
        "a&b",
        "a|b",
        "a<b",
        "a>b",
        "a^b",
        "%PATH%",
        "wow!",
    )
    environment = {
        **os.environ,
        "OO_TEST_PYTHON": sys.executable,
        "OO_TEST_CAPTURE": str(capture),
    }

    launch = _stdio_launch(str(launcher), arguments, environment)
    assert launch.raw_command_line is not None and launch.executable is not None
    completed = subprocess.run(
        launch.raw_command_line,
        executable=launch.executable,
        cwd=tmp_path,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == list(arguments)


@pytest.mark.parametrize("argument", ["line\rbreak", "line\nbreak"])
def test_windows_batch_arguments_reject_command_separating_newlines(argument: str) -> None:
    with pytest.raises(ValueError, match="CR or LF"):
        _windows_batch_arg(argument)


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


class _RaisingCloseStream(_ChunkStream):
    async def aclose(self) -> None:
        raise RuntimeError("close failed")


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


class _RaisingCloseEncodingTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=_RaisingCloseStream([b"not-read"]),
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


class _RuntimeOverflowTransport(httpx.AsyncBaseTransport):
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
        else:  # pragma: no cover - reports unexpected SDK traffic
            raise AssertionError(f"unexpected MCP request: {method}")
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
        return httpx.Response(200, headers=headers, stream=_ChunkStream([body]))


def _runtime_overflow_client_factory(backend: httpx.AsyncBaseTransport) -> Any:
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


def _runtime_overflow_config() -> StreamableHttpMcpServerConfig:
    return StreamableHttpMcpServerConfig(
        name="overflow",
        transport="streamable_http",
        url="https://mcp.invalid/mcp",
        headers={},
        enabled_capabilities=[],
    )


def _real_overflow_runtime(
    monkeypatch: pytest.MonkeyPatch,
    backend: httpx.AsyncBaseTransport,
) -> McpServerRuntime:
    monkeypatch.setattr(
        runtime_module,
        "build_runtime_client",
        _runtime_overflow_client_factory(backend),
    )
    return McpServerRuntime(
        _runtime_overflow_config(),
        connect_timeout=2,
        discovery_timeout=2,
        invocation_timeout=2,
        cleanup_timeout=0.2,
    )


async def _bind_runtime_overflow_tool(
    runtime: McpServerRuntime,
) -> UUID:
    source = await runtime.start()
    assert len(source.tools) == 1
    tool = source.tools[0]
    entry_id = new_uuid7()
    runtime.bind_persisted(
        PersistedMcpServerCatalog(
            name="overflow",
            entries=[
                PersistedMcpCatalogEntry(
                    entry_id=entry_id,
                    server="overflow",
                    surface="tool",
                    raw_name=tool.raw_name,
                    invocation_identity=tool.raw_name,
                    final_name="mcp_overflow_echo",
                    provider_description=tool.description or "Echo text.",
                    input_schema=tool.input_schema,
                    output_schema=tool.output_schema,
                    enabled=True,
                )
            ],
        )
    )
    runtime.mark_ready(runtime.generation)
    return entry_id


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
    failure_signal = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            overflow,
            transport_failure_signal=failure_signal,
        )
    ) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid/messages")
    assert failure_signal.kind == "message_too_large"


@pytest.mark.asyncio
async def test_http_rejects_length_and_encoding_before_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport.MCP_MESSAGE_BYTES_MAX", 8)
    too_long = _ResponseTransport([b"not-read"], headers={"Content-Length": "9"})
    length_failure = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            too_long,
            transport_failure_signal=length_failure,
        )
    ) as client:
        with pytest.raises(McpMessageTooLargeError):
            await client.get("https://mcp.invalid/messages")
    assert length_failure.kind == "message_too_large"

    compressed = _ResponseTransport([b"not-read"], headers={"Content-Encoding": "gzip"})
    encoding_failure = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            compressed,
            transport_failure_signal=encoding_failure,
        )
    ) as client:
        with pytest.raises(UnsupportedMcpContentEncodingError):
            await client.get("https://mcp.invalid/messages")
    assert encoding_failure.kind == "unsupported_content_encoding"

    close_failure = McpTransportFailureSignal()
    async with httpx.AsyncClient(
        transport=BoundedHttpTransport(
            _RaisingCloseEncodingTransport(),
            transport_failure_signal=close_failure,
        )
    ) as client:
        with pytest.raises(UnsupportedMcpContentEncodingError):
            await client.get("https://mcp.invalid/messages")
    assert close_failure.kind == "unsupported_content_encoding"


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
async def test_real_fastmcp_initialize_overflow_keeps_specific_failure_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _real_overflow_runtime(
        monkeypatch,
        _RuntimeOverflowTransport("initialize"),
    )
    monkeypatch.setattr(transport_module, "MCP_MESSAGE_BYTES_MAX", 64)

    try:
        with pytest.raises(McpRuntimeError) as captured:
            async with asyncio.timeout(2):
                await runtime.start()

        assert captured.value.failure.code == "mcp_message_too_large"
        assert captured.value.failure.stage == "connect"
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_real_fastmcp_discovery_overflow_keeps_specific_failure_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _real_overflow_runtime(
        monkeypatch,
        _RuntimeOverflowTransport("discovery"),
    )
    monkeypatch.setattr(transport_module, "MCP_MESSAGE_BYTES_MAX", 1_024)

    try:
        with pytest.raises(McpRuntimeError) as captured:
            async with asyncio.timeout(2):
                await runtime.start()

        assert captured.value.failure.code == "mcp_message_too_large"
        assert captured.value.failure.stage == "discovery"
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_real_fastmcp_unsupported_encoding_is_permanent_during_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _real_overflow_runtime(
        monkeypatch,
        _RuntimeOverflowTransport("encoding"),
    )

    try:
        with pytest.raises(McpRuntimeError) as captured:
            async with asyncio.timeout(2):
                await runtime.start()

        assert captured.value.failure.code == "config_validation_failed"
        assert captured.value.failure.stage == "connect"
        assert runtime.permanent_failure is True
        assert runtime.enter_backoff(jitter=0.5) is None
    finally:
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("include_content_length", [True, False])
async def test_real_fastmcp_invocation_overflow_keeps_specific_tool_code(
    monkeypatch: pytest.MonkeyPatch,
    include_content_length: bool,
) -> None:
    runtime = _real_overflow_runtime(
        monkeypatch,
        _RuntimeOverflowTransport(
            "invocation",
            include_content_length=include_content_length,
        ),
    )
    monkeypatch.setattr(transport_module, "MCP_MESSAGE_BYTES_MAX", 1_024)

    try:
        entry_id = await _bind_runtime_overflow_tool(runtime)
        async with asyncio.timeout(2):
            output = await runtime.invoke(
                entry_id,
                {"text": "hello"},
                runtime_generation=runtime.generation,
                request_id=new_uuid7(),
                max_result_bytes=4_096,
            )

        assert output.code == "tool_mcp_message_too_large"
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_real_fastmcp_invocation_encoding_failure_stays_suspended(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _real_overflow_runtime(
        monkeypatch,
        _RuntimeOverflowTransport("invocation_encoding"),
    )

    try:
        entry_id = await _bind_runtime_overflow_tool(runtime)
        async with asyncio.timeout(2):
            output = await runtime.invoke(
                entry_id,
                {"text": "hello"},
                runtime_generation=runtime.generation,
                request_id=new_uuid7(),
                max_result_bytes=4_096,
            )

        assert output.code == "tool_execution_outcome_unknown"
        assert runtime.permanent_failure is True
        assert runtime.code == "config_validation_failed"
        assert runtime.enter_backoff(jitter=0.5) is None
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_cancelled_idle_waiter_does_not_lose_transport_overflow_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _real_overflow_runtime(
        monkeypatch,
        _RuntimeOverflowTransport("idle"),
    )
    monkeypatch.setattr(transport_module, "MCP_MESSAGE_BYTES_MAX", 1_024)

    try:
        await _bind_runtime_overflow_tool(runtime)
        cancelled_waiter = asyncio.create_task(runtime.next_event())
        await asyncio.sleep(0)
        cancelled_waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_waiter

        signal = getattr(runtime, "_transport_failure_signal")
        signal.report("message_too_large")
        async with asyncio.timeout(2):
            event = await runtime.next_event()
        assert event.kind == "transport_failed"

        await runtime.mark_transport_unavailable()
        assert runtime.code == "tool_mcp_message_too_large"
        assert runtime.enter_backoff(jitter=0.5) == 1
        runtime.begin_retry()
        replacement_signal = getattr(runtime, "_transport_failure_signal")
        assert replacement_signal is not signal
        assert replacement_signal.kind is None
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_real_legacy_sse_transport_initializes_explicitly() -> None:
    fake = _SseMcpTransport()
    effective_timeouts: list[httpx.Timeout] = []

    def http_client_factory(
        headers: dict[str, str] | None = None,
        timeout: httpx.Timeout | None = None,
        auth: httpx.Auth | None = None,
        **kwargs: Any,
    ) -> httpx.AsyncClient:
        client = create_mcp_http_client(
            headers=headers,
            timeout=timeout,
            auth=auth,
            _transport=fake,
            **kwargs,
        )
        effective_timeouts.append(client.timeout)
        return client

    transport = SSETransport(
        "https://mcp.invalid/sse",
        headers={"X-MCP-Test": "present"},
        httpx_client_factory=http_client_factory,
    )
    client = create_fastmcp_client(transport)

    async with client:
        tools = await client.session.list_tools()

    assert [tool.name for tool in tools.tools] == ["remote_tool"]
    assert any(request.method == "GET" for request in fake.requests)
    assert any(request.method == "POST" for request in fake.requests)
    assert all(request.headers["accept-encoding"] == "identity" for request in fake.requests)
    assert effective_timeouts
    assert all(
        timeout.connect is None
        and timeout.read is None
        and timeout.write is None
        and timeout.pool is None
        for timeout in effective_timeouts
    )


def test_stdio_relative_command_resolves_from_configured_cwd(tmp_path: Path) -> None:
    configured_cwd = tmp_path / "configured"
    configured_cwd.mkdir()
    if os.name == "nt":
        executable = configured_cwd / "mcp-local.cmd"
        executable.write_text("@exit /b 0\n", encoding="utf-8")
        command = r".\mcp-local"
        environment = {"PATH": "", "PATHEXT": ".CMD"}
    else:
        executable = configured_cwd / "mcp-local"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o700)
        command = "./mcp-local"
        environment = {"PATH": ""}

    argv = _stdio_argv(command, (), environment, cwd=configured_cwd)

    assert str(executable.resolve()).casefold() in " ".join(argv).casefold()


def test_stdio_bare_command_resolves_relative_path_from_configured_cwd(
    tmp_path: Path,
) -> None:
    configured_cwd = tmp_path / "configured"
    executable_dir = configured_cwd / "bin"
    executable_dir.mkdir(parents=True)
    if os.name == "nt":
        executable = executable_dir / "mcp-local.cmd"
        executable.write_text("@exit /b 0\n", encoding="utf-8")
        environment = {"PATH": "bin", "PATHEXT": ".CMD"}
    else:
        executable = executable_dir / "mcp-local"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o700)
        environment = {"PATH": "bin"}

    argv = _stdio_argv("mcp-local", (), environment, cwd=configured_cwd)

    assert str(executable.resolve()).casefold() in " ".join(argv).casefold()


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
    failure_signal = McpTransportFailureSignal()
    transport = BoundedStdioTransport(
        command=sys.executable,
        args=("-c", code),
        transport_failure_signal=failure_signal,
    )
    client = create_fastmcp_client(transport)

    with pytest.raises(McpError, match="Connection closed"):
        async with client:
            pass
    assert isinstance(transport.terminal_error, McpMessageTooLargeError)
    assert failure_signal.kind == "message_too_large"
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
async def test_stdio_start_failure_after_spawn_reaps_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openoctopus_client.mcp.transport._STDIN_EOF_SECONDS", 0.01)
    monkeypatch.setattr("openoctopus_client.mcp.transport._TERMINATE_SECONDS", 0.01)
    monkeypatch.setattr("openoctopus_client.mcp.transport._FORCE_KILL_SECONDS", 0.2)

    class FailMemoryStream:
        def __class_getitem__(cls, item: object) -> type[FailMemoryStream]:
            del item
            return cls

        def __new__(cls, *args: object, **kwargs: object) -> FailMemoryStream:
            del args, kwargs
            raise RuntimeError("post-spawn setup failed")

    monkeypatch.setattr(
        "openoctopus_client.mcp.transport.anyio.create_memory_object_stream",
        FailMemoryStream,
    )
    transport = BoundedStdioTransport(
        command=sys.executable,
        args=("-c", "import time; time.sleep(60)"),
    )

    with pytest.raises(RuntimeError, match="post-spawn setup failed"):
        await transport._start()
    assert transport.process is not None
    wait_task = asyncio.create_task(transport.process.wait())
    try:
        done, _pending = await asyncio.wait({wait_task}, timeout=0.5)
        assert wait_task in done
    finally:
        await transport.close()
        await wait_task


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
