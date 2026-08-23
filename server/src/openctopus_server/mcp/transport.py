"""Bounded Linux FastMCP client transports owned by the Server."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import signal
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import IO, Any, Literal, Protocol, Unpack, cast

import anyio
import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from fastmcp import Client
from fastmcp.client.transports import ClientTransport, SSETransport, StreamableHttpTransport
from fastmcp.client.transports.base import SessionKwargs
from mcp import ClientSession, types
from mcp.client.session import MessageHandlerFnT
from mcp.shared.message import SessionMessage
from pydantic import AnyUrl

from openctopus_server.mcp.models import (
    ServerMcpServerConfig,
    ServerSseMcpServerConfig,
    ServerStdioMcpServerConfig,
    ServerStreamableHttpMcpServerConfig,
)

MCP_MESSAGE_BYTES_MAX = 12 * 1024 * 1024
_STDIN_EOF_SECONDS = 2.0
_TERMINATE_SECONDS = 3.0
_FORCE_KILL_SECONDS = 5.0
_READ_CHUNK_BYTES = 64 * 1024
_POSIX_SAFE_ENV = ("HOME", "LOGNAME", "PATH", "SHELL", "TERM", "USER")
_DISCARDED_LOGGER_ROOTS = ("fastmcp", "mcp", "httpx", "httpcore")


class McpTransportError(RuntimeError):
    """Stable base class for MCP transport boundary failures."""


class McpMessageTooLargeError(McpTransportError):
    """An inbound MCP message exceeded the pre-decode byte limit."""


class UnsupportedMcpContentEncodingError(McpTransportError):
    """A remote MCP response attempted content encoding."""


class McpTransportClosingError(McpTransportError):
    """A stdio transport cannot accept another connection."""


type McpTransportFailureKind = Literal[
    "message_too_large",
    "unsupported_content_encoding",
]


class McpTransportFailureSignal:
    """Publish the first payload-free terminal transport failure."""

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._kind: McpTransportFailureKind | None = None

    @property
    def kind(self) -> McpTransportFailureKind | None:
        return self._kind

    def report(self, kind: McpTransportFailureKind) -> None:
        if self._kind is None:
            self._kind = kind
            self._event.set()

    async def wait(self) -> McpTransportFailureKind:
        await self._event.wait()
        assert self._kind is not None
        return self._kind


def _message_limit_error(
    message: str,
    failure_signal: McpTransportFailureSignal | None,
) -> McpMessageTooLargeError:
    if failure_signal is not None:
        failure_signal.report("message_too_large")
    return McpMessageTooLargeError(message)


def _unsupported_content_encoding_error(
    failure_signal: McpTransportFailureSignal | None,
) -> UnsupportedMcpContentEncodingError:
    if failure_signal is not None:
        failure_signal.report("unsupported_content_encoding")
    return UnsupportedMcpContentEncodingError(
        "MCP responses must use identity content encoding"
    )


def _is_server_secret(name: str) -> bool:
    return name.casefold().startswith("openoctopus_")


def build_mcp_environment(
    parent: Mapping[str, str],
    overlay: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return only the POSIX safe baseline plus the configured overlay."""

    result = {
        name: value
        for name in _POSIX_SAFE_ENV
        if (value := parent.get(name)) is not None
        and not value.startswith("()")
        and not _is_server_secret(name)
    }
    for name, value in (overlay or {}).items():
        if not _is_server_secret(name):
            result[name] = value
    return {name: value for name, value in result.items() if not _is_server_secret(name)}


class _DiscardHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        del record


def install_mcp_log_discard_boundary() -> None:
    """Drop third-party payload-bearing records before Server handlers."""

    names = set(_DISCARDED_LOGGER_ROOTS)
    for name in tuple(logging.root.manager.loggerDict):
        if any(name == root or name.startswith(root + ".") for root in names):
            names.add(name)
    for name in names:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.addHandler(_DiscardHandler())
        logger.propagate = False


class _BoundedEntityStream(httpx.AsyncByteStream):
    def __init__(
        self,
        inner: httpx.AsyncByteStream,
        transport_failure_signal: McpTransportFailureSignal | None,
    ) -> None:
        self._inner = inner
        self._transport_failure_signal = transport_failure_signal
        self._message_limit_exceeded = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        size = 0
        async for chunk in self._inner:
            size += len(chunk)
            if size > MCP_MESSAGE_BYTES_MAX:
                self._message_limit_exceeded = True
                error = _message_limit_error(
                    "MCP HTTP response exceeds the raw byte limit",
                    self._transport_failure_signal,
                )
                with contextlib.suppress(BaseException):
                    await self._inner.aclose()
                raise error
            yield chunk

    async def aclose(self) -> None:
        if self._message_limit_exceeded:
            with contextlib.suppress(BaseException):
                await self._inner.aclose()
            return
        await self._inner.aclose()


class _BoundedSseStream(httpx.AsyncByteStream):
    def __init__(
        self,
        inner: httpx.AsyncByteStream,
        *,
        report_eof: bool,
        transport_failure_signal: McpTransportFailureSignal | None,
    ) -> None:
        self._inner = inner
        self._report_eof = report_eof
        self._transport_failure_signal = transport_failure_signal
        self._message_limit_exceeded = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        event_size = 0
        line_has_content = False
        pending_cr = False
        reset_after_cr = False
        async for chunk in self._inner:
            for byte in chunk:
                if pending_cr:
                    if byte == 0x0A:
                        event_size += 1
                        if event_size > MCP_MESSAGE_BYTES_MAX:
                            self._message_limit_exceeded = True
                            error = _message_limit_error(
                                "MCP SSE event exceeds the raw byte limit",
                                self._transport_failure_signal,
                            )
                            with contextlib.suppress(BaseException):
                                await self._inner.aclose()
                            raise error
                        if reset_after_cr:
                            event_size = 0
                        pending_cr = False
                        reset_after_cr = False
                        continue
                    if reset_after_cr:
                        event_size = 0
                    pending_cr = False
                    reset_after_cr = False
                event_size += 1
                if event_size > MCP_MESSAGE_BYTES_MAX:
                    self._message_limit_exceeded = True
                    error = _message_limit_error(
                        "MCP SSE event exceeds the raw byte limit",
                        self._transport_failure_signal,
                    )
                    with contextlib.suppress(BaseException):
                        await self._inner.aclose()
                    raise error
                if byte == 0x0D:
                    pending_cr = True
                    reset_after_cr = not line_has_content
                    line_has_content = False
                elif byte == 0x0A:
                    if not line_has_content:
                        event_size = 0
                    line_has_content = False
                else:
                    line_has_content = True
            yield chunk
        if self._report_eof:
            raise McpTransportError("MCP SSE stream ended unexpectedly")

    async def aclose(self) -> None:
        if self._message_limit_exceeded:
            with contextlib.suppress(BaseException):
                await self._inner.aclose()
            return
        await self._inner.aclose()


class BoundedHttpTransport(httpx.AsyncBaseTransport):
    """Apply entity and per-event limits before HTTPX/MCP decoding."""

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport | None = None,
        *,
        report_sse_eof: bool = False,
        transport_failure_signal: McpTransportFailureSignal | None = None,
    ) -> None:
        self._inner = inner or httpx.AsyncHTTPTransport(verify=True)
        self._report_sse_eof = report_sse_eof
        self._transport_failure_signal = transport_failure_signal

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        encoding = response.headers.get("content-encoding")
        if encoding is not None and encoding.strip().casefold() != "identity":
            encoding_error = _unsupported_content_encoding_error(
                self._transport_failure_signal
            )
            with contextlib.suppress(BaseException):
                await response.aclose()
            raise encoding_error
        content_type = response.headers.get("content-type", "").partition(";")[0].strip()
        content_length = response.headers.get("content-length")
        if content_type.casefold() != "text/event-stream" and content_length is not None:
            with contextlib.suppress(ValueError):
                if int(content_length) > MCP_MESSAGE_BYTES_MAX:
                    error = _message_limit_error(
                        "MCP HTTP response exceeds the raw byte limit",
                        self._transport_failure_signal,
                    )
                    with contextlib.suppress(BaseException):
                        await response.aclose()
                    raise error
        if content_type.casefold() == "text/event-stream":
            stream: httpx.AsyncByteStream = _BoundedSseStream(
                cast(httpx.AsyncByteStream, response.stream),
                report_eof=self._report_sse_eof and request.method == "GET",
                transport_failure_signal=self._transport_failure_signal,
            )
        else:
            stream = _BoundedEntityStream(
                cast(httpx.AsyncByteStream, response.stream),
                self._transport_failure_signal,
            )
        response.stream = stream
        return response

    async def aclose(self) -> None:
        await self._inner.aclose()


def create_mcp_http_client(
    *,
    headers: Mapping[str, str] | None = None,
    auth: httpx.Auth | None = None,
    timeout: httpx.Timeout | float | None = None,
    _transport: httpx.AsyncBaseTransport | None = None,
    transport_failure_signal: McpTransportFailureSignal | None = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """FastMCP client factory with fixed TLS, proxy and redirect behavior."""

    del timeout
    kwargs.pop("follow_redirects", None)
    bounded_headers = dict(headers or {})
    for name in tuple(bounded_headers):
        if name.casefold() == "accept-encoding":
            del bounded_headers[name]
    bounded_headers["Accept-Encoding"] = "identity"
    return httpx.AsyncClient(
        headers=bounded_headers,
        auth=auth,
        timeout=None,
        follow_redirects=False,
        trust_env=False,
        verify=True,
        transport=BoundedHttpTransport(
            _transport,
            report_sse_eof=True,
            transport_failure_signal=transport_failure_signal,
        ),
    )


async def _discard_log_notification(params: types.LoggingMessageNotificationParams) -> None:
    del params


def create_fastmcp_client(
    transport: ClientTransport,
    *,
    message_handler: MessageHandlerFnT | None = None,
) -> Client[ClientTransport]:
    install_mcp_log_discard_boundary()
    return Client(
        transport,
        timeout=None,
        init_timeout=0,
        log_handler=_discard_log_notification,
        message_handler=message_handler,
    )


class RuntimeSession(Protocol):
    def get_server_capabilities(self) -> types.ServerCapabilities | None: ...

    async def send_request(
        self,
        request: types.ClientRequest,
        result_type: type[types.CallToolResult],
    ) -> types.CallToolResult: ...

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult: ...

    async def list_resources(self, cursor: str | None = None) -> types.ListResourcesResult: ...

    async def list_resource_templates(
        self, cursor: str | None = None
    ) -> types.ListResourceTemplatesResult: ...

    async def list_prompts(self, cursor: str | None = None) -> types.ListPromptsResult: ...

    async def read_resource(self, uri: AnyUrl) -> types.ReadResourceResult: ...

    async def get_prompt(
        self,
        name: str,
        arguments: dict[str, str] | None = None,
    ) -> types.GetPromptResult: ...


class RuntimeClient(Protocol):
    @property
    def session(self) -> RuntimeSession: ...

    @property
    def transport(self) -> object: ...

    async def __aenter__(self) -> object: ...

    async def close(self) -> None: ...


type RuntimeClientFactory = Callable[[ServerMcpServerConfig], RuntimeClient]


def _runtime_http_client(
    headers: dict[str, str] | None = None,
    timeout: httpx.Timeout | None = None,
    auth: httpx.Auth | None = None,
    transport_failure_signal: McpTransportFailureSignal | None = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    return create_mcp_http_client(
        headers=headers,
        timeout=timeout,
        auth=auth,
        transport_failure_signal=transport_failure_signal,
        **kwargs,
    )


def build_runtime_client(
    config: ServerMcpServerConfig,
    *,
    message_handler: MessageHandlerFnT | None = None,
    transport_failure_signal: McpTransportFailureSignal | None = None,
) -> RuntimeClient:
    """Build exactly one shared FastMCP client for the configured transport."""

    transport: ClientTransport
    if isinstance(config, ServerStdioMcpServerConfig):
        transport = BoundedStdioTransport(
            config.command,
            config.args,
            cwd=config.cwd,
            env={name: value.get_secret_value() for name, value in config.env.items()},
            transport_failure_signal=transport_failure_signal,
        )
    elif isinstance(config, ServerStreamableHttpMcpServerConfig):
        transport = StreamableHttpTransport(
            config.url,
            headers={name: value.get_secret_value() for name, value in config.headers.items()},
            httpx_client_factory=partial(
                _runtime_http_client,
                transport_failure_signal=transport_failure_signal,
            ),
        )
    elif isinstance(config, ServerSseMcpServerConfig):
        transport = SSETransport(
            config.url,
            headers={name: value.get_secret_value() for name, value in config.headers.items()},
            httpx_client_factory=partial(
                _runtime_http_client,
                transport_failure_signal=transport_failure_signal,
            ),
        )
    else:  # pragma: no cover - the strict tagged union is exhaustive
        raise TypeError("unsupported Server MCP transport")
    return cast(
        RuntimeClient,
        create_fastmcp_client(transport, message_handler=message_handler),
    )


def _resolved_argv(
    command: str,
    args: Sequence[str],
    environment: Mapping[str, str],
    cwd: Path,
) -> tuple[str, ...]:
    candidate = command
    search_path = environment.get("PATH")
    if os.path.dirname(command) and not os.path.isabs(command):
        candidate = str(cwd / command)
    elif search_path is not None:
        search_path = os.pathsep.join(
            item if os.path.isabs(item) else str(cwd / item)
            for item in search_path.split(os.pathsep)
        )
    resolved = shutil.which(candidate, path=search_path)
    if resolved is None:
        raise FileNotFoundError(command)
    return resolved, *args


def _process_group_exists(pid: int) -> bool:
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class BoundedStdioTransport(ClientTransport):
    """Linux direct-argv stdio transport with bounded records and tree cleanup."""

    def __init__(
        self,
        command: str,
        args: Sequence[str] = (),
        *,
        cwd: str | Path | None = None,
        env: Mapping[str, str] | None = None,
        stderr_sink: IO[bytes] | None = None,
        transport_failure_signal: McpTransportFailureSignal | None = None,
    ) -> None:
        install_mcp_log_discard_boundary()
        self.command = command
        self.args = tuple(args)
        self.cwd = Path.home() if cwd is None else Path(cwd).expanduser()
        if not self.cwd.is_absolute() or not self.cwd.is_dir():
            raise FileNotFoundError("configured MCP cwd is unavailable")
        self.environment = build_mcp_environment(os.environ, env)
        self._provided_stderr_sink = stderr_sink
        self._transport_failure_signal = transport_failure_signal
        self._owned_stderr_sink: IO[bytes] | None = None
        self.process: asyncio.subprocess.Process | None = None
        self.terminal_error: Exception | None = None
        self.cleanup_incomplete = False
        self._cleanup_blocked = False
        self._connecting = False
        self._closing = False
        self._cleanup_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._read_sender: MemoryObjectSendStream[SessionMessage | Exception] | None = None
        self._read_receiver: MemoryObjectReceiveStream[SessionMessage | Exception] | None = None
        self._write_sender: MemoryObjectSendStream[SessionMessage] | None = None
        self._write_receiver: MemoryObjectReceiveStream[SessionMessage] | None = None

    async def _start(self) -> None:
        if self._cleanup_blocked:
            raise McpTransportClosingError("stdio cleanup is incomplete")
        if self._connecting or (self.process is not None and self.process.returncode is None):
            raise McpTransportClosingError("stdio transport is already connected")
        self._connecting = True
        self._closing = False
        self.cleanup_incomplete = False
        self.terminal_error = None
        self._cleanup_task = None
        if self._provided_stderr_sink is None:
            self._owned_stderr_sink = open(os.devnull, "wb")  # noqa: SIM115
        stderr_sink = self._provided_stderr_sink or self._owned_stderr_sink
        assert stderr_sink is not None
        spawned = False
        try:
            argv = _resolved_argv(self.command, self.args, self.environment, self.cwd)
            self.process = await asyncio.create_subprocess_exec(
                *argv,
                shell=False,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_sink,
                cwd=str(self.cwd),
                env=self.environment,
                start_new_session=True,
            )
            spawned = True
            if self.process.stdin is None or self.process.stdout is None:
                raise McpTransportError("stdio process streams were not created")
            read_sender, read_receiver = anyio.create_memory_object_stream[
                SessionMessage | Exception
            ](0)
            write_sender, write_receiver = anyio.create_memory_object_stream[SessionMessage](0)
            self._read_sender = read_sender
            self._read_receiver = read_receiver
            self._write_sender = write_sender
            self._write_receiver = write_receiver
            self._reader_task = asyncio.create_task(
                self._read_stdout(), name="server-mcp-stdio-reader"
            )
            self._writer_task = asyncio.create_task(
                self._write_stdin(), name="server-mcp-stdio-writer"
            )
        except BaseException:
            self._connecting = False
            if spawned:
                with contextlib.suppress(BaseException):
                    await self.close()
            else:
                self._close_stderr()
            raise

    async def _publish_error(self, error: Exception) -> None:
        if self._read_sender is not None:
            with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
                await self._read_sender.send(error)

    async def _read_stdout(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        buffer = bytearray()
        try:
            while chunk := await self.process.stdout.read(_READ_CHUNK_BYTES):
                buffer.extend(chunk)
                while (newline := buffer.find(b"\n")) >= 0:
                    record_size = newline + 1
                    if record_size > MCP_MESSAGE_BYTES_MAX:
                        raise _message_limit_error(
                            "MCP stdio record exceeds the raw byte limit",
                            self._transport_failure_signal,
                        )
                    record = bytes(buffer[:newline])
                    del buffer[:record_size]
                    message = types.JSONRPCMessage.model_validate_json(record)
                    assert self._read_sender is not None
                    await self._read_sender.send(SessionMessage(message))
                if len(buffer) > MCP_MESSAGE_BYTES_MAX:
                    raise _message_limit_error(
                        "MCP stdio record exceeds the raw byte limit",
                        self._transport_failure_signal,
                    )
            if buffer:
                raise McpTransportError(
                    "MCP stdio stream ended with an incomplete record"
                )
            if not self._closing:
                error = McpTransportError("MCP stdio stream ended unexpectedly")
                self.terminal_error = error
                await self._publish_error(error)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.terminal_error = exc
            await self._publish_error(exc)
        finally:
            if self._read_sender is not None:
                await self._read_sender.aclose()

    async def _write_stdin(self) -> None:
        assert self.process is not None and self.process.stdin is not None
        assert self._write_receiver is not None
        try:
            async with self._write_receiver:
                async for session_message in self._write_receiver:
                    payload = session_message.message.model_dump_json(
                        by_alias=True, exclude_none=True
                    ).encode("utf-8") + b"\n"
                    self.process.stdin.write(payload)
                    await self.process.stdin.drain()
        except asyncio.CancelledError:
            raise
        except (BrokenPipeError, ConnectionError, OSError):
            error = McpTransportError("MCP stdio input closed unexpectedly")
            self.terminal_error = error
            await self._publish_error(error)

    @asynccontextmanager
    async def connect_session(
        self, **session_kwargs: Unpack[SessionKwargs]
    ) -> AsyncIterator[ClientSession]:
        await self._start()
        assert self._read_receiver is not None and self._write_sender is not None
        try:
            async with ClientSession(
                self._read_receiver,
                self._write_sender,
                **session_kwargs,
            ) as session:
                yield session
        finally:
            await self.close()

    async def _tree_converged(self) -> bool:
        if self.process is None:
            return True
        return self.process.returncode is not None and not _process_group_exists(
            self.process.pid
        )

    async def _wait_for_tree(self, timeout: float) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if await self._tree_converged():
                return True
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(min(0.05, timeout))

    async def _signal_tree(self, sig: int) -> None:
        if self.process is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(self.process.pid, sig)

    async def _close_streams_and_tasks(self) -> None:
        for stream in (
            self._write_sender,
            self._write_receiver,
            self._read_sender,
            self._read_receiver,
        ):
            if stream is not None:
                with contextlib.suppress(
                    anyio.ClosedResourceError, anyio.BrokenResourceError
                ):
                    await stream.aclose()
        current = asyncio.current_task()
        tasks = [
            task
            for task in (self._reader_task, self._writer_task)
            if task is not None and task is not current
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _close_stderr(self) -> None:
        if self._owned_stderr_sink is not None:
            self._owned_stderr_sink.close()
            self._owned_stderr_sink = None

    async def _cleanup(self) -> None:
        process = self.process
        if process is None:
            self._connecting = False
            self._close_stderr()
            return
        loop = asyncio.get_running_loop()
        phase_deadline = loop.time() + _STDIN_EOF_SECONDS
        if process.stdin is not None:
            process.stdin.close()
            with contextlib.suppress(
                BrokenPipeError, ConnectionError, OSError, TimeoutError
            ):
                await asyncio.wait_for(
                    process.stdin.wait_closed(), max(0, phase_deadline - loop.time())
                )
        stopped = await self._wait_for_tree(max(0, phase_deadline - loop.time()))
        if not stopped:
            phase_deadline = loop.time() + _TERMINATE_SECONDS
            await self._signal_tree(signal.SIGTERM)
            stopped = await self._wait_for_tree(max(0, phase_deadline - loop.time()))
        if not stopped:
            phase_deadline = loop.time() + _FORCE_KILL_SECONDS
            await self._signal_tree(signal.SIGKILL)
            stopped = await self._wait_for_tree(max(0, phase_deadline - loop.time()))
        self.cleanup_incomplete = not stopped
        self._cleanup_blocked = not stopped
        await self._close_streams_and_tasks()
        self._close_stderr()
        self._connecting = False

    async def close(self) -> None:
        """Run one bounded cleanup while deferring caller cancellation."""

        self._closing = True
        if self._cleanup_task is None or (
            self._cleanup_task.done() and self.cleanup_incomplete
        ):
            self._cleanup_task = asyncio.create_task(
                self._cleanup(), name="server-mcp-stdio-cleanup"
            )
        cancelled = False
        while not self._cleanup_task.done():
            try:
                await asyncio.shield(self._cleanup_task)
            except asyncio.CancelledError:
                cancelled = True
        await self._cleanup_task
        if cancelled:
            raise asyncio.CancelledError
