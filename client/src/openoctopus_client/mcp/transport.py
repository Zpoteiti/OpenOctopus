"""Bounded FastMCP client transports.

FastMCP owns MCP session initialization and protocol semantics.  This module
owns the OpenOctopus safety boundaries which must run before SDK decoding:
stdio record limits, remote response/event limits, secret-free child process
creation, and bounded process cleanup.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ntpath
import os
import shutil
import signal
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import IO, Any, Unpack, cast

import anyio
import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from fastmcp import Client
from fastmcp.client.transports import ClientTransport
from fastmcp.client.transports.base import SessionKwargs
from mcp import ClientSession, types
from mcp.client.session import MessageHandlerFnT
from mcp.shared.message import SessionMessage

from openoctopus_client.process import (
    _create_windows_job,
    _new_session_kwargs,
    _process_group_exists,
    _send_process_group_signal,
    _windows_process_snapshot,
    _windows_process_tree,
)

MCP_MESSAGE_BYTES_MAX = 12 * 1024 * 1024
_STDIN_EOF_SECONDS = 2.0
_TERMINATE_SECONDS = 3.0
_FORCE_KILL_SECONDS = 5.0
_READ_CHUNK_BYTES = 64 * 1024

_POSIX_SAFE_ENV = ("HOME", "LOGNAME", "PATH", "SHELL", "TERM", "USER")
_WINDOWS_SAFE_ENV = (
    "APPDATA",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "USERNAME",
    "USERPROFILE",
)
_DISCARDED_LOGGER_ROOTS = ("fastmcp", "mcp", "httpx", "httpcore")


class McpTransportError(RuntimeError):
    """Stable base class for MCP transport boundary failures."""


class McpMessageTooLargeError(McpTransportError):
    """An inbound MCP message exceeded the pre-decode byte limit."""


class UnsupportedMcpContentEncodingError(McpTransportError):
    """A remote MCP response attempted content encoding."""


class McpTransportClosingError(McpTransportError):
    """A stdio transport cannot accept another connection."""


def _is_client_secret(name: str) -> bool:
    return name.casefold().startswith("openoctopus_")


def _environment_value(source: Mapping[str, str], name: str, *, windows: bool) -> str | None:
    if not windows:
        return source.get(name)
    normalized = name.casefold()
    for key, value in source.items():
        if key.casefold() == normalized:
            return value
    return None


def _overlay_environment(
    target: dict[str, str], overlay: Mapping[str, str], *, windows: bool
) -> None:
    for key, value in overlay.items():
        if _is_client_secret(key):
            continue
        if windows:
            normalized = key.casefold()
            for existing in tuple(target):
                if existing.casefold() == normalized:
                    del target[existing]
        target[key] = value


def build_mcp_environment(
    parent: Mapping[str, str],
    overlay: Mapping[str, str] | None = None,
    *,
    windows: bool | None = None,
) -> dict[str, str]:
    """Build the MCP SDK safe baseline plus a secret-filtered config overlay."""

    is_windows = os.name == "nt" if windows is None else windows
    safe_names = _WINDOWS_SAFE_ENV if is_windows else _POSIX_SAFE_ENV
    result: dict[str, str] = {}
    for name in safe_names:
        value = _environment_value(parent, name, windows=is_windows)
        if value is not None and not value.startswith("()") and not _is_client_secret(name):
            result[name] = value
    _overlay_environment(result, overlay or {}, windows=is_windows)
    return {key: value for key, value in result.items() if not _is_client_secret(key)}


class _DiscardHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        del record


def install_mcp_log_discard_boundary() -> None:
    """Stop third-party MCP/HTTP log records before application handlers."""

    roots = set(_DISCARDED_LOGGER_ROOTS)
    for name in tuple(logging.root.manager.loggerDict):
        if any(name == root or name.startswith(root + ".") for root in roots):
            roots.add(name)
    for name in roots:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.addHandler(_DiscardHandler())
        logger.propagate = False


class _BoundedEntityStream(httpx.AsyncByteStream):
    def __init__(self, inner: httpx.AsyncByteStream) -> None:
        self._inner = inner

    async def __aiter__(self) -> AsyncIterator[bytes]:
        size = 0
        async for chunk in self._inner:
            size += len(chunk)
            if size > MCP_MESSAGE_BYTES_MAX:
                await self._inner.aclose()
                raise McpMessageTooLargeError("MCP HTTP response exceeds the raw byte limit")
            yield chunk

    async def aclose(self) -> None:
        await self._inner.aclose()


class _BoundedSseStream(httpx.AsyncByteStream):
    def __init__(self, inner: httpx.AsyncByteStream, *, report_eof: bool) -> None:
        self._inner = inner
        self._report_eof = report_eof

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
                            await self._inner.aclose()
                            raise McpMessageTooLargeError(
                                "MCP SSE event exceeds the raw byte limit"
                            )
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
                    await self._inner.aclose()
                    raise McpMessageTooLargeError("MCP SSE event exceeds the raw byte limit")
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
        await self._inner.aclose()


class BoundedHttpTransport(httpx.AsyncBaseTransport):
    """Apply raw entity/SSE limits before HTTPX or MCP decode the body."""

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport | None = None,
        *,
        report_sse_eof: bool = False,
    ) -> None:
        self._inner = inner or httpx.AsyncHTTPTransport(verify=True)
        self._report_sse_eof = report_sse_eof

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        content_encoding = response.headers.get("content-encoding")
        if content_encoding is not None and content_encoding.strip().casefold() != "identity":
            await response.aclose()
            raise UnsupportedMcpContentEncodingError(
                "MCP responses must use identity content encoding"
            )
        content_type = response.headers.get("content-type", "").partition(";")[0].strip()
        content_length = response.headers.get("content-length")
        if content_type.casefold() != "text/event-stream" and content_length is not None:
            with contextlib.suppress(ValueError):
                if int(content_length) > MCP_MESSAGE_BYTES_MAX:
                    await response.aclose()
                    raise McpMessageTooLargeError(
                        "MCP HTTP response exceeds the raw byte limit"
                    )
        stream: httpx.AsyncByteStream
        if content_type.casefold() == "text/event-stream":
            stream = _BoundedSseStream(
                cast(httpx.AsyncByteStream, response.stream),
                report_eof=self._report_sse_eof and request.method == "GET",
            )
        else:
            stream = _BoundedEntityStream(cast(httpx.AsyncByteStream, response.stream))
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
    **kwargs: Any,
) -> httpx.AsyncClient:
    """FastMCP-compatible HTTP client factory with fixed network semantics."""

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
        transport=BoundedHttpTransport(_transport, report_sse_eof=True),
    )


def _windows_env_value(environment: Mapping[str, str], name: str) -> str | None:
    return _environment_value(environment, name, windows=True)


def _resolve_windows_command(
    command: str,
    environment: Mapping[str, str],
    *,
    cwd: str | Path | None = None,
) -> str:
    path = _windows_env_value(environment, "PATH") or ""
    pathext_value = _windows_env_value(environment, "PATHEXT") or ".COM;.EXE;.BAT;.CMD"
    extensions = tuple(
        item.casefold() for item in pathext_value.split(";") if item.startswith(".")
    )
    supplied_extension = ntpath.splitext(command)[1].casefold()
    working_dir = str(Path.cwd() if cwd is None else cwd)
    candidates: tuple[str, ...]
    if ntpath.dirname(command):
        candidates = (
            command if ntpath.isabs(command) else ntpath.join(working_dir, command),
        )
    else:
        candidates = tuple(
            ntpath.join(
                directory.strip('"')
                if ntpath.isabs(directory.strip('"'))
                else ntpath.join(working_dir, directory.strip('"')),
                command,
            )
            for directory in path.split(";")
        )
    for candidate in candidates:
        names = (
            (candidate,)
            if supplied_extension in extensions
            else tuple(candidate + ext for ext in extensions)
        )
        for resolved in names:
            resolved = ntpath.normpath(resolved)
            if os.path.isfile(resolved):
                return resolved
    raise FileNotFoundError(command)


_WINDOWS_BATCH_UNQUOTED = frozenset(r"#$*+-./:?@\_")


def _windows_batch_arg(argument: str) -> str:
    """Encode one literal argument for cmd.exe's batch-file parser."""

    if "\r" in argument or "\n" in argument:
        raise ValueError("Windows batch arguments must not contain CR or LF")
    quoted = (
        not argument
        or argument.endswith("\\")
        or any(
            (
                character.isascii()
                and not (character.isalnum() or character in _WINDOWS_BATCH_UNQUOTED)
            )
            or not character.isprintable()
            for character in argument
        )
    )
    encoded: list[str] = ['"'] if quoted else []
    backslashes = 0
    for character in argument:
        if character == "\\":
            backslashes += 1
        else:
            if character == '"':
                encoded.append("\\" * backslashes)
                encoded.append('"')
            elif character == "%":
                # Expand an empty substring before the original percent so cmd
                # cannot reinterpret a later pair as %VARIABLE%.
                encoded.append("%%cd:~,")
            backslashes = 0
        encoded.append(character)
    if quoted:
        encoded.append("\\" * backslashes)
        encoded.append('"')
    return "".join(encoded)


def _windows_batch_command_line(script: str, args: Sequence[str]) -> str:
    if '"' in script or script.endswith("\\"):
        raise ValueError("Windows batch script path is invalid")
    encoded_script = _windows_batch_arg(script)
    if not encoded_script.startswith('"'):
        encoded_script = f'"{encoded_script}"'
    encoded_args = " ".join(_windows_batch_arg(argument) for argument in args)
    suffix = f" {encoded_args}" if encoded_args else ""
    return f'"{encoded_script}{suffix}"'


def _stdio_argv(
    command: str,
    args: Sequence[str],
    environment: Mapping[str, str],
    *,
    cwd: str | Path | None = None,
) -> tuple[str, ...]:
    if os.name != "nt":
        candidate = command
        search_path = environment.get("PATH")
        if os.path.dirname(command) and not os.path.isabs(command):
            candidate = str((Path.cwd() if cwd is None else Path(cwd)) / command)
        elif cwd is not None and search_path is not None:
            search_path = os.pathsep.join(
                entry if os.path.isabs(entry) else str(Path(cwd) / entry)
                for entry in search_path.split(os.pathsep)
            )
        resolved = shutil.which(candidate, path=search_path)
        if resolved is None:
            raise FileNotFoundError(command)
        return (resolved, *args)

    resolved = _resolve_windows_command(command, environment, cwd=cwd)
    if ntpath.splitext(resolved)[1].casefold() not in {".cmd", ".bat"}:
        return (resolved, *args)
    system_root = _windows_env_value(os.environ, "SystemRoot") or r"C:\Windows"
    comspec = ntpath.join(system_root, "System32", "cmd.exe")
    if not os.path.isfile(comspec):
        raise FileNotFoundError(comspec)
    command_line = _windows_batch_command_line(resolved, args)
    return (comspec, "/D", "/E:ON", "/V:OFF", "/S", "/C", command_line)


async def _discard_log_notification(params: types.LoggingMessageNotificationParams) -> None:
    del params


def create_fastmcp_client(
    transport: ClientTransport,
    *,
    message_handler: MessageHandlerFnT | None = None,
) -> Client[ClientTransport]:
    """Construct FastMCP with all SDK-internal deadlines disabled."""

    install_mcp_log_discard_boundary()
    return Client(
        transport,
        timeout=None,
        init_timeout=0,
        log_handler=_discard_log_notification,
        message_handler=message_handler,
    )


class BoundedStdioTransport(ClientTransport):
    """Dedicated bidirectional stdio transport with pre-decode record bounds."""

    def __init__(
        self,
        command: str,
        args: Sequence[str] = (),
        *,
        cwd: str | Path | None = None,
        env: Mapping[str, str] | None = None,
        stderr_sink: IO[bytes] | None = None,
    ) -> None:
        install_mcp_log_discard_boundary()
        self.command = command
        self.args = tuple(args)
        self.cwd = Path(cwd) if cwd is not None else None
        self.environment = build_mcp_environment(os.environ, env)
        self._provided_stderr_sink = stderr_sink
        self._owned_stderr_sink: IO[bytes] | None = None
        self.process: asyncio.subprocess.Process | None = None
        self.terminal_error: Exception | None = None
        self.cleanup_incomplete = False
        self._cleanup_blocked = False
        self._connecting = False
        self._cleanup_task: asyncio.Task[None] | None = None
        self._closing = False
        self._reader_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._read_sender: MemoryObjectSendStream[SessionMessage | Exception] | None = None
        self._read_receiver: MemoryObjectReceiveStream[SessionMessage | Exception] | None = None
        self._write_sender: MemoryObjectSendStream[SessionMessage] | None = None
        self._write_receiver: MemoryObjectReceiveStream[SessionMessage] | None = None
        self._job: Any | None = None
        self._job_assignment_failed = False
        self._windows_tree: set[int] | None = None

    async def _start(self) -> None:
        if self._cleanup_blocked:
            raise McpTransportClosingError("stdio cleanup is incomplete")
        if self._connecting or (self.process is not None and self.process.returncode is None):
            raise McpTransportClosingError("stdio transport is already connected")
        self._connecting = True
        self.cleanup_incomplete = False
        self._closing = False
        self.terminal_error = None
        self._cleanup_task = None
        if self._provided_stderr_sink is None:
            self._owned_stderr_sink = open(os.devnull, "wb")  # noqa: SIM115
        stderr_sink = self._provided_stderr_sink or self._owned_stderr_sink
        spawned = False
        try:
            self.process = await asyncio.create_subprocess_exec(
                *_stdio_argv(self.command, self.args, self.environment, cwd=self.cwd),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_sink,
                cwd=str(self.cwd) if self.cwd is not None else None,
                env=self.environment,
                **_new_session_kwargs(),
            )
            spawned = True
            if self.process.stdin is None or self.process.stdout is None:
                raise McpTransportError("stdio process streams were not created")
            if os.name == "nt":
                self._job = _create_windows_job(self.process.pid)
                self._job_assignment_failed = self._job is None
                self._windows_tree = _windows_process_tree(self.process.pid)
            read_sender, read_receiver = anyio.create_memory_object_stream[
                SessionMessage | Exception
            ](0)
            write_sender, write_receiver = anyio.create_memory_object_stream[SessionMessage](0)
            self._read_sender = read_sender
            self._read_receiver = read_receiver
            self._write_sender = write_sender
            self._write_receiver = write_receiver
            self._reader_task = asyncio.create_task(self._read_stdout(), name="mcp-stdio-reader")
            self._writer_task = asyncio.create_task(self._write_stdin(), name="mcp-stdio-writer")
        except BaseException:
            self._connecting = False
            if spawned:
                with contextlib.suppress(BaseException):
                    await self.close()
            else:
                await self._close_owned_stderr()
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
                        raise McpMessageTooLargeError(
                            "MCP stdio record exceeds the raw byte limit"
                        )
                    record = bytes(buffer[:newline])
                    del buffer[:record_size]
                    message = types.JSONRPCMessage.model_validate_json(record)
                    assert self._read_sender is not None
                    await self._read_sender.send(SessionMessage(message))
                if len(buffer) > MCP_MESSAGE_BYTES_MAX:
                    raise McpMessageTooLargeError("MCP stdio record exceeds the raw byte limit")
            if buffer:
                raise McpTransportError("MCP stdio stream ended with an incomplete record")
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
        process = self.process
        if process is None:
            return True
        root_gone = process.returncode is not None
        if os.name != "nt":
            return root_gone and not _process_group_exists(process.pid)
        snapshot = _windows_process_snapshot()
        if snapshot is None:
            return False
        tracked = set(self._windows_tree or {process.pid})
        while True:
            children = {
                child
                for child, parent in snapshot.items()
                if parent in tracked and child not in tracked
            }
            if not children:
                break
            tracked.update(children)
        self._windows_tree = tracked
        return root_gone and not tracked.intersection(snapshot)

    async def _wait_for_tree(self, timeout: float) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if await self._tree_converged():
                return True
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(min(0.05, timeout))

    async def _terminate_tree(self) -> None:
        process = self.process
        if process is None:
            return
        if os.name != "nt":
            with contextlib.suppress(ProcessLookupError, PermissionError):
                _send_process_group_signal(process.pid, signal.SIGTERM)
            return
        taskkill = ntpath.join(
            _windows_env_value(os.environ, "SystemRoot") or r"C:\Windows",
            "System32",
            "taskkill.exe",
        )
        killer: asyncio.subprocess.Process | None = None
        try:
            killer = await asyncio.create_subprocess_exec(
                taskkill,
                "/PID",
                str(process.pid),
                "/T",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await killer.wait()
        except OSError:
            return
        finally:
            if killer is not None and killer.returncode is None:
                with contextlib.suppress(OSError, ProcessLookupError):
                    killer.kill()
                with contextlib.suppress(OSError, TimeoutError):
                    await asyncio.wait_for(killer.wait(), 1)

    async def _force_kill_tree(self) -> None:
        process = self.process
        if process is None:
            return
        if os.name != "nt":
            with contextlib.suppress(ProcessLookupError, PermissionError):
                sigkill = cast(int, getattr(signal, "SIGKILL"))
                _send_process_group_signal(process.pid, sigkill)
            return
        if self._job is not None:
            with contextlib.suppress(OSError):
                self._job.terminate()
        if process.returncode is None:
            with contextlib.suppress(OSError, ProcessLookupError):
                process.kill()

    async def _close_streams_and_tasks(self) -> None:
        for stream in (
            self._write_sender,
            self._write_receiver,
            self._read_sender,
            self._read_receiver,
        ):
            if stream is not None:
                with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
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

    async def _close_owned_stderr(self) -> None:
        if self._owned_stderr_sink is not None:
            self._owned_stderr_sink.close()
            self._owned_stderr_sink = None

    async def _cleanup(self) -> None:
        process = self.process
        if process is None:
            self._connecting = False
            await self._close_owned_stderr()
            return
        loop = asyncio.get_running_loop()
        phase_deadline = loop.time() + _STDIN_EOF_SECONDS
        if process.stdin is not None:
            process.stdin.close()
            with contextlib.suppress(BrokenPipeError, ConnectionError, OSError, TimeoutError):
                await asyncio.wait_for(
                    process.stdin.wait_closed(), max(0, phase_deadline - loop.time())
                )
        converged = await self._wait_for_tree(max(0, phase_deadline - loop.time()))
        if not converged:
            phase_deadline = loop.time() + _TERMINATE_SECONDS
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._terminate_tree(), max(0, phase_deadline - loop.time())
                )
            converged = await self._wait_for_tree(max(0, phase_deadline - loop.time()))
        if not converged:
            phase_deadline = loop.time() + _FORCE_KILL_SECONDS
            await self._force_kill_tree()
            converged = await self._wait_for_tree(max(0, phase_deadline - loop.time()))
        if os.name == "nt" and self._job is not None and converged:
            converged = bool(self._job.close())
            self._job = None
        if self._job_assignment_failed:
            converged = False
        self.cleanup_incomplete = not converged
        self._cleanup_blocked = not converged
        await self._close_streams_and_tasks()
        await self._close_owned_stderr()
        self._connecting = False

    async def close(self) -> None:
        """Close once while deferring caller cancellation until cleanup finishes."""

        self._closing = True
        if self._cleanup_task is None or (
            self._cleanup_task.done() and self.cleanup_incomplete
        ):
            self._cleanup_task = asyncio.create_task(self._cleanup(), name="mcp-stdio-cleanup")
        cancelled = False
        while not self._cleanup_task.done():
            try:
                await asyncio.shield(self._cleanup_task)
            except asyncio.CancelledError:
                cancelled = True
        await self._cleanup_task
        if cancelled:
            raise asyncio.CancelledError
