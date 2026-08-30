"""Composable Client-side MCP validation and active runtime primitives."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from uuid import UUID

import httpx
from fastmcp.client.messages import MessageHandler
from fastmcp.client.transports import ClientTransport, SSETransport, StreamableHttpTransport
from mcp import types
from mcp.shared.exceptions import McpError
from pydantic import AnyUrl, ValidationError

from openoctopus_client.mcp.catalog import (
    CatalogSession,
    McpCatalogError,
    McpEntryRoute,
    bind_server_entries,
    canonical_json_bytes,
    canonicalize_source_catalog,
    discover_server_catalog,
    expand_resource_template,
    normalized_resource_uri,
)
from openoctopus_client.mcp.models import (
    McpServerConfig,
    PersistedMcpServerCatalog,
    RemoteMcpServerConfigBase,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SseMcpServerConfig,
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
)
from openoctopus_client.mcp.result import (
    map_prompt_result,
    map_resource_result,
    map_tool_result,
)
from openoctopus_client.mcp.transport import (
    BoundedStdioTransport,
    McpMessageTooLargeError,
    McpTransportError,
    McpTransportFailureSignal,
    UnsupportedMcpContentEncodingError,
    create_fastmcp_client,
    create_mcp_http_client,
)
from openoctopus_client.protocol import new_uuid7
from openoctopus_client.tools.common import ToolOutput, fail

CONNECT_TIMEOUT_SECONDS = 30.0
DISCOVERY_TIMEOUT_SECONDS = 30.0
INVOCATION_TIMEOUT_SECONDS = 60.0
REMOTE_CLEANUP_TIMEOUT_SECONDS = 10.0
CANDIDATE_TIMEOUT_SECONDS = 300.0
VALIDATION_PARALLELISM = 4

type ValidationStage = Literal["connect", "discovery", "binding", "candidate", "cleanup"]
type McpRuntimeEventKind = Literal[
    "tools_changed",
    "resources_changed",
    "prompts_changed",
    "transport_failed",
]


def _find_nested_exception[T: BaseException](
    error: BaseException,
    expected: type[T],
) -> T | None:
    pending = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(current, expected):
            return current
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return None


def _contains_message_too_large(error: BaseException) -> bool:
    return _find_nested_exception(error, McpMessageTooLargeError) is not None


def _contains_unsupported_content_encoding(error: BaseException) -> bool:
    return _find_nested_exception(error, UnsupportedMcpContentEncodingError) is not None


@dataclass(frozen=True, slots=True)
class McpRuntimeEvent:
    kind: McpRuntimeEventKind


class McpRuntimeMessageHandler(MessageHandler):
    """Translate public FastMCP message hooks into bounded runtime signals."""

    def __init__(self, emit: Callable[[McpRuntimeEventKind], None]) -> None:
        self._emit = emit
        self._message_too_large = False
        self._unsupported_content_encoding = False

    @property
    def message_too_large(self) -> bool:
        return self._message_too_large

    def clear_terminal_failure(self) -> None:
        self._message_too_large = False
        self._unsupported_content_encoding = False

    @property
    def unsupported_content_encoding(self) -> bool:
        return self._unsupported_content_encoding

    async def on_tool_list_changed(
        self, message: types.ToolListChangedNotification
    ) -> None:
        del message
        self._emit("tools_changed")

    async def on_resource_list_changed(
        self, message: types.ResourceListChangedNotification
    ) -> None:
        del message
        self._emit("resources_changed")

    async def on_prompt_list_changed(
        self, message: types.PromptListChangedNotification
    ) -> None:
        del message
        self._emit("prompts_changed")

    async def on_exception(self, message: Exception) -> None:
        if _contains_message_too_large(message):
            self._message_too_large = True
        elif _contains_unsupported_content_encoding(message):
            self._unsupported_content_encoding = True
        self._emit("transport_failed")


class RuntimeSession(CatalogSession, Protocol):
    async def send_request(
        self,
        request: types.ClientRequest,
        result_type: type[types.CallToolResult],
    ) -> types.CallToolResult: ...

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


class RuntimeClientFactory(Protocol):
    def __call__(
        self,
        config: McpServerConfig,
        *,
        message_handler: McpRuntimeMessageHandler | None = None,
        transport_failure_signal: McpTransportFailureSignal | None = None,
    ) -> RuntimeClient: ...


class McpRuntimeState(StrEnum):
    ABSENT = "absent"
    STARTING = "starting"
    DISCOVERING = "discovering"
    AWAITING_ACK = "awaiting_ack"
    READY = "ready"
    UNAVAILABLE = "unavailable"
    BACKOFF = "backoff"
    DRIFTED = "drifted"
    CLOSING = "closing"
    CLEANUP_BLOCKED = "cleanup_blocked"


@dataclass(frozen=True, slots=True)
class McpValidationFailure:
    server: str
    stage: ValidationStage
    code: str
    message: str

    @property
    def name(self) -> str:
        return self.server


class McpRuntimeError(RuntimeError):
    def __init__(self, failure: McpValidationFailure) -> None:
        self.failure = failure
        super().__init__(failure.message)


def _plain_secrets(config: RemoteMcpServerConfigBase) -> dict[str, str]:
    return {name: value.get_secret_value() for name, value in config.headers.items()}


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
    config: McpServerConfig,
    *,
    message_handler: McpRuntimeMessageHandler | None = None,
    transport_failure_signal: McpTransportFailureSignal | None = None,
) -> RuntimeClient:
    """Build exactly the transport selected by the validated tagged union."""

    transport: ClientTransport
    if isinstance(config, StdioMcpServerConfig):
        cwd = Path.home() if config.cwd is None else Path(config.cwd).expanduser()
        if not cwd.is_absolute() or not cwd.is_dir():
            raise FileNotFoundError("configured MCP cwd is unavailable")
        transport = BoundedStdioTransport(
            config.command,
            config.args,
            cwd=cwd,
            env={name: value.get_secret_value() for name, value in config.env.items()},
            transport_failure_signal=transport_failure_signal,
        )
    elif isinstance(config, StreamableHttpMcpServerConfig):
        transport = StreamableHttpTransport(
            config.url,
            headers=_plain_secrets(config),
            httpx_client_factory=partial(
                _runtime_http_client,
                transport_failure_signal=transport_failure_signal,
            ),
        )
    elif isinstance(config, SseMcpServerConfig):
        transport = SSETransport(
            config.url,
            headers=_plain_secrets(config),
            httpx_client_factory=partial(
                _runtime_http_client,
                transport_failure_signal=transport_failure_signal,
            ),
        )
    else:  # pragma: no cover - the strict tagged union is exhaustive
        raise TypeError("unsupported MCP transport")
    return cast(
        RuntimeClient,
        create_fastmcp_client(transport, message_handler=message_handler),
    )


def retry_backoff_seconds(attempt: int, *, jitter: float) -> float:
    """Return the 1/2/4/.../60 second retry sequence with bounded +/-20% jitter."""

    if attempt < 0 or not 0 <= jitter <= 1:
        raise ValueError("attempt and jitter are outside their valid ranges")
    base = min(float(2**attempt), 60.0)
    return min(60.0, base * (0.8 + 0.4 * jitter))


def _safe_failure(
    server: str,
    stage: ValidationStage,
    error: BaseException,
) -> McpValidationFailure:
    if isinstance(error, McpMessageTooLargeError):
        code = "mcp_message_too_large"
        message = f"MCP server '{server}' exceeded the inbound message limit during {stage}"
    elif isinstance(error, FileNotFoundError | PermissionError):
        code = "mcp_spawn_failed"
        message = f"MCP server '{server}' could not be started"
    elif isinstance(error, McpCatalogError):
        code = error.code
        message = f"MCP server '{server}' failed bounded catalog validation during {stage}"
    else:
        code = "config_validation_failed"
        message = f"MCP server '{server}' failed validation during {stage}"
    return McpValidationFailure(server=server, stage=stage, code=code, message=message)


def _is_permanent_failure(error: BaseException) -> bool:
    if _contains_unsupported_content_encoding(error):
        return True
    if isinstance(error, FileNotFoundError | PermissionError | McpCatalogError | ValidationError):
        return True
    return isinstance(error, httpx.HTTPStatusError) and error.response.status_code in {401, 403}


def _is_sdk_connection_closed(error: BaseException) -> bool:
    return bool(
        isinstance(error, McpError)
        and error.error.code == types.CONNECTION_CLOSED
        and error.error.message == "Connection closed"
    )


@dataclass(slots=True)
class CandidateValidation:
    source_catalog: SourceMcpCatalog | None
    failures: tuple[McpValidationFailure, ...]
    runtimes: dict[str, McpServerRuntime] = field(default_factory=dict, repr=False)

    @property
    def ok(self) -> bool:
        return self.source_catalog is not None and not self.failures

    async def close(self) -> None:
        await _close_runtimes(tuple(self.runtimes.values()))


class McpServerRuntime:
    """One MCP process/connection attempt with immutable generation identity."""

    def __init__(
        self,
        config: McpServerConfig,
        *,
        client_factory: RuntimeClientFactory | None = None,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
        discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
        invocation_timeout: float = INVOCATION_TIMEOUT_SECONDS,
        cleanup_timeout: float = REMOTE_CLEANUP_TIMEOUT_SECONDS,
    ) -> None:
        if (
            connect_timeout <= 0
            or discovery_timeout <= 0
            or invocation_timeout <= 0
            or cleanup_timeout <= 0
        ):
            raise ValueError("MCP runtime deadlines must be positive")
        self.config = config
        self.generation = new_uuid7()
        self.state = McpRuntimeState.STARTING
        self.code: str | None = "mcp_starting"
        self.last_failure: McpValidationFailure | None = None
        self.permanent_failure = False
        self._client_factory = client_factory or build_runtime_client
        self._connect_timeout = connect_timeout
        self._discovery_timeout = discovery_timeout
        self._invocation_timeout = invocation_timeout
        self._cleanup_timeout = cleanup_timeout
        self._client: RuntimeClient | None = None
        self._source_catalog: SourceMcpServerCatalog | None = None
        self._routes: dict[UUID, McpEntryRoute] = {}
        self._close_task: asyncio.Task[None] | None = None
        self._stdio_cleanup_failed = False
        self._remote_cleanup_pending = False
        self._retry_attempt = 0
        self._event_kinds: set[McpRuntimeEventKind] = set()
        self._event_order: deque[McpRuntimeEventKind] = deque()
        self._event_ready = asyncio.Event()
        self._message_handler = McpRuntimeMessageHandler(self._emit_event)
        self._transport_failure_signal = McpTransportFailureSignal()

    @property
    def message_handler(self) -> McpRuntimeMessageHandler:
        return self._message_handler

    def _emit_event(self, kind: McpRuntimeEventKind) -> None:
        if kind not in self._event_kinds:
            self._event_kinds.add(kind)
            self._event_order.append(kind)
            self._event_ready.set()

    async def _next_handler_event(self) -> McpRuntimeEvent:
        while not self._event_order:
            await self._event_ready.wait()
            self._event_ready.clear()
        kind = self._event_order.popleft()
        self._event_kinds.remove(kind)
        if self._event_order:
            self._event_ready.set()
        return McpRuntimeEvent(kind=kind)

    async def next_event(self) -> McpRuntimeEvent:
        handler_event = asyncio.create_task(self._next_handler_event())
        transport_failure = asyncio.create_task(self._transport_failure_signal.wait())
        try:
            done, _ = await asyncio.wait(
                {handler_event, transport_failure},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if transport_failure in done:
                return McpRuntimeEvent(kind="transport_failed")
            return handler_event.result()
        finally:
            for task in (handler_event, transport_failure):
                if not task.done():
                    task.cancel()
            await asyncio.gather(handler_event, transport_failure, return_exceptions=True)

    def _clear_events(self) -> None:
        self._event_kinds.clear()
        self._event_order.clear()
        self._event_ready.clear()

    @property
    def source_catalog(self) -> SourceMcpServerCatalog | None:
        return self._source_catalog

    @property
    def routes(self) -> Mapping[UUID, McpEntryRoute]:
        return dict(self._routes)

    async def _run_close_task(self) -> None:
        if self._close_task is None or (
            self._close_task.done() and self._cleanup_incomplete()
        ):
            self._stdio_cleanup_failed = False
            self._remote_cleanup_pending = False
            client = self._client

            async def close_client() -> None:
                if client is None:
                    return
                if not isinstance(self.config, StdioMcpServerConfig):
                    with contextlib.suppress(Exception):
                        await client.close()
                    return

                client_close_failed = False
                try:
                    await client.close()
                except asyncio.CancelledError:
                    client_close_failed = True
                except Exception:
                    client_close_failed = True

                transport = client.transport
                if isinstance(transport, BoundedStdioTransport):
                    try:
                        await transport.close()
                    except asyncio.CancelledError:
                        self._stdio_cleanup_failed = True
                    except Exception:
                        self._stdio_cleanup_failed = True
                    else:
                        self._stdio_cleanup_failed = transport.cleanup_incomplete
                else:
                    self._stdio_cleanup_failed = bool(
                        client_close_failed
                        or getattr(transport, "cleanup_incomplete", False)
                    )

            self._close_task = asyncio.create_task(
                close_client(),
                name=f"mcp-close-{self.config.name}",
            )
        cancelled = False
        deadline = (
            asyncio.get_running_loop().time() + self._cleanup_timeout
            if isinstance(self.config, RemoteMcpServerConfigBase)
            else None
        )
        while not self._close_task.done():
            try:
                if deadline is None:
                    await asyncio.shield(self._close_task)
                    continue
                remaining = max(0.0, deadline - asyncio.get_running_loop().time())
                done, _pending = await asyncio.wait(
                    {self._close_task},
                    timeout=remaining,
                )
                if self._close_task not in done:
                    self._close_task.cancel()
                    await asyncio.sleep(0)
                    break
            except asyncio.CancelledError:
                cancelled = True
        if self._close_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await self._close_task
            self._remote_cleanup_pending = False
        elif isinstance(self.config, RemoteMcpServerConfigBase):
            self._remote_cleanup_pending = True
        if cancelled:
            raise asyncio.CancelledError

    def _cleanup_incomplete(self) -> bool:
        if isinstance(self.config, RemoteMcpServerConfigBase):
            return bool(
                self._remote_cleanup_pending
                and self._close_task is not None
                and not self._close_task.done()
            )
        return bool(
            self._stdio_cleanup_failed
            or (
                self._client is not None
                and getattr(self._client.transport, "cleanup_incomplete", False)
            )
        )

    async def _await_transport_operation[T](self, operation: Awaitable[T]) -> T:
        operation_task = asyncio.ensure_future(operation)
        transport_failure = asyncio.create_task(self._transport_failure_signal.wait())
        try:
            done, _ = await asyncio.wait(
                {operation_task, transport_failure},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if transport_failure in done:
                raise self._transport_signal_error() from None
            return operation_task.result()
        finally:
            for task in (operation_task, transport_failure):
                if not task.done():
                    task.cancel()
            await asyncio.gather(operation_task, transport_failure, return_exceptions=True)

    def _transport_signal_error(self) -> McpTransportError:
        kind = self._transport_failure_signal.kind
        if kind == "message_too_large":
            return McpMessageTooLargeError(
                "MCP inbound message exceeded its raw byte limit"
            )
        if kind == "unsupported_content_encoding":
            return UnsupportedMcpContentEncodingError(
                "MCP responses must use identity content encoding"
            )
        raise RuntimeError("MCP transport failure signal has no failure kind")

    def _effective_terminal_error(
        self,
        error: BaseException | None,
    ) -> BaseException | None:
        if self._transport_failure_signal.kind is not None:
            return self._transport_signal_error()
        if error is not None and _contains_message_too_large(error):
            return McpMessageTooLargeError(
                "MCP inbound message exceeded its raw byte limit"
            )
        if error is not None and _contains_unsupported_content_encoding(error):
            return UnsupportedMcpContentEncodingError(
                "MCP responses must use identity content encoding"
            )
        if error is not None and not _is_sdk_connection_closed(error):
            return error
        if self._message_handler.message_too_large:
            return McpMessageTooLargeError("MCP inbound message exceeded its raw byte limit")
        if self._message_handler.unsupported_content_encoding:
            return UnsupportedMcpContentEncodingError(
                "MCP responses must use identity content encoding"
            )
        client = self._client
        terminal_error = (
            getattr(client.transport, "terminal_error", None)
            if client is not None
            else None
        )
        if isinstance(terminal_error, BaseException):
            return terminal_error
        return error

    async def _fail_start(
        self,
        stage: ValidationStage,
        error: BaseException,
    ) -> McpRuntimeError:
        effective_error = self._effective_terminal_error(error) or error
        failure = _safe_failure(self.config.name, stage, effective_error)
        self.last_failure = failure
        self.permanent_failure = _is_permanent_failure(effective_error)
        try:
            await self._run_close_task()
        finally:
            if self._cleanup_incomplete():
                self.state = McpRuntimeState.CLEANUP_BLOCKED
                self.code = "mcp_cleanup_incomplete"
            else:
                self.state = McpRuntimeState.UNAVAILABLE
                self.code = failure.code
        return McpRuntimeError(failure)

    async def start(self) -> SourceMcpServerCatalog:
        if self.state is not McpRuntimeState.STARTING:
            raise RuntimeError("MCP runtime attempt has already started")
        try:
            self._client = self._client_factory(
                self.config,
                message_handler=self._message_handler,
                transport_failure_signal=self._transport_failure_signal,
            )
            async with asyncio.timeout(self._connect_timeout):
                await self._await_transport_operation(self._client.__aenter__())
        except asyncio.CancelledError:
            await self._run_close_task()
            raise
        except Exception as exc:
            raise await self._fail_start("connect", exc) from None

        self.state = McpRuntimeState.DISCOVERING
        self.code = None
        try:
            async with asyncio.timeout(self._discovery_timeout):
                source = await self._await_transport_operation(
                    discover_server_catalog(
                        self.config.name,
                        self._client.session,
                    )
                )
        except asyncio.CancelledError:
            await self._run_close_task()
            raise
        except Exception as exc:
            raise await self._fail_start("discovery", exc) from None
        self._source_catalog = source
        self.state = McpRuntimeState.AWAITING_ACK
        self.code = None
        return source

    def bind_persisted(
        self,
        persisted: PersistedMcpServerCatalog,
    ) -> Mapping[UUID, McpEntryRoute]:
        if self.state is not McpRuntimeState.AWAITING_ACK or self._source_catalog is None:
            raise RuntimeError("MCP runtime is not ready for persisted route binding")
        try:
            routes = bind_server_entries(self._source_catalog, persisted)
        except McpCatalogError as exc:
            self._routes.clear()
            self.state = McpRuntimeState.DRIFTED
            self.code = "mcp_schema_drift"
            failure = _safe_failure(self.config.name, "binding", exc)
            self.last_failure = failure
            raise McpRuntimeError(failure) from None
        self._routes = routes
        return dict(routes)

    def mark_ready(self, generation: UUID) -> None:
        if generation != self.generation:
            raise RuntimeError("stale MCP registration acknowledgement")
        if self.state is McpRuntimeState.READY:
            return
        if self.state is not McpRuntimeState.AWAITING_ACK:
            raise RuntimeError("stale MCP registration acknowledgement")
        if not self._routes and self._source_catalog is None:
            raise RuntimeError("MCP runtime has no discovered catalog")
        self.state = McpRuntimeState.READY
        self.code = None
        self._retry_attempt = 0

    async def refresh(self) -> bool:
        client = self._client
        previous = self._source_catalog
        if client is None or previous is None or self.state not in {
            McpRuntimeState.AWAITING_ACK,
            McpRuntimeState.READY,
        }:
            raise RuntimeError("MCP runtime is not available for discovery refresh")
        previous_state = self.state
        try:
            async with asyncio.timeout(self._discovery_timeout):
                fresh = await self._await_transport_operation(
                    discover_server_catalog(self.config.name, client.session)
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            effective_error = self._effective_terminal_error(exc) or exc
            failure = _safe_failure(self.config.name, "discovery", effective_error)
            self.last_failure = failure
            self.permanent_failure = _is_permanent_failure(effective_error)
            await self._close_unavailable(failure.code)
            raise McpRuntimeError(failure) from None
        if canonical_json_bytes(fresh) != canonical_json_bytes(previous):
            self.state = McpRuntimeState.DRIFTED
            self.code = "mcp_schema_drift"
            return False
        self._source_catalog = fresh
        self.state = previous_state
        self.code = None
        return True

    def enter_backoff(self, *, jitter: float) -> float | None:
        if self.state is McpRuntimeState.CLEANUP_BLOCKED or self.permanent_failure:
            return None
        if self.state is not McpRuntimeState.UNAVAILABLE:
            raise RuntimeError("only an unavailable MCP runtime may enter backoff")
        delay = retry_backoff_seconds(self._retry_attempt, jitter=jitter)
        self._retry_attempt += 1
        self.state = McpRuntimeState.BACKOFF
        return delay

    def begin_retry(self) -> None:
        if self.state is not McpRuntimeState.BACKOFF:
            raise RuntimeError("MCP runtime is not waiting for retry")
        if self._cleanup_incomplete():
            self.state = McpRuntimeState.CLEANUP_BLOCKED
            self.code = "mcp_cleanup_incomplete"
            return
        self.generation = new_uuid7()
        self.state = McpRuntimeState.STARTING
        self.code = "mcp_starting"
        self.last_failure = None
        self._client = None
        self._source_catalog = None
        self._routes.clear()
        self._close_task = None
        self._stdio_cleanup_failed = False
        self._remote_cleanup_pending = False
        self._message_handler.clear_terminal_failure()
        self._transport_failure_signal = McpTransportFailureSignal()
        self._clear_events()

    async def mark_transport_unavailable(self) -> None:
        if self.state not in {
            McpRuntimeState.DISCOVERING,
            McpRuntimeState.AWAITING_ACK,
            McpRuntimeState.READY,
        }:
            return
        terminal_error = self._effective_terminal_error(None)
        if isinstance(terminal_error, McpMessageTooLargeError):
            code = "tool_mcp_message_too_large"
            message = f"MCP server '{self.config.name}' exceeded the inbound message limit"
        elif isinstance(terminal_error, UnsupportedMcpContentEncodingError):
            code = "config_validation_failed"
            message = f"MCP server '{self.config.name}' failed protocol validation"
        else:
            code = "tool_mcp_unavailable"
            message = f"MCP server '{self.config.name}' transport became unavailable"
        failure = McpValidationFailure(
            server=self.config.name,
            stage="connect",
            code=code,
            message=message,
        )
        self.last_failure = failure
        self.permanent_failure = bool(
            terminal_error is not None and _is_permanent_failure(terminal_error)
        )
        await self._close_unavailable(code)

    def _pre_send_failure(self) -> ToolOutput:
        return fail(
            "tool_mcp_unavailable",
            "The MCP runtime or route is no longer current; the request was not sent",
        )

    async def _close_unavailable(self, code: str) -> None:
        try:
            await self._run_close_task()
        finally:
            if self._cleanup_incomplete():
                self.state = McpRuntimeState.CLEANUP_BLOCKED
                self.code = "mcp_cleanup_incomplete"
            elif self.state not in {
                McpRuntimeState.ABSENT,
                McpRuntimeState.BACKOFF,
                McpRuntimeState.CLOSING,
            }:
                self.state = McpRuntimeState.UNAVAILABLE
                self.code = code

    async def _message_too_large_output(self, *, request_was_sent: bool = True) -> ToolOutput:
        await self._close_unavailable("tool_mcp_message_too_large")
        message = (
            "The MCP response exceeded the raw message limit after the request may have "
            "been sent; do not retry blindly"
            if request_was_sent
            else "The MCP connection exceeded the raw message limit; the request was not sent"
        )
        return fail(
            "tool_mcp_message_too_large",
            message,
        )

    async def _outcome_unknown_output(
        self,
        message: str,
        *,
        terminal_error: BaseException | None = None,
    ) -> ToolOutput:
        runtime_code = "tool_execution_outcome_unknown"
        if terminal_error is not None and _is_permanent_failure(terminal_error):
            self.permanent_failure = True
            if _contains_unsupported_content_encoding(terminal_error):
                self.last_failure = _safe_failure(
                    self.config.name,
                    "connect",
                    terminal_error,
                )
                runtime_code = self.last_failure.code
        await self._close_unavailable(runtime_code)
        return fail("tool_execution_outcome_unknown", message)

    async def invoke(
        self,
        entry_id: UUID,
        arguments: Mapping[str, Any],
        *,
        runtime_generation: UUID,
        request_id: UUID,
        max_result_bytes: int,
    ) -> ToolOutput:
        if runtime_generation != self.generation or self.state not in {
            McpRuntimeState.READY,
            McpRuntimeState.AWAITING_ACK,
        }:
            return self._pre_send_failure()
        route = self._routes.get(entry_id)
        client = self._client
        if route is None or not route.enabled or client is None:
            return self._pre_send_failure()
        terminal_error = self._effective_terminal_error(None)
        if isinstance(terminal_error, McpMessageTooLargeError):
            return await self._message_too_large_output(request_was_sent=False)
        if terminal_error is not None:
            runtime_code = "tool_mcp_unavailable"
            if _is_permanent_failure(terminal_error):
                self.permanent_failure = True
                self.last_failure = _safe_failure(
                    self.config.name,
                    "connect",
                    terminal_error,
                )
                runtime_code = self.last_failure.code
            await self._close_unavailable(runtime_code)
            return self._pre_send_failure()
        try:
            async with asyncio.timeout(self._invocation_timeout):
                if route.surface == "tool":
                    request = types.ClientRequest(
                        root=types.CallToolRequest(
                            params=types.CallToolRequestParams(
                                name=route.invocation_identity,
                                arguments=dict(arguments),
                            )
                        )
                    )
                    tool_result = await self._await_transport_operation(
                        client.session.send_request(request, types.CallToolResult)
                    )
                    return map_tool_result(
                        tool_result,
                        request_id=request_id,
                        max_result_bytes=max_result_bytes,
                    )
                if route.surface == "resource":
                    if arguments:
                        return fail("tool_mcp_error", "Static MCP resources take no arguments")
                    resource_result = await self._await_transport_operation(
                        client.session.read_resource(
                            normalized_resource_uri(route.invocation_identity)
                        )
                    )
                    return map_resource_result(
                        resource_result,
                        request_id=request_id,
                        max_result_bytes=max_result_bytes,
                    )
                if route.surface == "resource_template":
                    if any(not isinstance(value, str) for value in arguments.values()):
                        return fail(
                            "tool_mcp_error",
                            "MCP resource template arguments must be strings",
                        )
                    uri = expand_resource_template(
                        route.invocation_identity,
                        cast(Mapping[str, str], arguments),
                    )
                    template_result = await self._await_transport_operation(
                        client.session.read_resource(uri)
                    )
                    return map_resource_result(
                        template_result,
                        request_id=request_id,
                        max_result_bytes=max_result_bytes,
                    )
                if any(not isinstance(value, str) for value in arguments.values()):
                    return fail("tool_mcp_error", "MCP prompt arguments must be strings")
                prompt_result = await self._await_transport_operation(
                    client.session.get_prompt(
                        route.invocation_identity,
                        dict(cast(Mapping[str, str], arguments)),
                    )
                )
                return map_prompt_result(
                    prompt_result,
                    request_id=request_id,
                    max_result_bytes=max_result_bytes,
                )
        except TimeoutError:
            return await self._outcome_unknown_output(
                "The MCP request timed out after it may have been sent; do not retry blindly",
            )
        except McpMessageTooLargeError:
            return await self._message_too_large_output()
        except UnsupportedMcpContentEncodingError as exc:
            return await self._outcome_unknown_output(
                "The MCP connection failed after the request may have been sent; do not retry "
                "blindly",
                terminal_error=exc,
            )
        except McpError as exc:
            if _is_sdk_connection_closed(exc):
                terminal_error = self._effective_terminal_error(exc) or exc
                if isinstance(terminal_error, McpMessageTooLargeError):
                    return await self._message_too_large_output()
                return await self._outcome_unknown_output(
                    "The MCP connection closed after the request may have been sent; do not "
                    "retry blindly",
                    terminal_error=terminal_error,
                )
            return fail("tool_mcp_error", "The MCP server returned a protocol error")
        except ValidationError:
            return fail("tool_mcp_invalid_result", "The MCP server returned an invalid result")
        except McpCatalogError:
            return fail("tool_mcp_error", "The MCP invocation arguments are invalid")
        except asyncio.CancelledError:
            raise
        except httpx.HTTPStatusError as exc:
            return await self._outcome_unknown_output(
                "The MCP connection failed after the request may have been sent; do not retry "
                "blindly",
                terminal_error=exc,
            )
        except Exception as exc:
            terminal_error = self._effective_terminal_error(exc) or exc
            if isinstance(terminal_error, McpMessageTooLargeError):
                return await self._message_too_large_output()
            return await self._outcome_unknown_output(
                "The MCP connection failed after the request may have been sent; do not retry "
                "blindly",
                terminal_error=terminal_error,
            )
        raise AssertionError("unknown MCP surface")  # pragma: no cover

    async def close(self) -> None:
        if self.state is McpRuntimeState.ABSENT:
            return
        self.state = McpRuntimeState.CLOSING
        try:
            await self._run_close_task()
        finally:
            if self._cleanup_incomplete():
                self.state = McpRuntimeState.CLEANUP_BLOCKED
                self.code = "mcp_cleanup_incomplete"
            else:
                self.state = McpRuntimeState.ABSENT
                self.code = None
                self._client = None
                self._routes.clear()
                self._clear_events()


async def _close_runtimes(runtimes: Sequence[McpServerRuntime]) -> None:
    if not runtimes:
        return
    tasks = [
        asyncio.create_task(runtime.close(), name=f"mcp-candidate-close-{runtime.config.name}")
        for runtime in runtimes
    ]
    aggregate = asyncio.gather(*tasks, return_exceptions=True)
    cancelled = False
    while not aggregate.done():
        try:
            await asyncio.shield(aggregate)
        except asyncio.CancelledError:
            cancelled = True
    await aggregate
    if cancelled:
        raise asyncio.CancelledError


async def validate_candidate(
    configs: Sequence[McpServerConfig],
    *,
    client_factory: RuntimeClientFactory | None = None,
    connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
    discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
    candidate_timeout: float = CANDIDATE_TIMEOUT_SECONDS,
    max_parallel: int = VALIDATION_PARALLELISM,
    cleanup_sink: Callable[[Sequence[McpServerRuntime]], None] | None = None,
) -> CandidateValidation:
    """Initialize and discover changed configs without touching active runtimes."""

    if not configs or max_parallel <= 0 or candidate_timeout <= 0:
        raise ValueError("candidate validation requires configs and positive bounds")
    names = [config.name for config in configs]
    if len(names) != len(set(names)):
        raise ValueError("candidate MCP server names must be unique")
    semaphore = asyncio.Semaphore(max_parallel)
    runtimes = {
        config.name: McpServerRuntime(
            config,
            client_factory=client_factory,
            connect_timeout=connect_timeout,
            discovery_timeout=discovery_timeout,
        )
        for config in configs
    }

    async def close_and_report_cleanup() -> None:
        try:
            await _close_runtimes(tuple(runtimes.values()))
        finally:
            if cleanup_sink is not None:
                cleanup_sink(
                    tuple(
                        runtime
                        for runtime in runtimes.values()
                        if runtime.state is McpRuntimeState.CLEANUP_BLOCKED
                    )
                )

    async def validate_one(
        runtime: McpServerRuntime,
    ) -> tuple[SourceMcpServerCatalog | None, McpValidationFailure | None]:
        async with semaphore:
            try:
                return await runtime.start(), None
            except McpRuntimeError as exc:
                return None, exc.failure

    tasks = {
        name: asyncio.create_task(validate_one(runtime), name=f"mcp-validate-{name}")
        for name, runtime in runtimes.items()
    }
    timed_out = False
    try:
        async with asyncio.timeout(candidate_timeout):
            results = await asyncio.gather(*tasks.values())
    except TimeoutError:
        timed_out = True
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        results = [task.result() for task in tasks.values() if not task.cancelled()]
    except asyncio.CancelledError:
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        await close_and_report_cleanup()
        raise

    sources = [source for source, _failure in results if source is not None]
    failures = [failure for _source, failure in results if failure is not None]
    if timed_out:
        completed_names = {source.name for source in sources}
        completed_names.update(failure.server for failure in failures)
        for config in configs:
            if config.name not in completed_names:
                failures.append(
                    McpValidationFailure(
                        server=config.name,
                        stage="candidate",
                        code="config_validation_failed",
                        message=f"MCP server '{config.name}' exceeded the candidate deadline",
                    )
                )
    if failures:
        await close_and_report_cleanup()
        return CandidateValidation(
            source_catalog=None,
            failures=tuple(sorted(failures, key=lambda failure: failure.server)),
            runtimes={},
        )
    try:
        catalog = canonicalize_source_catalog(SourceMcpCatalog(version=1, servers=sources))
    except McpCatalogError as exc:
        await close_and_report_cleanup()
        failure = _safe_failure(configs[0].name, "candidate", exc)
        return CandidateValidation(source_catalog=None, failures=(failure,), runtimes={})
    return CandidateValidation(
        source_catalog=catalog,
        failures=(),
        runtimes=runtimes,
    )
