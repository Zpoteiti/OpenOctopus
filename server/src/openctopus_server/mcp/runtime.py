"""One bounded Server MCP client/session generation."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal, Protocol, cast
from uuid import UUID

import httpx
from fastmcp.client.messages import MessageHandler
from mcp import types
from mcp.shared.exceptions import McpError
from pydantic import ValidationError

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.mcp_catalog import McpCatalogError, canonical_json_bytes
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpServerCatalog,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
)
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.mcp.catalog import (
    CatalogSession,
    McpEntryRoute,
    bind_server_entries,
    build_server_persisted_catalog,
    discover_server_catalog,
    expand_resource_template,
    normalized_resource_uri,
    with_catalog_digest,
)
from openctopus_server.mcp.models import (
    ServerMcpServerConfig,
    ServerStdioMcpServerConfig,
)
from openctopus_server.mcp.result import (
    map_prompt_result,
    map_resource_result,
    map_tool_result,
)
from openctopus_server.mcp.scheduler import RuntimeAdmission, ServerMcpCoordinator
from openctopus_server.mcp.transport import (
    McpMessageTooLargeError,
    McpTransportError,
    McpTransportFailureSignal,
    RuntimeClient,
    UnsupportedMcpContentEncodingError,
    build_runtime_client,
)
from openctopus_server.tools.base import ToolResult
from openctopus_server.tools.result import normalize_tool_result

CONNECT_TIMEOUT_SECONDS = 30.0
DISCOVERY_TIMEOUT_SECONDS = 30.0
REMOTE_CLEANUP_TIMEOUT_SECONDS = 10.0

type RuntimeEventKind = Literal["list_changed", "transport_failed"]
type RuntimePublicState = Literal[
    "starting",
    "discovering",
    "ready",
    "unavailable",
    "backoff",
    "drifted",
    "draining",
    "cleanup_blocked",
]
type Discoverer = Callable[[str, CatalogSession], Awaitable[SourceMcpServerCatalog]]


class RuntimeClientFactory(Protocol):
    def __call__(
        self,
        config: ServerMcpServerConfig,
        *,
        message_handler: object | None = None,
        transport_failure_signal: McpTransportFailureSignal | None = None,
    ) -> RuntimeClient: ...


class RuntimeState(StrEnum):
    STARTING = "starting"
    DISCOVERING = "discovering"
    READY = "ready"
    UNAVAILABLE = "unavailable"
    BACKOFF = "backoff"
    DRIFTED = "drifted"
    DRAINING = "draining"
    CLEANUP_BLOCKED = "cleanup_blocked"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class RuntimeFailure:
    code: str
    message: str
    permanent: bool


@dataclass(frozen=True, slots=True)
class RuntimeStatusSnapshot:
    state: RuntimePublicState
    origin: Literal["persisted", "candidate"]
    config_revision: int | None
    catalog_digest: str | None
    runtime_generation: UUID | None
    max_concurrent_calls: int
    active_calls: int
    waiting_calls: int
    draining_calls: int
    restart_attempt: int
    last_error: RuntimeFailure | None


class RuntimeOpenError(RuntimeError):
    def __init__(self, failure: RuntimeFailure) -> None:
        self.failure = failure
        super().__init__(failure.message)


class RuntimeTransportError(RuntimeError):
    """An issued request lost a trustworthy result boundary."""

    def __init__(self, message: str, *, failure: RuntimeFailure | None = None) -> None:
        self.failure = failure
        super().__init__(message)


class RuntimeMessageTooLargeError(RuntimeTransportError):
    """An issued request crossed the raw inbound message limit."""


def _is_sdk_connection_closed(error: BaseException) -> bool:
    return bool(
        isinstance(error, McpError)
        and error.error.code == types.CONNECTION_CLOSED
        and error.error.message == "Connection closed"
    )


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


def _contains_protocol_violation(error: BaseException) -> bool:
    return bool(
        _find_nested_exception(error, UnsupportedMcpContentEncodingError) is not None
        or _find_nested_exception(error, ValidationError) is not None
    )


def _tool_error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=normalize_tool_result(f"[{code.value}] {message}"),
        is_error=True,
        code=code,
    )


def _safe_failure(server: str, stage: str, error: BaseException) -> RuntimeFailure:
    if _contains_message_too_large(error):
        return RuntimeFailure(
            code="mcp_message_too_large",
            message=f"MCP server '{server}' exceeded the inbound message limit during {stage}",
            permanent=True,
        )
    if (
        _find_nested_exception(error, FileNotFoundError) is not None
        or _find_nested_exception(error, PermissionError) is not None
    ):
        return RuntimeFailure(
            code="mcp_spawn_failed",
            message=f"MCP server '{server}' could not be started",
            permanent=True,
        )
    catalog_error = _find_nested_exception(error, McpCatalogError)
    if catalog_error is not None:
        return RuntimeFailure(
            code=catalog_error.code,
            message=f"MCP server '{server}' failed bounded catalog validation during {stage}",
            permanent=True,
        )
    mcp_error = _find_nested_exception(error, McpError)
    http_status_error = _find_nested_exception(error, httpx.HTTPStatusError)
    permanent = bool(
        _find_nested_exception(error, ValidationError) is not None
        or _find_nested_exception(error, UnsupportedMcpContentEncodingError) is not None
        or (mcp_error is not None and not _is_sdk_connection_closed(mcp_error))
        or (
            http_status_error is not None
            and http_status_error.response.status_code in {401, 403}
        )
    )
    return RuntimeFailure(
        code="config_validation_failed",
        message=f"MCP server '{server}' failed validation during {stage}",
        permanent=permanent,
    )


class ServerMcpMessageHandler(MessageHandler):
    """Coalesce FastMCP notifications into payload-free runtime events."""

    def __init__(self) -> None:
        self._events: deque[RuntimeEventKind] = deque()
        self._pending: set[RuntimeEventKind] = set()
        self._ready = asyncio.Event()
        self.message_too_large = False
        self.protocol_violation = False

    def _emit(self, event: RuntimeEventKind) -> None:
        if event in self._pending:
            return
        self._pending.add(event)
        self._events.append(event)
        self._ready.set()

    async def on_tool_list_changed(self, message: types.ToolListChangedNotification) -> None:
        del message
        self._emit("list_changed")

    async def on_resource_list_changed(
        self, message: types.ResourceListChangedNotification
    ) -> None:
        del message
        self._emit("list_changed")

    async def on_prompt_list_changed(self, message: types.PromptListChangedNotification) -> None:
        del message
        self._emit("list_changed")

    async def on_exception(self, message: Exception) -> None:
        if _contains_message_too_large(message):
            self.message_too_large = True
        elif _contains_protocol_violation(message):
            self.protocol_violation = True
        self._emit("transport_failed")

    async def next_event(self) -> RuntimeEventKind:
        while not self._events:
            await self._ready.wait()
            self._ready.clear()
        event = self._events.popleft()
        self._pending.remove(event)
        if self._events:
            self._ready.set()
        return event


def _missing_entry_id() -> UUID:
    raise McpCatalogError(
        "config_validation_failed",
        "fresh MCP discovery contains an unknown capability",
    )


class RuntimeGeneration:
    """Exactly one transport and initialized FastMCP session."""

    def __init__(
        self,
        config: ServerMcpServerConfig,
        *,
        coordinator: ServerMcpCoordinator,
        client_factory: RuntimeClientFactory | None = None,
        discoverer: Discoverer = discover_server_catalog,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
        discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
        cleanup_timeout: float = REMOTE_CLEANUP_TIMEOUT_SECONDS,
    ) -> None:
        if connect_timeout <= 0 or discovery_timeout <= 0 or cleanup_timeout <= 0:
            raise ValueError("Server MCP runtime deadlines must be positive")
        self.config = config
        self.generation: UUID | None = None
        self.state = RuntimeState.STARTING
        self.last_error: RuntimeFailure | None = None
        self.restart_attempt = 0
        self.config_revision: int | None = None
        self.catalog_digest: str | None = None
        self.admission: RuntimeAdmission = coordinator.create_runtime(
            max_concurrent_calls=config.max_concurrent_calls
        )
        self._client_factory = client_factory or cast(RuntimeClientFactory, build_runtime_client)
        self._discoverer = discoverer
        self._connect_timeout = connect_timeout
        self._discovery_timeout = discovery_timeout
        self._cleanup_timeout = cleanup_timeout
        self._client: RuntimeClient | None = None
        self._transport_failure_signal = McpTransportFailureSignal()
        self._source_catalog: SourceMcpServerCatalog | None = None
        self._routes: dict[UUID, McpEntryRoute] = {}
        self._handler = ServerMcpMessageHandler()
        self._close_task: asyncio.Task[None] | None = None
        self._client_close_task: asyncio.Task[None] | None = None
        self._invocations: set[asyncio.Task[ToolResult]] = set()

    @property
    def source_catalog(self) -> SourceMcpServerCatalog | None:
        return self._source_catalog

    @property
    def routes(self) -> Mapping[UUID, McpEntryRoute]:
        return MappingProxyType(dict(self._routes))

    @property
    def is_remote(self) -> bool:
        return not isinstance(self.config, ServerStdioMcpServerConfig)

    @property
    def cleanup_complete(self) -> bool:
        return self.state is RuntimeState.CLOSED

    @property
    def invocation_tasks(self) -> tuple[asyncio.Task[ToolResult], ...]:
        return tuple(self._invocations)

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

    def _transport_signal_error(self) -> McpMessageTooLargeError | McpTransportError:
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

    async def open(self) -> SourceMcpServerCatalog:
        if self.state is not RuntimeState.STARTING or self._client is not None:
            raise RuntimeError("Server MCP generation has already started")
        try:
            self._client = self._client_factory(
                self.config,
                message_handler=self._handler,
                transport_failure_signal=self._transport_failure_signal,
            )
            self.generation = new_uuid7()
            async with asyncio.timeout(self._connect_timeout):
                await self._await_transport_operation(self._client.__aenter__())
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception as exc:
            effective_error = self._effective_terminal_error(exc) or exc
            failure = _safe_failure(self.config.name, "connect", effective_error)
            self.last_error = failure
            self.state = RuntimeState.UNAVAILABLE
            await self.close()
            raise RuntimeOpenError(failure) from None

        self.state = RuntimeState.DISCOVERING
        try:
            async with asyncio.timeout(self._discovery_timeout):
                source = await self._await_transport_operation(
                    self._discoverer(self.config.name, self._client.session)
                )
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception as exc:
            effective_error = self._effective_terminal_error(exc) or exc
            failure = _safe_failure(self.config.name, "discovery", effective_error)
            self.last_error = failure
            self.state = RuntimeState.UNAVAILABLE
            await self.close()
            raise RuntimeOpenError(failure) from None
        self._source_catalog = source
        return source

    def bind_authority(
        self,
        persisted: PersistedMcpServerCatalog,
        *,
        config_revision: int,
        catalog_digest: str,
    ) -> bool:
        source = self._source_catalog
        if source is None or self.state is not RuntimeState.DISCOVERING:
            raise RuntimeError("Server MCP generation has not completed discovery")
        existing = with_catalog_digest(
            PersistedMcpCatalog(
                version=1,
                digest="0" * 64,
                servers=[persisted.model_copy(deep=True)],
            )
        )
        try:
            fresh = build_server_persisted_catalog(
                [self.config],
                SourceMcpCatalog(version=1, servers=[source]),
                existing_catalog=existing,
                entry_id_factory=_missing_entry_id,
            )
            if canonical_json_bytes(fresh.servers) != canonical_json_bytes(existing.servers):
                raise McpCatalogError(
                    "config_validation_failed", "fresh MCP discovery differs from authority"
                )
            routes = bind_server_entries(source, persisted)
        except (McpCatalogError, RuntimeError, ValueError):
            self._routes.clear()
            self.config_revision = config_revision
            self.catalog_digest = catalog_digest
            self.state = RuntimeState.DRIFTED
            self.last_error = RuntimeFailure(
                code="tool_mcp_unavailable",
                message=f"MCP server '{self.config.name}' schema differs from saved authority",
                permanent=True,
            )
            return False
        self._routes = dict(routes)
        self.config_revision = config_revision
        self.catalog_digest = catalog_digest
        self.state = RuntimeState.READY
        self.last_error = None
        self.restart_attempt = 0
        return True

    def update_authority(self, *, config_revision: int, catalog_digest: str) -> None:
        self.config_revision = config_revision
        self.catalog_digest = catalog_digest

    def mark_backoff(self, *, restart_attempt: int, failure: RuntimeFailure) -> None:
        self.restart_attempt = restart_attempt
        self.last_error = failure
        self.state = RuntimeState.BACKOFF

    def mark_unavailable(self, failure: RuntimeFailure) -> None:
        self.last_error = failure
        self.state = RuntimeState.UNAVAILABLE

    def mark_draining(self) -> None:
        if self.state is not RuntimeState.CLOSED:
            self.state = RuntimeState.DRAINING

    def _mark_cleanup_blocked(self) -> None:
        self.state = RuntimeState.CLEANUP_BLOCKED
        self.last_error = RuntimeFailure(
            code="mcp_cleanup_incomplete",
            message=f"MCP server '{self.config.name}' cleanup did not converge",
            permanent=False,
        )

    async def next_event(self) -> RuntimeEventKind:
        handler_event = asyncio.create_task(self._handler.next_event())
        transport_failure = asyncio.create_task(self._transport_failure_signal.wait())
        try:
            done, _ = await asyncio.wait(
                {handler_event, transport_failure},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if transport_failure in done:
                return "transport_failed"
            return handler_event.result()
        finally:
            for task in (handler_event, transport_failure):
                if not task.done():
                    task.cancel()
            await asyncio.gather(handler_event, transport_failure, return_exceptions=True)

    def transport_failure(self) -> RuntimeFailure:
        terminal_error = self._effective_terminal_error(None)
        if terminal_error is not None:
            failure = _safe_failure(self.config.name, "runtime", terminal_error)
            if failure.permanent:
                return failure
        return RuntimeFailure(
            code="tool_mcp_unavailable",
            message=f"MCP server '{self.config.name}' transport became unavailable",
            permanent=False,
        )

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
        if error is not None and _contains_protocol_violation(error):
            return UnsupportedMcpContentEncodingError(
                "MCP inbound protocol validation failed"
            )
        if error is not None and not _is_sdk_connection_closed(error):
            return error
        if self._handler.message_too_large:
            return McpMessageTooLargeError("MCP inbound message exceeded its raw byte limit")
        if self._handler.protocol_violation:
            return UnsupportedMcpContentEncodingError(
                "MCP inbound protocol validation failed"
            )
        client = self._client
        terminal_error = (
            getattr(client.transport, "terminal_error", None) if client is not None else None
        )
        if isinstance(terminal_error, BaseException):
            return terminal_error
        return error

    def _issued_transport_error(self, error: BaseException) -> RuntimeTransportError:
        if isinstance(error, McpMessageTooLargeError):
            return RuntimeMessageTooLargeError(
                "Server MCP response exceeded the raw message limit"
            )
        failure = _safe_failure(self.config.name, "runtime", error)
        return RuntimeTransportError(
            "Server MCP transport lost the result boundary",
            failure=failure if failure.permanent else None,
        )

    async def refresh_authority(
        self,
        persisted: PersistedMcpServerCatalog,
        *,
        config_revision: int,
        catalog_digest: str,
    ) -> bool:
        client = self._client
        if client is None or self.state is not RuntimeState.READY:
            raise RuntimeError("Server MCP generation is not ready for rediscovery")
        self.state = RuntimeState.DISCOVERING
        try:
            async with asyncio.timeout(self._discovery_timeout):
                self._source_catalog = await self._await_transport_operation(
                    self._discoverer(self.config.name, client.session)
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            effective_error = self._effective_terminal_error(exc) or exc
            failure = _safe_failure(self.config.name, "discovery", effective_error)
            self.last_error = failure
            self.state = RuntimeState.UNAVAILABLE
            raise RuntimeOpenError(failure) from None
        return self.bind_authority(
            persisted,
            config_revision=config_revision,
            catalog_digest=catalog_digest,
        )

    def track_invocation(self, task: asyncio.Task[ToolResult]) -> None:
        self._invocations.add(task)
        task.add_done_callback(self._invocations.discard)

    async def _invoke_protocol(
        self,
        route: McpEntryRoute,
        client: RuntimeClient,
        arguments: Mapping[str, Any],
    ) -> ToolResult:
        if route.surface == "tool":
            request = types.ClientRequest(
                root=types.CallToolRequest(
                    params=types.CallToolRequestParams(
                        name=route.invocation_identity,
                        arguments=dict(arguments),
                    )
                )
            )
            tool_result = await client.session.send_request(request, types.CallToolResult)
            return map_tool_result(tool_result)
        if route.surface == "resource":
            if arguments:
                return _tool_error(
                    ErrorCode.TOOL_MCP_ERROR,
                    "Static MCP resources do not accept arguments",
                )
            resource_result = await client.session.read_resource(
                normalized_resource_uri(route.invocation_identity)
            )
            return map_resource_result(resource_result)
        if route.surface == "resource_template":
            if any(not isinstance(value, str) for value in arguments.values()):
                return _tool_error(
                    ErrorCode.TOOL_MCP_ERROR,
                    "MCP resource template arguments must be strings",
                )
            uri = expand_resource_template(
                route.invocation_identity,
                cast(Mapping[str, str], arguments),
            )
            template_result = await client.session.read_resource(uri)
            return map_resource_result(template_result)
        if any(not isinstance(value, str) for value in arguments.values()):
            return _tool_error(
                ErrorCode.TOOL_MCP_ERROR,
                "MCP prompt arguments must be strings",
            )
        prompt_result = await client.session.get_prompt(
            route.invocation_identity,
            dict(cast(Mapping[str, str], arguments)),
        )
        return map_prompt_result(prompt_result)

    async def invoke(self, entry_id: UUID, arguments: Mapping[str, Any]) -> ToolResult:
        terminal_error = self._effective_terminal_error(None)
        if terminal_error is not None:
            raise self._issued_transport_error(terminal_error) from None
        route = self._routes.get(entry_id)
        client = self._client
        if self.state not in {RuntimeState.READY, RuntimeState.DRAINING}:
            raise RuntimeTransportError("Server MCP generation is not invokable")
        if route is None or not route.enabled or client is None:
            raise RuntimeTransportError("Server MCP route is not bound")
        try:
            return await self._await_transport_operation(
                self._invoke_protocol(route, client, arguments)
            )
        except McpMessageTooLargeError as exc:
            raise self._issued_transport_error(exc) from None
        except McpError as exc:
            if _is_sdk_connection_closed(exc):
                terminal_error = self._effective_terminal_error(exc) or exc
                raise self._issued_transport_error(terminal_error) from None
            return _tool_error(ErrorCode.TOOL_MCP_ERROR, "The MCP server returned an error")
        except ValidationError:
            return _tool_error(
                ErrorCode.TOOL_MCP_INVALID_RESULT,
                "The MCP server returned an invalid result",
            )
        except McpCatalogError:
            return _tool_error(
                ErrorCode.TOOL_MCP_ERROR,
                "MCP invocation arguments are invalid",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            terminal_error = self._effective_terminal_error(exc) or exc
            raise self._issued_transport_error(terminal_error) from None

    async def close(self) -> None:
        if self.state is RuntimeState.CLOSED:
            return
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_impl(), name=f"server-mcp-close-{self.config.name}"
            )
        await await_future_cancellation_safe(self._close_task)

    async def _close_impl(self) -> None:
        self.mark_draining()
        await self.admission.retire()
        client = self._client
        if client is not None:
            if self._client_close_task is None:
                self._client_close_task = asyncio.create_task(
                    client.close(), name=f"server-mcp-client-close-{self.config.name}"
                )
            if self.is_remote:
                done, _ = await asyncio.wait(
                    {self._client_close_task}, timeout=self._cleanup_timeout
                )
                if self._client_close_task not in done:
                    self._mark_cleanup_blocked()
                    return
            try:
                await self._client_close_task
            except asyncio.CancelledError:
                self._mark_cleanup_blocked()
                return
            except Exception:
                self._mark_cleanup_blocked()
                return
            if getattr(client.transport, "cleanup_incomplete", False):
                self._mark_cleanup_blocked()
                return
        await self._finish_closed()

    async def _finish_closed(self) -> None:
        pending = [task for task in self._invocations if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._routes.clear()
        self.state = RuntimeState.CLOSED

    async def retry_cleanup(self) -> bool:
        if self.state is not RuntimeState.CLEANUP_BLOCKED:
            return self.cleanup_complete
        client_close = self._client_close_task
        if client_close is not None and not client_close.done():
            return False
        if client_close is not None:
            try:
                await client_close
            except BaseException:
                self._client_close_task = None
            else:
                client = self._client
                if client is None or not getattr(client.transport, "cleanup_incomplete", False):
                    await self._finish_closed()
                    return True
                self._client_close_task = None
        self._close_task = None
        await self.close()
        return self.cleanup_complete

    async def cancel_pending_cleanup(self) -> None:
        task = self._client_close_task
        if task is None or task.done():
            return
        task.cancel()
        await asyncio.wait({task}, timeout=self._cleanup_timeout)

    def snapshot(
        self,
        *,
        origin: Literal["persisted", "candidate"],
    ) -> RuntimeStatusSnapshot:
        public_state = (
            "draining"
            if self.state is RuntimeState.CLOSED
            else cast(RuntimePublicState, self.state.value)
        )
        return RuntimeStatusSnapshot(
            state=public_state,
            origin=origin,
            config_revision=self.config_revision if origin == "persisted" else None,
            catalog_digest=self.catalog_digest if origin == "persisted" else None,
            runtime_generation=self.generation,
            max_concurrent_calls=self.config.max_concurrent_calls,
            active_calls=self.admission.active_count,
            waiting_calls=self.admission.waiting_count,
            draining_calls=self.admission.draining_count,
            restart_attempt=self.restart_attempt,
            last_error=self.last_error,
        )


__all__ = [
    "CONNECT_TIMEOUT_SECONDS",
    "DISCOVERY_TIMEOUT_SECONDS",
    "REMOTE_CLEANUP_TIMEOUT_SECONDS",
    "Discoverer",
    "RuntimeClientFactory",
    "RuntimeFailure",
    "RuntimeGeneration",
    "RuntimeMessageTooLargeError",
    "RuntimeOpenError",
    "RuntimePublicState",
    "RuntimeState",
    "RuntimeStatusSnapshot",
    "RuntimeTransportError",
    "ServerMcpMessageHandler",
]
