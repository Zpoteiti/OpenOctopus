"""Composable Client-side MCP validation and active runtime primitives."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from uuid import UUID

import httpx
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
    create_fastmcp_client,
    create_mcp_http_client,
)
from openoctopus_client.protocol import new_uuid7
from openoctopus_client.tools.common import ToolOutput, fail

CONNECT_TIMEOUT_SECONDS = 30.0
DISCOVERY_TIMEOUT_SECONDS = 30.0
INVOCATION_TIMEOUT_SECONDS = 60.0
CANDIDATE_TIMEOUT_SECONDS = 300.0
VALIDATION_PARALLELISM = 4

type ValidationStage = Literal["connect", "discovery", "binding", "candidate", "cleanup"]


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


type RuntimeClientFactory = Callable[[McpServerConfig], RuntimeClient]


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
) -> httpx.AsyncClient:
    return create_mcp_http_client(headers=headers, timeout=timeout, auth=auth)


def build_runtime_client(config: McpServerConfig) -> RuntimeClient:
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
        )
    elif isinstance(config, StreamableHttpMcpServerConfig):
        transport = StreamableHttpTransport(
            config.url,
            headers=_plain_secrets(config),
            httpx_client_factory=_runtime_http_client,
        )
    elif isinstance(config, SseMcpServerConfig):
        transport = SSETransport(
            config.url,
            headers=_plain_secrets(config),
            httpx_client_factory=_runtime_http_client,
        )
    else:  # pragma: no cover - the strict tagged union is exhaustive
        raise TypeError("unsupported MCP transport")
    return cast(RuntimeClient, create_fastmcp_client(transport))


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
    if isinstance(error, FileNotFoundError | PermissionError | McpCatalogError | ValidationError):
        return True
    return isinstance(error, httpx.HTTPStatusError) and error.response.status_code in {401, 403}


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
        client_factory: RuntimeClientFactory = build_runtime_client,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
        discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
        invocation_timeout: float = INVOCATION_TIMEOUT_SECONDS,
    ) -> None:
        if connect_timeout <= 0 or discovery_timeout <= 0 or invocation_timeout <= 0:
            raise ValueError("MCP runtime deadlines must be positive")
        self.config = config
        self.generation = new_uuid7()
        self.state = McpRuntimeState.STARTING
        self.code: str | None = "mcp_starting"
        self.last_failure: McpValidationFailure | None = None
        self.permanent_failure = False
        self._client_factory = client_factory
        self._connect_timeout = connect_timeout
        self._discovery_timeout = discovery_timeout
        self._invocation_timeout = invocation_timeout
        self._client: RuntimeClient | None = None
        self._source_catalog: SourceMcpServerCatalog | None = None
        self._routes: dict[UUID, McpEntryRoute] = {}
        self._close_task: asyncio.Task[None] | None = None
        self._close_failed = False
        self._retry_attempt = 0

    @property
    def source_catalog(self) -> SourceMcpServerCatalog | None:
        return self._source_catalog

    @property
    def routes(self) -> Mapping[UUID, McpEntryRoute]:
        return dict(self._routes)

    async def _run_close_task(self) -> None:
        if self._close_task is None:
            client = self._client

            async def close_client() -> None:
                if client is not None:
                    try:
                        await client.close()
                    except Exception:
                        self._close_failed = True

            self._close_task = asyncio.create_task(
                close_client(),
                name=f"mcp-close-{self.config.name}",
            )
        cancelled = False
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                cancelled = True
        await self._close_task
        if cancelled:
            raise asyncio.CancelledError

    def _cleanup_incomplete(self) -> bool:
        return bool(
            self._close_failed
            or (
                self._client is not None
                and getattr(self._client.transport, "cleanup_incomplete", False)
            )
        )

    async def _fail_start(
        self,
        stage: ValidationStage,
        error: BaseException,
    ) -> McpRuntimeError:
        failure = _safe_failure(self.config.name, stage, error)
        self.last_failure = failure
        self.permanent_failure = _is_permanent_failure(error)
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
            self._client = self._client_factory(self.config)
            async with asyncio.timeout(self._connect_timeout):
                await self._client.__aenter__()
        except asyncio.CancelledError:
            await self._run_close_task()
            raise
        except Exception as exc:
            raise await self._fail_start("connect", exc) from None

        self.state = McpRuntimeState.DISCOVERING
        self.code = None
        try:
            async with asyncio.timeout(self._discovery_timeout):
                source = await discover_server_catalog(
                    self.config.name,
                    self._client.session,
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
        if generation != self.generation or self.state is not McpRuntimeState.AWAITING_ACK:
            raise RuntimeError("stale MCP registration acknowledgement")
        if not self._routes and self._source_catalog is None:
            raise RuntimeError("MCP runtime has no discovered catalog")
        self.state = McpRuntimeState.READY
        self.code = None

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
                fresh = await discover_server_catalog(self.config.name, client.session)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failure = _safe_failure(self.config.name, "discovery", exc)
            self.last_failure = failure
            self.permanent_failure = _is_permanent_failure(exc)
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
        self._close_failed = False

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
            else:
                self.state = McpRuntimeState.UNAVAILABLE
                self.code = code

    async def invoke(
        self,
        entry_id: UUID,
        arguments: Mapping[str, Any],
        *,
        runtime_generation: UUID,
    ) -> ToolOutput:
        if runtime_generation != self.generation or self.state is not McpRuntimeState.READY:
            return self._pre_send_failure()
        route = self._routes.get(entry_id)
        client = self._client
        if route is None or not route.enabled or client is None:
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
                    tool_result = await client.session.send_request(
                        request, types.CallToolResult
                    )
                    return map_tool_result(tool_result)
                if route.surface == "resource":
                    if arguments:
                        return fail("tool_mcp_error", "Static MCP resources take no arguments")
                    resource_result = await client.session.read_resource(
                        normalized_resource_uri(route.invocation_identity)
                    )
                    return map_resource_result(resource_result)
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
                    template_result = await client.session.read_resource(uri)
                    return map_resource_result(template_result)
                if any(not isinstance(value, str) for value in arguments.values()):
                    return fail("tool_mcp_error", "MCP prompt arguments must be strings")
                prompt_result = await client.session.get_prompt(
                    route.invocation_identity,
                    dict(cast(Mapping[str, str], arguments)),
                )
                return map_prompt_result(prompt_result)
        except TimeoutError:
            return fail(
                "tool_execution_outcome_unknown",
                "The MCP request timed out after it may have been sent; do not retry blindly",
            )
        except McpMessageTooLargeError:
            await self._close_unavailable("tool_mcp_message_too_large")
            return fail(
                "tool_mcp_message_too_large",
                "The MCP response exceeded the raw message limit after the request may have "
                "been sent; do not retry blindly",
            )
        except McpError:
            return fail("tool_mcp_error", "The MCP server returned a protocol error")
        except ValidationError:
            return fail("tool_mcp_invalid_result", "The MCP server returned an invalid result")
        except McpCatalogError:
            return fail("tool_mcp_error", "The MCP invocation arguments are invalid")
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._close_unavailable("tool_execution_outcome_unknown")
            return fail(
                "tool_execution_outcome_unknown",
                "The MCP connection failed after the request may have been sent; do not retry "
                "blindly",
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
    client_factory: RuntimeClientFactory = build_runtime_client,
    connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
    discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
    candidate_timeout: float = CANDIDATE_TIMEOUT_SECONDS,
    max_parallel: int = VALIDATION_PARALLELISM,
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
        await _close_runtimes(tuple(runtimes.values()))
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
        await _close_runtimes(tuple(runtimes.values()))
        return CandidateValidation(
            source_catalog=None,
            failures=tuple(sorted(failures, key=lambda failure: failure.server)),
            runtimes={},
        )
    try:
        catalog = canonicalize_source_catalog(SourceMcpCatalog(version=1, servers=sources))
    except McpCatalogError as exc:
        await _close_runtimes(tuple(runtimes.values()))
        failure = _safe_failure(configs[0].name, "candidate", exc)
        return CandidateValidation(source_catalog=None, failures=(failure,), runtimes={})
    return CandidateValidation(
        source_catalog=catalog,
        failures=(),
        runtimes=runtimes,
    )
