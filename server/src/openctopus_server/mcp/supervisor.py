"""Conversation-owned Server MCP transports and bounded process admission."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, cast
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.mcp_catalog import wrapped_capability_name
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
)
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.dto.server_mcp import ServerMcpRuntimeError, ServerMcpRuntimeSlot
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.catalog import (
    build_server_persisted_catalog,
    canonicalize_source_catalog,
)
from openctopus_server.mcp.models import (
    ServerMcpEnvelope,
    ServerMcpServerConfig,
    ServerStdioMcpServerConfig,
)
from openctopus_server.mcp.routes import FrozenServerMcpEntryRoute
from openctopus_server.mcp.runtime import (
    CONNECT_TIMEOUT_SECONDS,
    DISCOVERY_TIMEOUT_SECONDS,
    Discoverer,
    RuntimeClientFactory,
    RuntimeFailure,
    RuntimeGeneration,
    RuntimeMessageTooLargeError,
    RuntimeOpenError,
    RuntimeState,
    RuntimeTransportError,
)
from openctopus_server.mcp.scheduler import (
    AdmissionClock,
    AdmissionLease,
    CoordinatorSnapshot,
    EventLoopAdmissionClock,
    ServerMcpBusyError,
    ServerMcpCoordinator,
    ServerMcpUnavailableError,
)
from openctopus_server.mcp.transport import build_runtime_client
from openctopus_server.tools.base import ToolResult
from openctopus_server.tools.result import normalize_tool_result

IDLE_SECONDS = 600.0
MAX_CLIENTS = 256
MAX_STDIO_CLIENTS = 32
MAX_STARTING = 8
REMOTE_RESULT_DRAIN_SECONDS = 60.0


def _config_error(code: ErrorCode, message: str) -> ConfigError:
    return ConfigError(code, message)


def _tool_error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=normalize_tool_result(f"[{code.value}] {message}"), is_error=True, code=code
    )


@dataclass(slots=True)
class ValidatedServerMcpCandidate:
    source_catalog: SourceMcpCatalog
    configs: tuple[ServerMcpServerConfig, ...]
    claimed_names: frozenset[str]
    consumed: bool = False


@dataclass(slots=True)
class _Entry:
    user_id: UUID
    session_id: UUID
    config: ServerMcpServerConfig
    runtime: RuntimeGeneration
    revision: int
    state: str = "opening"
    stale: bool = False
    last_used: float = 0.0
    idle_task: asyncio.Task[None] | None = None
    monitor_task: asyncio.Task[None] | None = None

    @property
    def key(self) -> tuple[UUID, UUID, str]:
        return self.user_id, self.session_id, self.config.name

    @property
    def conversation(self) -> tuple[UUID, UUID]:
        return self.user_id, self.session_id

    @property
    def stdio(self) -> bool:
        return isinstance(self.config, ServerStdioMcpServerConfig)


class ServerMcpSupervisor:
    """One transport per conversation and configured MCP server."""

    def __init__(
        self,
        *,
        client_factory: RuntimeClientFactory | None = None,
        discoverer: Discoverer,
        clock: AdmissionClock | None = None,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
        discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
        max_clients: int = MAX_CLIENTS,
        max_stdio_clients: int = MAX_STDIO_CLIENTS,
        max_starting: int = MAX_STARTING,
        idle_seconds: float = IDLE_SECONDS,
    ) -> None:
        if min(max_clients, max_stdio_clients, max_starting) < 1 or idle_seconds <= 0:
            raise ValueError("Server MCP pool limits must be positive")
        self._clock = clock or EventLoopAdmissionClock()
        self._coordinator = ServerMcpCoordinator(clock=self._clock)
        self._client_factory = client_factory or cast(RuntimeClientFactory, build_runtime_client)
        self._discoverer = discoverer
        self._connect_timeout = connect_timeout
        self._discovery_timeout = discovery_timeout
        self._max_clients = max_clients
        self._max_stdio_clients = max_stdio_clients
        self._max_starting = max_starting
        self._idle_seconds = idle_seconds
        self._lock = asyncio.Lock()
        self._entries: dict[tuple[UUID, UUID, str], _Entry] = {}
        self._candidates: set[RuntimeGeneration] = set()
        self._candidate_opening = 0
        self._runs: dict[tuple[UUID, UUID], int] = {}
        self._session_locks: dict[tuple[UUID, UUID], asyncio.Lock] = {}
        self._catalogs: dict[tuple[UUID, UUID], PersistedMcpCatalog] = {}
        self._forgotten: set[tuple[UUID, UUID]] = set()
        self._authority: ServerMcpEnvelope | None = None
        self._last_errors: dict[str, RuntimeFailure] = {}
        self._retained_leases: dict[RuntimeGeneration, set[AdmissionLease]] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._waitable_tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self._shutdown_task: asyncio.Task[None] | None = None

    @classmethod
    def create_default(
        cls,
        *,
        max_clients: int | None = None,
        max_stdio_clients: int | None = None,
        max_starting: int | None = None,
    ) -> ServerMcpSupervisor:
        from openctopus_server.config import get_settings
        from openctopus_server.mcp.catalog import discover_server_catalog

        settings = get_settings()
        return cls(
            discoverer=discover_server_catalog,
            max_clients=max_clients if max_clients is not None else settings.server_mcp_max_clients,
            max_stdio_clients=(
                max_stdio_clients
                if max_stdio_clients is not None
                else min(
                    settings.server_mcp_max_stdio_clients,
                    max_clients if max_clients is not None else settings.server_mcp_max_clients,
                )
            ),
            max_starting=max_starting
            if max_starting is not None
            else min(
                settings.server_mcp_max_starting,
                max_clients if max_clients is not None else settings.server_mcp_max_clients,
            ),
        )

    def _new_runtime(self, config: ServerMcpServerConfig) -> RuntimeGeneration:
        return RuntimeGeneration(
            config,
            coordinator=self._coordinator,
            client_factory=self._client_factory,
            discoverer=self._discoverer,
            connect_timeout=self._connect_timeout,
            discovery_timeout=self._discovery_timeout,
        )

    def _spawn(
        self, coro: Coroutine[Any, Any, None], *, waitable: bool = False
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        if waitable:
            self._waitable_tasks.add(task)
            task.add_done_callback(self._waitable_tasks.discard)
        return task

    def _counts_locked(self) -> tuple[int, int, int]:
        all_runtimes = [entry.runtime for entry in self._entries.values()] + list(self._candidates)
        return (
            len(all_runtimes),
            sum(isinstance(runtime.config, ServerStdioMcpServerConfig) for runtime in all_runtimes),
            sum(entry.state == "opening" for entry in self._entries.values())
            + self._candidate_opening,
        )

    def _fits_locked(self, config: ServerMcpServerConfig) -> bool:
        clients, stdio, starting = self._counts_locked()
        return (
            clients < self._max_clients
            and (
                not isinstance(config, ServerStdioMcpServerConfig)
                or stdio < self._max_stdio_clients
            )
            and starting < self._max_starting
        )

    def _lru_idle_locked(self, config: ServerMcpServerConfig) -> _Entry | None:
        idle = [
            entry
            for entry in self._entries.values()
            if entry.state == "ready" and self._runs.get(entry.conversation, 0) == 0
        ]
        if not idle:
            return None
        if isinstance(config, ServerStdioMcpServerConfig):
            clients, stdio, _ = self._counts_locked()
            if stdio >= self._max_stdio_clients:
                idle = [entry for entry in idle if entry.stdio]
        return min(idle, key=lambda entry: entry.last_used) if idle else None

    async def _reserve(
        self,
        config: ServerMcpServerConfig,
        user_id: UUID,
        session_id: UUID,
        envelope: ServerMcpEnvelope,
    ) -> _Entry:
        await self._retry_blocked()
        while True:
            async with self._lock:
                if (
                    self._closed
                    or (user_id, session_id) in self._forgotten
                    or not self._runs.get((user_id, session_id))
                ):
                    raise _config_error(
                        ErrorCode.TOOL_MCP_UNAVAILABLE, "Server MCP conversation is unavailable"
                    )
                if not self._authority_matches(envelope):
                    raise _config_error(
                        ErrorCode.SERVER_MCP_CONFIG_CONFLICT, "Server MCP configuration changed"
                    )
                if self._fits_locked(config):
                    entry = _Entry(
                        user_id,
                        session_id,
                        config,
                        self._new_runtime(config),
                        envelope.config_revision,
                        last_used=self._clock.now(),
                    )
                    self._entries[entry.key] = entry
                    return entry
                if self._counts_locked()[2] >= self._max_starting:
                    raise _config_error(
                        ErrorCode.TOOL_MCP_BUSY, "Server MCP startup capacity is exhausted"
                    )
                victim = self._lru_idle_locked(config)
                if victim is None:
                    raise _config_error(
                        ErrorCode.TOOL_MCP_BUSY, "Server MCP client capacity is exhausted"
                    )
                victim.state = "closing"
                if victim.idle_task is not None:
                    victim.idle_task.cancel()
            await self._close_entry(victim)
            if self._entries.get(victim.key) is victim:
                raise _config_error(
                    ErrorCode.TOOL_MCP_BUSY, "Server MCP client cleanup is incomplete"
                )

    async def _close_entry(self, entry: _Entry) -> None:
        async with self._lock:
            if self._entries.get(entry.key) is not entry:
                return
            entry.state = "closing"
            if entry.idle_task is not None and entry.idle_task is not asyncio.current_task():
                entry.idle_task.cancel()
            if entry.monitor_task is not None and entry.monitor_task is not asyncio.current_task():
                entry.monitor_task.cancel()
            entry.stale = True
        await entry.runtime.close()
        async with self._lock:
            if entry.runtime.cleanup_complete:
                if self._entries.get(entry.key) is entry:
                    self._entries.pop(entry.key)
                    self._purge_conversation_locked(entry.conversation)
                retained = self._retained_leases.pop(entry.runtime, set())
            else:
                self._last_errors[entry.config.name] = entry.runtime.last_error or RuntimeFailure(
                    "mcp_cleanup_incomplete", "MCP cleanup did not converge", False
                )
                retained = set()
        for lease in retained:
            await lease.aclose()

    def _purge_conversation_locked(self, conversation: tuple[UUID, UUID]) -> None:
        if self._runs.get(conversation) or any(
            entry.conversation == conversation for entry in self._entries.values()
        ):
            return
        self._catalogs.pop(conversation, None)
        self._session_locks.pop(conversation, None)
        self._forgotten.discard(conversation)

    async def _retry_blocked(self) -> None:
        async with self._lock:
            blocked = [entry for entry in self._entries.values() if entry.state == "closing"]
            candidates = [
                runtime
                for runtime in self._candidates
                if runtime.state is RuntimeState.CLEANUP_BLOCKED
            ]
        for entry in blocked:
            if await entry.runtime.retry_cleanup():
                await self._close_entry(entry)
        for runtime in candidates:
            if await runtime.retry_cleanup():
                async with self._lock:
                    self._candidates.discard(runtime)

    async def _monitor(self, entry: _Entry) -> None:
        try:
            while True:
                event = await entry.runtime.next_event()
                async with self._lock:
                    if self._entries.get(entry.key) is not entry or entry.state != "ready":
                        return
                    entry.stale = True
                    if event == "transport_failed":
                        self._last_errors[entry.config.name] = entry.runtime.transport_failure()
                if event == "transport_failed":
                    return
        except asyncio.CancelledError:
            pass

    async def _idle_expiry(self, entry: _Entry, deadline: float) -> None:
        try:
            await self._clock.sleep_until(deadline)
            async with self._lock:
                if (
                    self._entries.get(entry.key) is not entry
                    or entry.state != "ready"
                    or self._runs.get(entry.conversation, 0)
                    or entry.last_used > deadline - self._idle_seconds
                ):
                    return
                entry.state = "closing"
            await self._close_entry(entry)
        except asyncio.CancelledError:
            pass

    @asynccontextmanager
    async def run(self, *, user_id: UUID, session_id: UUID) -> AsyncIterator[None]:
        conversation = (user_id, session_id)
        async with self._lock:
            if self._closed or conversation in self._forgotten:
                raise _config_error(ErrorCode.TOOL_MCP_UNAVAILABLE, "Server MCP is shutting down")
            self._runs[conversation] = self._runs.get(conversation, 0) + 1
            for entry in self._entries.values():
                if entry.conversation == conversation and entry.idle_task is not None:
                    entry.idle_task.cancel()
                    entry.idle_task = None
        try:
            yield
        finally:
            release = asyncio.create_task(self._release_run(conversation))
            await await_future_cancellation_safe(release)

    async def _release_run(self, conversation: tuple[UUID, UUID]) -> None:
        async with self._lock:
            count = self._runs[conversation] - 1
            if count:
                self._runs[conversation] = count
                return
            del self._runs[conversation]
            now = self._clock.now()
            retire = []
            for entry in self._entries.values():
                if entry.conversation != conversation:
                    continue
                if (
                    entry.state == "ready"
                    and not entry.stale
                    and entry.runtime.state is RuntimeState.READY
                ):
                    entry.last_used = now
                    entry.idle_task = self._spawn(
                        self._idle_expiry(entry, now + self._idle_seconds)
                    )
                elif entry.state != "closing":
                    retire.append(entry)
            self._purge_conversation_locked(conversation)
        for entry in retire:
            await self._close_entry(entry)

    async def prepare(
        self, *, user_id: UUID, session_id: UUID, envelope: ServerMcpEnvelope
    ) -> tuple[ServerMcpEnvelope, Mapping[str, UUID | None]]:
        conversation = (user_id, session_id)
        async with self._lock:
            if conversation in self._forgotten or not self._runs.get(conversation):
                raise _config_error(
                    ErrorCode.TOOL_MCP_UNAVAILABLE, "Server MCP conversation run is not active"
                )
            if not self._authority_matches(envelope):
                raise _config_error(
                    ErrorCode.SERVER_MCP_CONFIG_CONFLICT, "Server MCP configuration changed"
                )
            session_lock = self._session_locks.setdefault(conversation, asyncio.Lock())
        async with session_lock:
            sources: list[SourceMcpServerCatalog] = []
            entries: list[_Entry] = []
            for config in envelope.mcp_servers:
                key = (user_id, session_id, config.name)
                async with self._lock:
                    entry = self._entries.get(key)
                    same = (
                        entry is not None and entry.config.storage_dict() == config.storage_dict()
                    )
                    reuse = (
                        entry is not None
                        and same
                        and entry.state == "ready"
                        and entry.runtime.state is RuntimeState.READY
                    )
                    if entry is not None and entry.idle_task is not None:
                        entry.idle_task.cancel()
                        entry.idle_task = None
                if entry is not None and not reuse:
                    if entry.state == "closing":
                        raise _config_error(
                            ErrorCode.TOOL_MCP_BUSY, "Server MCP cleanup is incomplete"
                        )
                    await self._close_entry(entry)
                if reuse:
                    assert entry is not None
                    try:
                        source = await entry.runtime.rediscover()
                    except RuntimeOpenError as exc:
                        async with self._lock:
                            self._last_errors[config.name] = exc.failure
                        await self._close_entry(entry)
                        raise _config_error(
                            ErrorCode.CONFIG_VALIDATION_FAILED, exc.failure.message
                        ) from None
                else:
                    entry = await self._reserve(config, user_id, session_id, envelope)
                    try:
                        source = await entry.runtime.open()
                    except RuntimeOpenError as exc:
                        async with self._lock:
                            self._last_errors[config.name] = exc.failure
                        await self._close_entry(entry)
                        raise _config_error(
                            ErrorCode.CONFIG_VALIDATION_FAILED, exc.failure.message
                        ) from None
                    async with self._lock:
                        entry.state = "ready"
                sources.append(source)
                entries.append(entry)
            try:
                source_catalog = canonicalize_source_catalog(
                    SourceMcpCatalog(version=1, servers=sources)
                )
                async with self._lock:
                    previous = self._catalogs.get(conversation, envelope.mcp_catalog)
                fresh_by_name = {server.name: server for server in source_catalog.servers}
                catalog_configs = []
                for config in envelope.mcp_servers:
                    selected = config.enabled_capabilities
                    if selected is None or not selected:
                        catalog_configs.append(config)
                        continue
                    fresh = fresh_by_name[config.name]
                    raw_names = (
                        [item.raw_name for item in fresh.tools]
                        + [item.raw_name for item in fresh.resources]
                        + [item.raw_name for item in fresh.resource_templates]
                        + [item.raw_name for item in fresh.prompts]
                    )
                    known = {
                        wrapped_capability_name(config.name, raw_name) for raw_name in raw_names
                    }
                    intersection = [name for name in selected if name in known]
                    catalog_configs.append(
                        config.model_copy(
                            update={
                                "enabled_capabilities": intersection or None,
                            }
                        )
                    )
                catalog = build_server_persisted_catalog(
                    catalog_configs,
                    source_catalog,
                    existing_catalog=previous,
                    entry_id_factory=new_uuid7,
                )
                private = envelope.model_copy(update={"mcp_catalog": catalog})
                persisted = {server.name: server for server in catalog.servers}
                async with self._lock:
                    if (
                        self._closed
                        or conversation in self._forgotten
                        or not self._runs.get(conversation)
                    ):
                        raise _config_error(
                            ErrorCode.TOOL_MCP_UNAVAILABLE, "Server MCP conversation is unavailable"
                        )
                    if not self._authority_matches(envelope):
                        raise _config_error(
                            ErrorCode.SERVER_MCP_CONFIG_CONFLICT, "Server MCP configuration changed"
                        )
                    if any(
                        self._entries.get(entry.key) is not entry or entry.state != "ready"
                        for entry in entries
                    ):
                        raise _config_error(
                            ErrorCode.TOOL_MCP_UNAVAILABLE, "Server MCP runtime changed"
                        )
                    for entry in entries:
                        entry.runtime.bind_private_catalog(
                            persisted[entry.config.name],
                            config_revision=envelope.config_revision,
                            catalog_digest=catalog.digest,
                        )
                        entry.revision = envelope.config_revision
                        entry.stale = False
                        if entry.monitor_task is None or entry.monitor_task.done():
                            entry.monitor_task = self._spawn(self._monitor(entry))
                    self._catalogs[conversation] = catalog
                    generations = {entry.config.name: entry.runtime.generation for entry in entries}
                return private, MappingProxyType(generations)
            except ConfigError:
                raise
            except (ValueError, RuntimeError) as exc:
                raise _config_error(
                    ErrorCode.CONFIG_VALIDATION_FAILED, "Server MCP catalog validation failed"
                ) from exc

    async def preflight(
        self, *, configs: tuple[ServerMcpServerConfig, ...], changed_names: tuple[str, ...]
    ) -> None:
        del configs, changed_names
        if self._closed:
            raise _config_error(ErrorCode.SERVER_MCP_CONFIG_CONFLICT, "Server MCP is shutting down")

    async def validate(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
        validate_servers: tuple[str, ...],
    ) -> ValidatedServerMcpCandidate:
        if self._closed:
            raise _config_error(ErrorCode.SERVER_MCP_CONFIG_CONFLICT, "Server MCP is shutting down")
        changed, selected = frozenset(changed_names), frozenset(validate_servers)
        if (
            not changed
            or len(changed) != len(changed_names)
            or len(selected) != len(validate_servers)
            or not selected.issubset(changed)
            or not selected.issubset({config.name for config in configs})
        ):
            raise _config_error(
                ErrorCode.CONFIG_VALIDATION_FAILED, "Server MCP validation selection is invalid"
            )
        sources = []
        for config in configs:
            if config.name not in selected:
                continue
            await self._retry_blocked()
            while True:
                async with self._lock:
                    if self._closed:
                        raise _config_error(
                            ErrorCode.SERVER_MCP_CONFIG_CONFLICT, "Server MCP is shutting down"
                        )
                    if self._fits_locked(config):
                        runtime = self._new_runtime(config)
                        self._candidates.add(runtime)
                        self._candidate_opening += 1
                        break
                    if self._counts_locked()[2] >= self._max_starting:
                        raise _config_error(
                            ErrorCode.TOOL_MCP_BUSY, "Server MCP startup capacity is exhausted"
                        )
                    victim = self._lru_idle_locked(config)
                    if victim is None:
                        raise _config_error(
                            ErrorCode.TOOL_MCP_BUSY, "Server MCP client capacity is exhausted"
                        )
                    victim.state = "closing"
                await self._close_entry(victim)
                if self._entries.get(victim.key) is victim:
                    raise _config_error(
                        ErrorCode.TOOL_MCP_BUSY, "Server MCP client cleanup is incomplete"
                    )
            try:
                source = await runtime.open()
                sources.append(source)
            except RuntimeOpenError as exc:
                async with self._lock:
                    self._last_errors[config.name] = exc.failure
                raise _config_error(
                    ErrorCode.CONFIG_VALIDATION_FAILED, exc.failure.message
                ) from None
            finally:
                async with self._lock:
                    self._candidate_opening -= 1
                close = asyncio.create_task(runtime.close())
                await await_future_cancellation_safe(close)
                async with self._lock:
                    if runtime.cleanup_complete:
                        self._candidates.discard(runtime)
                    else:
                        self._last_errors[config.name] = runtime.last_error or RuntimeFailure(
                            "mcp_cleanup_incomplete", "MCP cleanup did not converge", False
                        )
            if not runtime.cleanup_complete:
                raise _config_error(
                    ErrorCode.TOOL_MCP_BUSY,
                    f"Server MCP validation cleanup is incomplete for '{config.name}'",
                )
        if self._closed:
            raise _config_error(ErrorCode.SERVER_MCP_CONFIG_CONFLICT, "Server MCP is shutting down")
        return ValidatedServerMcpCandidate(
            canonicalize_source_catalog(SourceMcpCatalog(version=1, servers=sources)),
            configs,
            changed,
        )

    async def discard(self, candidate: ValidatedServerMcpCandidate) -> None:
        candidate.consumed = True

    async def publish(
        self, candidate: ValidatedServerMcpCandidate, envelope: ServerMcpEnvelope
    ) -> None:
        candidate.consumed = True
        await self._set_authority(envelope, candidate.claimed_names)

    async def reconcile(self, envelope: ServerMcpEnvelope) -> None:
        async with self._lock:
            current = self._authority
            if (
                current is not None
                and current.config_revision == envelope.config_revision
                and current.mcp_catalog.digest == envelope.mcp_catalog.digest
            ):
                return
            changed = frozenset(config.name for config in envelope.mcp_servers) | (
                frozenset(config.name for config in current.mcp_servers)
                if current is not None
                else frozenset()
            )
        await self._set_authority(envelope, changed)

    async def start(self, envelope: ServerMcpEnvelope) -> None:
        await self._set_authority(envelope, frozenset())

    async def _set_authority(self, envelope: ServerMcpEnvelope, changed: frozenset[str]) -> None:
        async with self._lock:
            self._authority = envelope
            retire = []
            for entry in self._entries.values():
                if entry.config.name in changed:
                    entry.stale = True
                    if not self._runs.get(entry.conversation) and entry.state == "ready":
                        retire.append(entry)
        for entry in retire:
            await self._close_entry(entry)

    def refresh_names(self, envelope: ServerMcpEnvelope) -> tuple[str, ...]:
        return tuple(config.name for config in envelope.mcp_servers)

    def _authority_matches(self, envelope: ServerMcpEnvelope) -> bool:
        authority = self._authority
        return bool(
            authority is not None
            and authority.config_revision == envelope.config_revision
            and authority.mcp_catalog.digest == envelope.mcp_catalog.digest
        )

    def runtime_snapshot(
        self, envelope: ServerMcpEnvelope | None
    ) -> dict[str, ServerMcpRuntimeSlot]:
        names = {config.name for config in envelope.mcp_servers} if envelope is not None else set()
        names.update(entry.config.name for entry in self._entries.values())
        result = {}
        for name in sorted(names):
            entries = [entry for entry in self._entries.values() if entry.config.name == name]
            failure = self._last_errors.get(name)
            result[name] = ServerMcpRuntimeSlot(
                configured=envelope is not None
                and name in {config.name for config in envelope.mcp_servers},
                active_sessions=sum(
                    entry.state == "ready" and self._runs.get(entry.conversation, 0) > 0
                    for entry in entries
                ),
                idle_sessions=sum(
                    entry.state == "ready" and self._runs.get(entry.conversation, 0) == 0
                    for entry in entries
                ),
                closing_sessions=sum(entry.state == "closing" for entry in entries),
                active_calls=sum(entry.runtime.admission.active_count for entry in entries),
                last_error=ServerMcpRuntimeError(code=failure.code, message=failure.message)
                if failure
                else None,
            )
        return result

    def coordinator_snapshot(self) -> CoordinatorSnapshot:
        return self._coordinator.snapshot()

    async def forget_session(self, user_id: UUID, session_id: UUID) -> None:
        conversation = (user_id, session_id)
        async with self._lock:
            entries = [
                entry for entry in self._entries.values() if entry.conversation == conversation
            ]
            self._forgotten.add(conversation)
            self._catalogs.pop(conversation, None)
            for entry in entries:
                entry.stale = True
            close = not self._runs.get(conversation)
        if close:
            for entry in entries:
                await self._close_entry(entry)
            async with self._lock:
                self._purge_conversation_locked(conversation)

    async def forget_user(self, user_id: UUID) -> None:
        async with self._lock:
            sessions = {
                entry.session_id for entry in self._entries.values() if entry.user_id == user_id
            }
            sessions.update(session for owner, session in self._catalogs if owner == user_id)
            sessions.update(session for owner, session in self._runs if owner == user_id)
        for session_id in sessions:
            await self.forget_session(user_id, session_id)

    async def wait_background(self) -> None:
        await asyncio.gather(*tuple(self._waitable_tasks), return_exceptions=True)

    async def begin_shutdown(self) -> None:
        async with self._lock:
            self._closed = True
            runtimes = [entry.runtime for entry in self._entries.values()] + list(self._candidates)
        for runtime in runtimes:
            await runtime.admission.retire()

    async def shutdown(self) -> None:
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(self._shutdown_impl())
        await await_future_cancellation_safe(self._shutdown_task)

    async def _shutdown_impl(self) -> None:
        await self.begin_shutdown()
        async with self._lock:
            entries = tuple(self._entries.values())
            candidates = tuple(self._candidates)
        for entry in entries:
            await self._close_entry(entry)
        for runtime in candidates:
            await runtime.close()
            if runtime.cleanup_complete:
                self._candidates.discard(runtime)
        await self._coordinator.close()

    def _route_is_current(
        self,
        entry: _Entry | None,
        route: FrozenServerMcpEntryRoute,
        name: str,
        user_id: UUID,
        session_id: UUID,
    ) -> bool:
        if (
            entry is None
            or entry.user_id != user_id
            or entry.session_id != session_id
            or entry.state != "ready"
            or entry.stale
            or entry.runtime.state is not RuntimeState.READY
            or (user_id, session_id) in self._forgotten
            or not self._runs.get((user_id, session_id))
            or route.runtime_generation != entry.runtime.generation
            or route.config_revision != entry.runtime.config_revision
            or route.catalog_digest != entry.runtime.catalog_digest
            or route.config_revision
            != (self._authority.config_revision if self._authority else None)
            or route.final_name != name
        ):
            return False
        bound = entry.runtime.routes.get(route.entry_id)
        return bool(
            bound is not None
            and bound.enabled
            and bound.server == route.server
            and bound.surface == route.surface
            and bound.raw_name == route.raw_name
            and bound.invocation_identity == route.invocation_identity
            and bound.final_name == route.final_name
        )

    @staticmethod
    def _outcome_unknown() -> ToolResult:
        return _tool_error(
            ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
            "MCP call may have executed, but its outcome is unknown; do not replay it blindly",
        )

    async def _close_failed(
        self, entry: _Entry, lease: AdmissionLease, failure: RuntimeFailure | None = None
    ) -> None:
        async with self._lock:
            entry.stale = True
            if failure is not None:
                self._last_errors[entry.config.name] = failure
            self._retained_leases.setdefault(entry.runtime, set()).add(lease)
        await self._close_entry(entry)

    async def _drain_late(
        self, entry: _Entry, invocation: asyncio.Task[ToolResult], lease: AdmissionLease
    ) -> None:
        deadline = asyncio.create_task(
            self._clock.sleep_until(self._clock.now() + REMOTE_RESULT_DRAIN_SECONDS)
        )
        try:
            done, _ = await asyncio.wait(
                {invocation, deadline}, return_when=asyncio.FIRST_COMPLETED
            )
            if invocation in done:
                with contextlib.suppress(BaseException):
                    await invocation
            else:
                await self._close_failed(entry, lease)
        finally:
            deadline.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await deadline
            if entry.runtime.cleanup_complete or entry.state == "ready":
                await lease.aclose()
            else:
                async with self._lock:
                    self._retained_leases.setdefault(entry.runtime, set()).add(lease)

    async def dispatch_server_mcp(
        self,
        *,
        route: FrozenServerMcpEntryRoute,
        user_id: UUID,
        session_id: UUID,
        name: str,
        args: dict[str, object],
        on_issued: Callable[[], None] | None = None,
        issue_guard: Callable[[], bool] | None = None,
    ) -> ToolResult:
        async with self._lock:
            entry = self._entries.get((user_id, session_id, route.server))
            if not self._route_is_current(entry, route, name, user_id, session_id):
                return _tool_error(
                    ErrorCode.TOOL_MCP_UNAVAILABLE,
                    "Server MCP capability changed before it could be called",
                )
            assert entry is not None
            runtime = entry.runtime

            def start(lease: AdmissionLease) -> object:
                if (issue_guard is not None and not issue_guard()) or not self._route_is_current(
                    entry, route, name, user_id, session_id
                ):
                    raise ServerMcpUnavailableError
                task = asyncio.create_task(runtime.invoke(route.entry_id, args))
                runtime.track_invocation(task)
                try:
                    if on_issued is not None:
                        on_issued()
                except BaseException:
                    task.cancel()
                    raise
                return task

            try:
                issued = await runtime.admission.admit_now(user_id, start)
            except ServerMcpBusyError:
                return _tool_error(ErrorCode.TOOL_MCP_BUSY, "Server MCP runtime is busy")
            except ServerMcpUnavailableError:
                return _tool_error(
                    ErrorCode.TOOL_MCP_UNAVAILABLE, "Server MCP runtime is unavailable"
                )
        invocation = cast(asyncio.Task[ToolResult], issued.invocation)
        deadline = asyncio.create_task(self._clock.sleep_until(issued.public_deadline))
        try:
            done, _ = await asyncio.wait(
                {invocation, deadline}, return_when=asyncio.FIRST_COMPLETED
            )
            if invocation in done:
                deadline.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await deadline
                try:
                    result = invocation.result()
                except RuntimeMessageTooLargeError:
                    await self._close_failed(
                        entry,
                        issued.lease,
                        RuntimeFailure(
                            "mcp_message_too_large",
                            "MCP response exceeded the inbound message limit",
                            True,
                        ),
                    )
                    return _tool_error(
                        ErrorCode.TOOL_MCP_MESSAGE_TOO_LARGE,
                        "The MCP response exceeded the raw message limit",
                    )
                except RuntimeTransportError as exc:
                    await self._close_failed(entry, issued.lease, exc.failure)
                    return self._outcome_unknown()
                except asyncio.CancelledError:
                    await self._close_failed(entry, issued.lease)
                    return self._outcome_unknown()
                await issued.lease.aclose()
                return result
            if runtime.is_remote:
                await issued.lease.mark_draining()
                self._spawn(self._drain_late(entry, invocation, issued.lease), waitable=True)
            else:
                await self._close_failed(entry, issued.lease)
            return self._outcome_unknown()
        except asyncio.CancelledError:
            if runtime.is_remote:
                await issued.lease.mark_draining()
                self._spawn(self._drain_late(entry, invocation, issued.lease), waitable=True)
            else:
                await await_future_cancellation_safe(
                    asyncio.create_task(self._close_failed(entry, issued.lease))
                )
            raise
        finally:
            deadline.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await deadline
