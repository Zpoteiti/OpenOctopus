"""Process-lifetime authority, invocation, and cleanup for shared Server MCP."""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, cast
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.mcp_catalog import canonical_json_bytes
from openctopus_server.devices.mcp_models import SourceMcpCatalog, SourceMcpServerCatalog
from openctopus_server.dto.server_mcp import (
    ServerMcpRuntimeError,
    ServerMcpRuntimeSlot,
    ServerMcpRuntimeStatus,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.catalog import canonicalize_source_catalog
from openctopus_server.mcp.models import ServerMcpEnvelope, ServerMcpServerConfig
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
    RuntimePublicState,
    RuntimeState,
    RuntimeStatusSnapshot,
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

CANDIDATE_TIMEOUT_SECONDS = 300.0
VALIDATION_PARALLELISM = 4
GENERATION_DRAIN_SECONDS = 60.0
REMOTE_RESULT_DRAIN_SECONDS = 60.0
PROCESS_RESERVED_NAME_MAX = 16


@dataclass(slots=True)
class ValidatedServerMcpCandidate:
    source_catalog: SourceMcpCatalog
    configs: tuple[ServerMcpServerConfig, ...]
    runtimes: dict[str, RuntimeGeneration]
    claimed_names: frozenset[str]
    consumed: bool = False


@dataclass(frozen=True, slots=True)
class RuntimeSlotSnapshot:
    configured: bool
    active: RuntimeStatusSnapshot | None
    draining: RuntimeStatusSnapshot | None


@dataclass(slots=True)
class _RuntimeSlot:
    config: ServerMcpServerConfig | None = None
    active: RuntimeGeneration | None = None
    draining: RuntimeGeneration | None = None
    draining_origin: Literal["persisted", "candidate"] = "persisted"
    placeholder_state: RuntimeState = RuntimeState.STARTING
    placeholder_error: RuntimeFailure | None = None
    restart_attempt: int = 0
    deferred_failure: RuntimeGeneration | None = None


def _error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=normalize_tool_result(f"[{code.value}] {message}"),
        is_error=True,
        code=code,
    )


def _conflict(message: str) -> ConfigError:
    return ConfigError(ErrorCode.SERVER_MCP_CONFIG_CONFLICT, message)


def retry_backoff_seconds(attempt: int, *, jitter: float) -> float:
    if attempt < 0 or not 0 <= jitter <= 1:
        raise ValueError("retry backoff inputs are outside their valid ranges")
    base = min(60.0, float(2 ** min(attempt, 6)))
    return min(60.0, base * (0.8 + 0.4 * jitter))


class ServerMcpSupervisor:
    """Own all process-local Server MCP generations and admission state."""

    def __init__(
        self,
        *,
        client_factory: RuntimeClientFactory | None = None,
        discoverer: Discoverer,
        clock: AdmissionClock | None = None,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
        discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
        candidate_timeout: float = CANDIDATE_TIMEOUT_SECONDS,
        validation_parallelism: int = VALIDATION_PARALLELISM,
        jitter: Callable[[], float] | None = None,
    ) -> None:
        if candidate_timeout <= 0 or validation_parallelism <= 0:
            raise ValueError("Server MCP candidate bounds must be positive")
        self._clock = clock or EventLoopAdmissionClock()
        self._coordinator = ServerMcpCoordinator(clock=self._clock)
        self._client_factory = client_factory or cast(RuntimeClientFactory, build_runtime_client)
        self._discoverer = discoverer
        self._connect_timeout = connect_timeout
        self._discovery_timeout = discovery_timeout
        self._candidate_timeout = candidate_timeout
        self._validation_admission = asyncio.Semaphore(validation_parallelism)
        self._jitter = jitter or random.random
        self._lock = asyncio.Lock()
        self._slots: dict[str, _RuntimeSlot] = {}
        self._transition_names: set[str] = set()
        self._candidate_runtimes: dict[RuntimeGeneration, str] = {}
        self._candidate_open_tasks: dict[
            RuntimeGeneration, asyncio.Task[SourceMcpServerCatalog]
        ] = {}
        self._authority: ServerMcpEnvelope | None = None
        self._transient_tasks: set[asyncio.Task[None]] = set()
        self._lifecycle_tasks: set[asyncio.Task[None]] = set()
        self._monitor_tasks: dict[RuntimeGeneration, asyncio.Task[None]] = {}
        self._recovery_tasks: dict[str, asyncio.Task[None]] = {}
        self._recovery_requested: set[str] = set()
        self._cleanup_retry_runtimes: set[RuntimeGeneration] = set()
        self._retained_leases: dict[RuntimeGeneration, set[AdmissionLease]] = {}
        self._begin_shutdown_task: asyncio.Task[None] | None = None
        self._shutdown_task: asyncio.Task[None] | None = None
        self._shutdown_complete = False
        self._closed = False

    @classmethod
    def create_default(cls) -> ServerMcpSupervisor:
        from openctopus_server.mcp.catalog import discover_server_catalog

        return cls(discoverer=discover_server_catalog)

    async def preflight(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
    ) -> None:
        """Check the single-draining/name-credit invariant before DB work."""

        async with self._lock:
            self._preflight_locked(configs, changed_names)

    def _preflight_locked(
        self,
        configs: Sequence[ServerMcpServerConfig],
        changed_names: Sequence[str],
    ) -> None:
        if self._closed:
            raise _conflict("Server MCP supervisor is shutting down")
        for name in changed_names:
            slot = self._slots.get(name)
            if slot is not None and slot.draining is not None:
                raise _conflict(f"Server MCP server '{name}' is still draining")
        candidate_names = {config.name for config in configs}
        cleanup_names = {
            name
            for name, slot in self._slots.items()
            if slot.draining is not None
            or (slot.active is not None and name not in candidate_names)
        }
        if len(candidate_names | cleanup_names) > PROCESS_RESERVED_NAME_MAX:
            raise _conflict("Server MCP process-reserved name capacity is exhausted")

    def _new_runtime(self, config: ServerMcpServerConfig) -> RuntimeGeneration:
        return RuntimeGeneration(
            config,
            coordinator=self._coordinator,
            client_factory=self._client_factory,
            discoverer=self._discoverer,
            connect_timeout=self._connect_timeout,
            discovery_timeout=self._discovery_timeout,
        )

    async def validate(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
        validate_servers: tuple[str, ...],
    ) -> ValidatedServerMcpCandidate:
        changed = frozenset(changed_names)
        validate = frozenset(validate_servers)
        selected = {config.name: config for config in configs if config.name in validate}
        if (
            not changed
            or len(changed) != len(changed_names)
            or len(validate) != len(validate_servers)
            or not validate.issubset(changed)
            or set(selected) != validate
        ):
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                "Server MCP validation selection is invalid",
            )

        async def open_one(
            runtime: RuntimeGeneration,
        ) -> SourceMcpServerCatalog:
            async with self._validation_admission:
                return await runtime.open()

        async with self._lock:
            if self._transition_names.intersection(changed):
                raise _conflict("A Server MCP transition is already in progress")
            self._preflight_locked(configs, changed_names)
            self._transition_names.update(changed)
            runtimes = {name: self._new_runtime(config) for name, config in selected.items()}
            tasks = {
                name: asyncio.create_task(
                    open_one(runtime), name=f"server-mcp-candidate-{name}"
                )
                for name, runtime in runtimes.items()
            }
            for name, runtime in runtimes.items():
                self._candidate_runtimes[runtime] = name
                self._candidate_open_tasks[runtime] = tasks[name]
        try:
            async with asyncio.timeout(self._candidate_timeout):
                sources = await asyncio.gather(*tasks.values())
            source_catalog = canonicalize_source_catalog(
                SourceMcpCatalog(version=1, servers=list(sources))
            )
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(
                self._discard_runtimes(runtimes, changed, open_tasks=tuple(tasks.values())),
                name="server-mcp-candidate-cancel-cleanup",
            )
            await await_future_cancellation_safe(cleanup)
            raise
        except BaseException as exc:
            cleanup = asyncio.create_task(
                self._discard_runtimes(runtimes, changed, open_tasks=tuple(tasks.values())),
                name="server-mcp-candidate-failure-cleanup",
            )
            await await_future_cancellation_safe(cleanup)
            failure: RuntimeFailure | None = None
            for task in tasks.values():
                if not task.done() or task.cancelled():
                    continue
                task_error = task.exception()
                if isinstance(task_error, RuntimeOpenError):
                    failure = task_error.failure
                    break
            if failure is not None:
                with contextlib.suppress(ValueError):
                    code = ErrorCode(failure.code)
                    raise ConfigError(code, failure.message) from exc
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                "Server MCP candidate validation failed",
            ) from exc
        async with self._lock:
            for runtime in runtimes.values():
                self._candidate_open_tasks.pop(runtime, None)
            shutting_down = self._closed
        if shutting_down:
            cleanup = asyncio.create_task(
                self._discard_runtimes(runtimes, changed),
                name="server-mcp-candidate-shutdown-cleanup",
            )
            await await_future_cancellation_safe(cleanup)
            raise _conflict("Server MCP supervisor is shutting down")
        return ValidatedServerMcpCandidate(
            source_catalog=source_catalog,
            configs=configs,
            runtimes=runtimes,
            claimed_names=changed,
        )

    async def discard(self, candidate: ValidatedServerMcpCandidate) -> None:
        async with self._lock:
            if candidate.consumed:
                return
            candidate.consumed = True
        cleanup = asyncio.create_task(
            self._discard_runtimes(candidate.runtimes, candidate.claimed_names),
            name="server-mcp-candidate-discard",
        )
        await await_future_cancellation_safe(cleanup)

    async def _discard_runtimes(
        self,
        runtimes: Mapping[str, RuntimeGeneration],
        claimed_names: frozenset[str],
        *,
        open_tasks: tuple[asyncio.Task[SourceMcpServerCatalog], ...] = (),
    ) -> None:
        registered: list[tuple[str, RuntimeGeneration]] = []
        tasks_to_cancel = set(open_tasks)
        async with self._lock:
            for name, runtime in runtimes.items():
                tracked_task = self._candidate_open_tasks.pop(runtime, None)
                if tracked_task is not None:
                    tasks_to_cancel.add(tracked_task)
                self._candidate_runtimes.pop(runtime, None)
                if runtime.cleanup_complete:
                    continue
                slot = self._slots.setdefault(name, _RuntimeSlot())
                if slot.draining is not None and slot.draining is not runtime:
                    raise RuntimeError("Candidate cleanup lost its single-draining lease")
                if slot.draining is None:
                    runtime.mark_draining()
                    slot.draining = runtime
                    slot.draining_origin = "candidate"
                registered.append((name, runtime))
            self._transition_names.difference_update(claimed_names)
        for task in tasks_to_cancel:
            task.cancel()
        tasks = [
            self._spawn_transient(self._close_draining(name, runtime))
            for name, runtime in registered
        ]
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        if tasks:
            await await_future_cancellation_safe(asyncio.gather(*tasks, return_exceptions=True))
        for name in claimed_names:
            await self._continue_deferred_failure(name)
            await self._ensure_recovery(name)

    async def publish(
        self,
        candidate: ValidatedServerMcpCandidate | None,
        envelope: ServerMcpEnvelope,
    ) -> None:
        task = asyncio.create_task(
            self._publish_impl(candidate, envelope),
            name="server-mcp-publish",
        )
        await await_future_cancellation_safe(task)

    async def reconcile(self, envelope: ServerMcpEnvelope) -> None:
        """Converge process state to committed durable authority without replay."""

        await self.publish(None, envelope)

    async def _publish_impl(
        self,
        candidate: ValidatedServerMcpCandidate | None,
        envelope: ServerMcpEnvelope,
    ) -> None:
        candidate_runtimes: Mapping[str, RuntimeGeneration] = {}
        configs = {config.name: config for config in envelope.mcp_servers}
        persisted = {server.name: server for server in envelope.mcp_catalog.servers}
        retire: list[tuple[str, RuntimeGeneration]] = []
        close_now: list[tuple[str, RuntimeGeneration]] = []
        recover: list[str] = []
        monitor: list[tuple[str, RuntimeGeneration]] = []
        deferred: list[str] = []
        consume_claims = False

        async with self._lock:
            if self._closed:
                raise _conflict("Server MCP supervisor is shutting down")
            previous = self._authority
            previous_persisted = (
                {server.name: server for server in previous.mcp_catalog.servers}
                if previous is not None
                else {}
            )
            if candidate is not None and not candidate.consumed:
                candidate.consumed = True
                candidate_runtimes = candidate.runtimes
                consume_claims = True
            self._authority = envelope
            names = set(self._slots) | set(configs)
            for name in sorted(names):
                slot = self._slots.setdefault(name, _RuntimeSlot())
                config = configs.get(name)
                replacement = candidate_runtimes.get(name)
                if config is None:
                    slot.config = None
                    if slot.active is not None:
                        was_deferred = slot.deferred_failure is slot.active
                        old = await self._retire_active_locked(name, slot)
                        if old is not None:
                            (close_now if was_deferred else retire).append((name, old))
                        elif slot.deferred_failure is not None:
                            deferred.append(name)
                    if slot.active is None and slot.draining is None:
                        del self._slots[name]
                    continue

                replacement_matches = bool(
                    replacement is not None
                    and replacement.config.storage_dict() == config.storage_dict()
                )
                if replacement_matches:
                    if slot.active is not None:
                        was_deferred = slot.deferred_failure is slot.active
                        old = await self._retire_active_locked(name, slot)
                        if old is not None:
                            (close_now if was_deferred else retire).append((name, old))
                        elif slot.active is not None:
                            # Protected by the candidate lease in valid transitions.
                            replacement = None
                    slot.config = config
                if replacement_matches and replacement is not None:
                    server_catalog = persisted.get(name)
                    activation_failure: RuntimeFailure | None = None
                    if server_catalog is None:
                        activation_failure = RuntimeFailure(
                            code="tool_mcp_unavailable",
                            message=f"MCP server '{name}' has no saved catalog",
                            permanent=True,
                        )
                    else:
                        try:
                            replacement.bind_authority(
                                server_catalog,
                                config_revision=envelope.config_revision,
                                catalog_digest=envelope.mcp_catalog.digest,
                            )
                        except Exception:
                            activation_failure = RuntimeFailure(
                                code="tool_mcp_unavailable",
                                message=f"MCP server '{name}' activation failed",
                                permanent=False,
                            )
                    if activation_failure is not None:
                        await replacement.admission.retire()
                        replacement.update_authority(
                            config_revision=envelope.config_revision,
                            catalog_digest=envelope.mcp_catalog.digest,
                        )
                        replacement.mark_unavailable(activation_failure)
                    slot.active = replacement
                    self._candidate_runtimes.pop(replacement, None)
                    self._candidate_open_tasks.pop(replacement, None)
                    slot.placeholder_state = replacement.state
                    slot.placeholder_error = replacement.last_error
                    slot.restart_attempt = 0
                    if activation_failure is not None:
                        slot.deferred_failure = replacement
                        deferred.append(name)
                    elif replacement.state is RuntimeState.READY:
                        monitor.append((name, replacement))
                    continue

                old_config = slot.config
                active = slot.active
                previous_server = previous_persisted.get(name)
                current_server = persisted.get(name)
                config_unchanged = bool(
                    old_config is not None and old_config.storage_dict() == config.storage_dict()
                )
                same_config = bool(
                    config_unchanged
                    and active is not None
                    and active.config.storage_dict() == config.storage_dict()
                )
                same_catalog = bool(
                    previous_server is not None
                    and current_server is not None
                    and canonical_json_bytes(previous_server)
                    == canonical_json_bytes(current_server)
                )
                slot.config = config
                if (
                    active is None
                    and config_unchanged
                    and same_catalog
                    and slot.placeholder_error is not None
                    and slot.placeholder_error.permanent
                ):
                    continue
                if active is not None and same_config and same_catalog:
                    active.update_authority(
                        config_revision=envelope.config_revision,
                        catalog_digest=envelope.mcp_catalog.digest,
                    )
                    slot.placeholder_state = active.state
                    slot.placeholder_error = active.last_error
                    if slot.deferred_failure is active:
                        deferred.append(name)
                    continue
                if active is not None:
                    was_deferred = slot.deferred_failure is active
                    old = await self._retire_active_locked(name, slot)
                    if old is not None:
                        (close_now if was_deferred else retire).append((name, old))
                    elif slot.deferred_failure is not None:
                        deferred.append(name)
                if slot.active is None:
                    slot.placeholder_state = RuntimeState.STARTING
                    slot.placeholder_error = None
                    slot.restart_attempt = 0
                    if slot.draining is None:
                        recover.append(name)
            if candidate is not None and consume_claims:
                self._transition_names.difference_update(candidate.claimed_names)

        for name, runtime in retire:
            self._spawn_transient(self._drain_retired(name, runtime))
        for name, runtime in close_now:
            self._spawn_transient(self._close_draining(name, runtime))
        for name, runtime in monitor:
            self._start_monitor(name, runtime)
        for name in deferred:
            await self._continue_deferred_failure(name)
        for name in recover:
            self._schedule_recovery(name)

    async def start(self, envelope: ServerMcpEnvelope) -> None:
        """Install durable authority and start every endpoint asynchronously."""

        async with self._lock:
            if self._authority is not None:
                raise RuntimeError("Server MCP supervisor has already started")
            if self._closed:
                raise RuntimeError("Server MCP supervisor is shutting down")
            self._authority = envelope
            names = []
            for config in envelope.mcp_servers:
                slot = self._slots.setdefault(config.name, _RuntimeSlot())
                slot.config = config
                slot.placeholder_state = RuntimeState.STARTING
                names.append(config.name)
        for name in names:
            self._schedule_recovery(name)

    def _schedule_recovery(self, name: str) -> None:
        if self._closed:
            return
        existing = self._recovery_tasks.get(name)
        if existing is not None and not existing.done():
            self._recovery_requested.add(name)
            return
        self._recovery_requested.discard(name)
        task = self._spawn_lifecycle(self._recover(name))
        self._recovery_tasks[name] = task

        def remove(completed: asyncio.Task[None]) -> None:
            if self._recovery_tasks.get(name) is completed:
                del self._recovery_tasks[name]
                if name in self._recovery_requested:
                    self._recovery_requested.discard(name)
                    self._schedule_recovery(name)

        task.add_done_callback(remove)

    async def _recover(self, name: str) -> None:
        apply_initial_backoff = True
        while not self._closed:
            initial_delay: float | None = None
            async with self._lock:
                slot = self._slots.get(name)
                authority = self._authority
                if (
                    slot is None
                    or slot.config is None
                    or authority is None
                    or slot.draining is not None
                    or slot.active is not None
                    or name in self._transition_names
                ):
                    return
                if (
                    apply_initial_backoff
                    and slot.placeholder_state is RuntimeState.BACKOFF
                    and slot.restart_attempt > 0
                ):
                    initial_delay = retry_backoff_seconds(
                        slot.restart_attempt - 1,
                        jitter=float(self._jitter()),
                    )
                else:
                    config = slot.config
                    runtime = self._new_runtime(config)
                    runtime.update_authority(
                        config_revision=authority.config_revision,
                        catalog_digest=authority.mcp_catalog.digest,
                    )
                    slot.active = runtime
                    slot.placeholder_state = RuntimeState.STARTING
            if initial_delay is not None:
                apply_initial_backoff = False
                await self._clock.sleep_until(self._clock.now() + initial_delay)
                continue
            try:
                async with self._validation_admission:
                    await runtime.open()
                async with self._lock:
                    current = self._slots.get(name)
                    current_authority = self._authority
                    if (
                        current is not slot
                        or current.active is not runtime
                        or current.config != config
                        or current_authority is None
                    ):
                        return
                    persisted = next(
                        (
                            server
                            for server in current_authority.mcp_catalog.servers
                            if server.name == name
                        ),
                        None,
                    )
                    if persisted is None:
                        raise RuntimeOpenError(
                            RuntimeFailure(
                                code="tool_mcp_unavailable",
                                message=f"MCP server '{name}' has no saved catalog",
                                permanent=True,
                            )
                        )
                    try:
                        runtime.bind_authority(
                            persisted,
                            config_revision=current_authority.config_revision,
                            catalog_digest=current_authority.mcp_catalog.digest,
                        )
                    except Exception as exc:
                        raise RuntimeOpenError(
                            RuntimeFailure(
                                code="tool_mcp_unavailable",
                                message=f"MCP server '{name}' activation failed",
                                permanent=False,
                            )
                        ) from exc
                    current.placeholder_state = runtime.state
                    current.placeholder_error = runtime.last_error
                    current.restart_attempt = 0
                if runtime.state is RuntimeState.READY:
                    self._start_monitor(name, runtime)
                return
            except asyncio.CancelledError:
                await runtime.close()
                raise
            except (RuntimeOpenError, StopIteration) as exc:
                failure = (
                    exc.failure
                    if isinstance(exc, RuntimeOpenError)
                    else RuntimeFailure(
                        code="tool_mcp_unavailable",
                        message=f"MCP server '{name}' has no saved catalog",
                        permanent=True,
                    )
                )
                if (
                    not runtime.cleanup_complete
                    and runtime.state is not RuntimeState.CLEANUP_BLOCKED
                ):
                    await runtime.close()
                cleanup: tuple[str, RuntimeGeneration] | None = None
                delay: float | None = None
                async with self._lock:
                    current = self._slots.get(name)
                    if current is not slot or current.active is not runtime:
                        return
                    current.placeholder_error = failure
                    current.placeholder_state = RuntimeState.UNAVAILABLE
                    if runtime.cleanup_complete:
                        current.active = None
                    elif current.draining is None:
                        runtime.mark_draining()
                        current.active = None
                        current.draining = runtime
                        current.draining_origin = "persisted"
                        cleanup = (name, runtime)
                    else:
                        runtime.mark_unavailable(failure)
                        current.deferred_failure = runtime
                    if not failure.permanent and runtime.cleanup_complete:
                        delay = retry_backoff_seconds(
                            current.restart_attempt,
                            jitter=float(self._jitter()),
                        )
                        current.restart_attempt += 1
                        current.placeholder_state = RuntimeState.BACKOFF
                if cleanup is not None:
                    self._spawn_transient(self._close_draining(*cleanup))
                    return
                if failure.permanent or delay is None:
                    return
                apply_initial_backoff = False
                await self._clock.sleep_until(self._clock.now() + delay)

    async def _retire_active_locked(
        self,
        name: str,
        slot: _RuntimeSlot,
        *,
        failure: RuntimeFailure | None = None,
    ) -> RuntimeGeneration | None:
        runtime = slot.active
        if runtime is None:
            return None
        await runtime.admission.retire()
        self._cancel_monitor(runtime)
        if runtime.cleanup_complete:
            slot.active = None
            if slot.deferred_failure is runtime:
                slot.deferred_failure = None
            return None
        if slot.draining is None:
            runtime.mark_draining()
            slot.active = None
            slot.draining = runtime
            slot.draining_origin = "persisted"
            if slot.deferred_failure is runtime:
                slot.deferred_failure = None
            return runtime
        effective = (
            failure
            or runtime.last_error
            or RuntimeFailure(
                code="tool_mcp_unavailable",
                message=f"MCP server '{name}' runtime became unavailable",
                permanent=False,
            )
        )
        runtime.mark_unavailable(effective)
        slot.placeholder_state = RuntimeState.UNAVAILABLE
        slot.placeholder_error = effective
        slot.deferred_failure = runtime
        return None

    async def _continue_deferred_failure(self, name: str) -> None:
        runtime: RuntimeGeneration | None = None
        async with self._lock:
            slot = self._slots.get(name)
            if (
                slot is None
                or slot.draining is not None
                or name in self._transition_names
                or slot.deferred_failure is None
            ):
                return
            failed = slot.deferred_failure
            if slot.active is not failed:
                slot.deferred_failure = None
                return
            failed.mark_draining()
            slot.active = None
            slot.draining = failed
            slot.draining_origin = "persisted"
            slot.deferred_failure = None
            runtime = failed
        assert runtime is not None
        self._spawn_transient(self._close_draining(name, runtime))

    async def _ensure_recovery(self, name: str) -> None:
        async with self._lock:
            slot = self._slots.get(name)
            recover = bool(
                not self._closed
                and slot is not None
                and slot.config is not None
                and slot.active is None
                and slot.draining is None
                and name not in self._transition_names
                and not (slot.placeholder_error is not None and slot.placeholder_error.permanent)
            )
        if recover:
            self._schedule_recovery(name)

    async def dispatch_server_mcp(
        self,
        *,
        route: FrozenServerMcpEntryRoute,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        on_issued: Callable[[], None] | None = None,
        issue_guard: Callable[[], bool] | None = None,
    ) -> ToolResult:
        async with self._lock:
            authority = self._authority
            slot = self._slots.get(route.server)
            runtime = slot.active if slot is not None else None
            if not self._route_is_current(authority, runtime, route, name):
                return _error(
                    ErrorCode.TOOL_MCP_UNAVAILABLE,
                    "Server MCP capability changed before it could be called",
                )
            assert runtime is not None

            def start_invocation(_lease: object) -> object:
                if issue_guard is not None and not issue_guard():
                    raise ServerMcpUnavailableError
                current = self._slots.get(route.server)
                if not self._route_is_current(
                    self._authority,
                    current.active if current is not None else None,
                    route,
                    name,
                ):
                    raise ServerMcpUnavailableError
                task = asyncio.create_task(
                    runtime.invoke(route.entry_id, args),
                    name=f"server-mcp-call-{route.server}",
                )
                runtime.track_invocation(task)
                try:
                    if on_issued is not None:
                        on_issued()
                except BaseException:
                    task.cancel()
                    raise
                return task

            try:
                ticket = await runtime.admission.submit(user_id, start_invocation)
            except ServerMcpBusyError:
                return _error(ErrorCode.TOOL_MCP_BUSY, "Server MCP runtime is busy")
            except ServerMcpUnavailableError:
                return _error(
                    ErrorCode.TOOL_MCP_UNAVAILABLE,
                    "Server MCP runtime is unavailable",
                )

        try:
            ticket_waiter = asyncio.create_task(
                ticket.wait(), name=f"server-mcp-admission-{route.server}"
            )
            issued = await asyncio.shield(ticket_waiter)
        except asyncio.CancelledError:
            async def resolve_cancelled_admission() -> None:
                removed = await ticket.cancel()
                if removed:
                    ticket_waiter.cancel()
                    await asyncio.gather(ticket_waiter, return_exceptions=True)
                    return
                try:
                    cancelled_issued = await asyncio.shield(ticket_waiter)
                except BaseException:
                    return
                cancelled_invocation = cast(
                    asyncio.Task[ToolResult], cancelled_issued.invocation
                )
                if runtime.is_remote:
                    await self._handoff_remote_invocation(
                        route.server,
                        runtime,
                        cancelled_invocation,
                        cancelled_issued.lease,
                    )
                else:
                    await self._retire_failed_active(
                        route.server,
                        runtime,
                        lease=cancelled_issued.lease,
                    )

            resolution = asyncio.create_task(
                resolve_cancelled_admission(),
                name=f"server-mcp-cancel-resolution-{route.server}",
            )
            await await_future_cancellation_safe(resolution)
            raise
        except ServerMcpBusyError:
            return _error(ErrorCode.TOOL_MCP_BUSY, "Server MCP runtime is busy")
        except ServerMcpUnavailableError:
            return _error(
                ErrorCode.TOOL_MCP_UNAVAILABLE,
                "Server MCP runtime is unavailable",
            )

        invocation = cast(asyncio.Task[ToolResult], issued.invocation)
        deadline = asyncio.create_task(
            self._clock.sleep_until(issued.public_deadline),
            name=f"server-mcp-public-deadline-{route.server}",
        )
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
                    await self._retire_failed_active(
                        route.server,
                        runtime,
                        failure=RuntimeFailure(
                            code="mcp_message_too_large",
                            message=(
                                f"MCP server '{route.server}' exceeded the inbound message limit"
                            ),
                            permanent=True,
                        ),
                        lease=issued.lease,
                    )
                    return _error(
                        ErrorCode.TOOL_MCP_MESSAGE_TOO_LARGE,
                        "The MCP response exceeded the raw message limit",
                    )
                except RuntimeTransportError as exc:
                    await self._retire_failed_active(
                        route.server,
                        runtime,
                        failure=exc.failure,
                        lease=issued.lease,
                    )
                    return self._outcome_unknown()
                except asyncio.CancelledError:
                    await self._retain_or_release_lease(runtime, issued.lease)
                    return self._outcome_unknown()
                await issued.lease.aclose()
                return result

            if runtime.is_remote:
                await self._handoff_remote_invocation(
                    route.server,
                    runtime,
                    invocation,
                    issued.lease,
                )
            else:
                await self._retire_failed_active(
                    route.server,
                    runtime,
                    lease=issued.lease,
                )
            return self._outcome_unknown()
        except asyncio.CancelledError:
            deadline.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await deadline
            if runtime.is_remote:
                handoff = self._spawn_transient(
                    self._handoff_remote_invocation(
                        route.server,
                        runtime,
                        invocation,
                        issued.lease,
                    )
                )
                await await_future_cancellation_safe(handoff)
            else:
                retirement = asyncio.create_task(
                    self._retire_failed_active(
                        route.server,
                        runtime,
                        lease=issued.lease,
                    )
                )
                await await_future_cancellation_safe(retirement)
            raise

    async def _handoff_remote_invocation(
        self,
        name: str,
        runtime: RuntimeGeneration,
        invocation: asyncio.Task[ToolResult],
        lease: AdmissionLease,
    ) -> None:
        await lease.mark_draining()
        self._spawn_transient(self._drain_late_result(name, runtime, invocation, lease))

    @staticmethod
    def _route_is_current(
        authority: ServerMcpEnvelope | None,
        runtime: RuntimeGeneration | None,
        route: FrozenServerMcpEntryRoute,
        name: str,
    ) -> bool:
        if (
            authority is None
            or runtime is None
            or runtime.state is not RuntimeState.READY
            or runtime.generation is None
            or route.runtime_generation != runtime.generation
            or route.config_revision != authority.config_revision
            or route.catalog_digest != authority.mcp_catalog.digest
            or route.final_name != name
            or runtime.config_revision != authority.config_revision
            or runtime.catalog_digest != authority.mcp_catalog.digest
        ):
            return False
        bound = runtime.routes.get(route.entry_id)
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
        return _error(
            ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
            "MCP call may have executed, but its outcome is unknown; do not replay it blindly",
        )

    async def _drain_late_result(
        self,
        name: str,
        runtime: RuntimeGeneration,
        invocation: asyncio.Task[ToolResult],
        lease: AdmissionLease,
    ) -> None:
        deadline = asyncio.create_task(
            self._clock.sleep_until(self._clock.now() + REMOTE_RESULT_DRAIN_SECONDS),
            name=f"server-mcp-result-drain-{name}",
        )
        try:
            done, _ = await asyncio.wait(
                {invocation, deadline}, return_when=asyncio.FIRST_COMPLETED
            )
            if invocation in done:
                deadline.cancel()
                try:
                    await invocation
                except RuntimeMessageTooLargeError:
                    await self._retire_failed_active(
                        name,
                        runtime,
                        failure=RuntimeFailure(
                            code="mcp_message_too_large",
                            message=f"MCP server '{name}' exceeded the inbound message limit",
                            permanent=True,
                        ),
                        lease=lease,
                    )
                except RuntimeTransportError as exc:
                    await self._retire_failed_active(
                        name,
                        runtime,
                        failure=exc.failure,
                        lease=lease,
                    )
                except BaseException:
                    pass
                return
            await self._retire_failed_active(name, runtime, lease=lease)
        finally:
            if not deadline.done():
                deadline.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await deadline
            await self._retain_or_release_lease(runtime, lease)

    async def _retire_failed_active(
        self,
        name: str,
        runtime: RuntimeGeneration,
        *,
        failure: RuntimeFailure | None = None,
        lease: AdmissionLease | None = None,
    ) -> None:
        effective = failure or RuntimeFailure(
            code="tool_mcp_unavailable",
            message=f"MCP server '{name}' runtime became unavailable",
            permanent=False,
        )
        registered = False
        release_lease = False
        async with self._lock:
            slot = self._slots.get(name)
            if lease is not None:
                if runtime.cleanup_complete:
                    release_lease = True
                else:
                    self._retained_leases.setdefault(runtime, set()).add(lease)
            if slot is None:
                release_lease = True
            elif slot.active is runtime:
                if runtime.cleanup_complete:
                    slot.active = None
                    if slot.deferred_failure is runtime:
                        slot.deferred_failure = None
                    release_lease = True
                else:
                    await runtime.admission.retire()
                    self._cancel_monitor(runtime)
                    runtime.mark_unavailable(effective)
                    slot.placeholder_state = RuntimeState.UNAVAILABLE
                    slot.placeholder_error = effective
                    if name in self._transition_names or slot.draining is not None:
                        slot.deferred_failure = runtime
                    else:
                        runtime.mark_draining()
                        slot.active = None
                        slot.draining = runtime
                        slot.draining_origin = "persisted"
                        slot.deferred_failure = None
                        registered = True
            elif slot.draining is runtime:
                registered = True
            else:
                release_lease = True
        if release_lease and lease is not None:
            async with self._lock:
                retained = self._retained_leases.get(runtime)
                if retained is not None:
                    retained.discard(lease)
                    if not retained:
                        del self._retained_leases[runtime]
            await lease.aclose()
        if registered:
            await self._close_draining(name, runtime)

    async def _retain_or_release_lease(
        self,
        runtime: RuntimeGeneration,
        lease: AdmissionLease,
    ) -> None:
        release = False
        async with self._lock:
            retained = self._retained_leases.get(runtime)
            if retained is not None and lease in retained:
                return
            if runtime.cleanup_complete or runtime.state is RuntimeState.READY:
                release = True
            else:
                self._retained_leases.setdefault(runtime, set()).add(lease)
        if release:
            await lease.aclose()

    async def _monitor(self, name: str, runtime: RuntimeGeneration) -> None:
        while not self._closed:
            event = await runtime.next_event()
            if self._closed:
                return
            if event == "transport_failed":
                await self._retire_failed_active(
                    name,
                    runtime,
                    failure=runtime.transport_failure(),
                )
                return
            async with self._lock:
                slot = self._slots.get(name)
                authority = self._authority
                if slot is None or slot.active is not runtime or authority is None:
                    return
                persisted = next(
                    (server for server in authority.mcp_catalog.servers if server.name == name),
                    None,
                )
                if persisted is None:
                    return
                revision = authority.config_revision
                digest = authority.mcp_catalog.digest
            try:
                unchanged = await runtime.refresh_authority(
                    persisted,
                    config_revision=revision,
                    catalog_digest=digest,
                )
            except RuntimeOpenError as exc:
                await self._retire_failed_active(
                    name,
                    runtime,
                    failure=exc.failure,
                )
                return
            if not unchanged:
                return
            async with self._lock:
                current = self._slots.get(name)
                current_authority = self._authority
                if (
                    current is None
                    or current.active is not runtime
                    or current_authority is None
                ):
                    return
                runtime.update_authority(
                    config_revision=current_authority.config_revision,
                    catalog_digest=current_authority.mcp_catalog.digest,
                )
                current.placeholder_state = runtime.state
                current.placeholder_error = runtime.last_error

    async def _drain_retired(self, name: str, runtime: RuntimeGeneration) -> None:
        tasks = runtime.invocation_tasks
        if tasks:
            completion = asyncio.gather(*tasks, return_exceptions=True)
            deadline = asyncio.create_task(
                self._clock.sleep_until(self._clock.now() + GENERATION_DRAIN_SECONDS)
            )
            waiters: set[asyncio.Future[Any]] = {completion, deadline}
            done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            if completion in done:
                deadline.cancel()
            else:
                completion.cancel()
            with contextlib.suppress(BaseException):
                await deadline
            with contextlib.suppress(BaseException):
                await completion
        await self._close_draining(name, runtime)

    async def _close_draining(self, name: str, runtime: RuntimeGeneration) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await runtime.close()
        if runtime.state is RuntimeState.CLEANUP_BLOCKED and not self._closed:
            if runtime not in self._cleanup_retry_runtimes:
                self._cleanup_retry_runtimes.add(runtime)
                self._spawn_lifecycle(self._retry_cleanup(name, runtime))
            return
        await self._release_draining(name, runtime)

    async def _retry_cleanup(self, name: str, runtime: RuntimeGeneration) -> None:
        try:
            attempt = 0
            while not self._closed and runtime.state is RuntimeState.CLEANUP_BLOCKED:
                delay = retry_backoff_seconds(attempt, jitter=float(self._jitter()))
                attempt += 1
                await self._clock.sleep_until(self._clock.now() + delay)
                if self._closed:
                    return
                await runtime.retry_cleanup()
            if runtime.cleanup_complete:
                await self._release_draining(name, runtime)
        finally:
            self._cleanup_retry_runtimes.discard(runtime)

    async def _release_draining(self, name: str, runtime: RuntimeGeneration) -> None:
        leases: tuple[AdmissionLease, ...] = ()
        next_draining: RuntimeGeneration | None = None
        recover = False
        async with self._lock:
            slot = self._slots.get(name)
            if runtime.cleanup_complete:
                leases = tuple(self._retained_leases.pop(runtime, ()))
            if slot is None or slot.draining is not runtime or not runtime.cleanup_complete:
                pass
            else:
                slot.draining = None
                deferred = slot.deferred_failure
                if (
                    deferred is not None
                    and slot.active is deferred
                    and name not in self._transition_names
                ):
                    deferred.mark_draining()
                    slot.active = None
                    slot.draining = deferred
                    slot.draining_origin = "persisted"
                    slot.deferred_failure = None
                    next_draining = deferred
                elif slot.config is None and slot.active is None:
                    del self._slots[name]
                elif (
                    slot.config is not None
                    and slot.active is None
                    and name not in self._transition_names
                    and not (
                        slot.placeholder_error is not None and slot.placeholder_error.permanent
                    )
                ):
                    if slot.placeholder_error is not None:
                        slot.placeholder_state = RuntimeState.BACKOFF
                        slot.restart_attempt += 1
                    else:
                        slot.placeholder_state = RuntimeState.STARTING
                    recover = True
        if leases:
            await asyncio.gather(*(lease.aclose() for lease in leases))
        if next_draining is not None:
            self._spawn_transient(self._close_draining(name, next_draining))
        elif recover:
            self._schedule_recovery(name)

    def ready_generations(self, envelope: ServerMcpEnvelope) -> Mapping[str, UUID | None]:
        exact = self._authority_matches(envelope)
        result: dict[str, UUID | None] = {}
        for config in envelope.mcp_servers:
            slot = self._slots.get(config.name)
            runtime = slot.active if slot is not None else None
            result[config.name] = (
                runtime.generation
                if exact
                and runtime is not None
                and runtime.state is RuntimeState.READY
                and runtime.config_revision == envelope.config_revision
                and runtime.catalog_digest == envelope.mcp_catalog.digest
                else None
            )
        return MappingProxyType(result)

    def runtime_generations(self) -> Mapping[str, UUID | None]:
        authority = self._authority
        if authority is None:
            return MappingProxyType({})
        return self.ready_generations(authority)

    def _immutable_runtime_snapshot(
        self, envelope: ServerMcpEnvelope
    ) -> Mapping[str, RuntimeSlotSnapshot]:
        exact = self._authority_matches(envelope)
        configs = {config.name: config for config in envelope.mcp_servers}
        result: dict[str, RuntimeSlotSnapshot] = {}
        for name in sorted(set(configs) | set(self._slots)):
            configured = name in configs
            slot = self._slots.get(name)
            draining = (
                slot.draining.snapshot(origin=slot.draining_origin)
                if slot is not None and slot.draining is not None
                else None
            )
            active: RuntimeStatusSnapshot | None = None
            if configured and exact and slot is not None and slot.active is not None:
                active = slot.active.snapshot(origin="persisted")
            elif configured:
                config = configs[name]
                state: RuntimePublicState = (
                    cast(RuntimePublicState, slot.placeholder_state.value)
                    if exact and slot is not None
                    else "starting"
                )
                active = RuntimeStatusSnapshot(
                    state=state,
                    origin="persisted",
                    config_revision=envelope.config_revision,
                    catalog_digest=envelope.mcp_catalog.digest,
                    runtime_generation=None,
                    max_concurrent_calls=config.max_concurrent_calls,
                    active_calls=0,
                    waiting_calls=0,
                    draining_calls=0,
                    restart_attempt=(slot.restart_attempt if slot is not None else 0),
                    last_error=(slot.placeholder_error if exact and slot is not None else None),
                )
            if configured or draining is not None:
                result[name] = RuntimeSlotSnapshot(
                    configured=configured,
                    active=active,
                    draining=draining,
                )
        return MappingProxyType(result)

    def runtime_snapshot(self, envelope: ServerMcpEnvelope) -> dict[str, ServerMcpRuntimeSlot]:
        """Return one non-blocking strict DTO copy for the Admin response."""

        def status(value: RuntimeStatusSnapshot | None) -> ServerMcpRuntimeStatus | None:
            if value is None:
                return None
            last_error = (
                ServerMcpRuntimeError(
                    code=value.last_error.code,
                    message=value.last_error.message,
                )
                if value.last_error is not None
                else None
            )
            return ServerMcpRuntimeStatus(
                state=value.state,
                origin=value.origin,
                config_revision=value.config_revision,
                catalog_digest=value.catalog_digest,
                runtime_generation=value.runtime_generation,
                max_concurrent_calls=value.max_concurrent_calls,
                active_calls=value.active_calls,
                waiting_calls=value.waiting_calls,
                draining_calls=value.draining_calls,
                restart_attempt=value.restart_attempt,
                last_error=last_error,
            )

        return {
            name: ServerMcpRuntimeSlot(
                configured=slot.configured,
                active=status(slot.active),
                draining=status(slot.draining),
            )
            for name, slot in self._immutable_runtime_snapshot(envelope).items()
        }

    def refresh_names(self, envelope: ServerMcpEnvelope) -> tuple[str, ...]:
        """Select exact same-config runtimes that an Admin no-op may recover."""

        if not self._authority_matches(envelope):
            return tuple(config.name for config in envelope.mcp_servers)
        names: list[str] = []
        for config in envelope.mcp_servers:
            slot = self._slots.get(config.name)
            runtime = slot.active if slot is not None else None
            if (
                slot is None
                or slot.config is None
                or slot.config.storage_dict() != config.storage_dict()
                or runtime is None
                or runtime.config_revision != envelope.config_revision
                or runtime.catalog_digest != envelope.mcp_catalog.digest
                or runtime.state
                in {RuntimeState.DRIFTED, RuntimeState.UNAVAILABLE, RuntimeState.BACKOFF}
            ):
                names.append(config.name)
        return tuple(names)

    def snapshot(self) -> Mapping[str, RuntimeSlotSnapshot]:
        authority = self._authority
        if authority is None:
            return MappingProxyType({})
        return self._immutable_runtime_snapshot(authority)

    def _authority_matches(self, envelope: ServerMcpEnvelope) -> bool:
        authority = self._authority
        return bool(
            authority is not None
            and authority.config_revision == envelope.config_revision
            and authority.mcp_catalog.digest == envelope.mcp_catalog.digest
        )

    def coordinator_snapshot(self) -> CoordinatorSnapshot:
        return self._coordinator.snapshot()

    def _spawn_transient(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self._transient_tasks.add(task)
        task.add_done_callback(self._transient_tasks.discard)
        return task

    def _spawn_lifecycle(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self._lifecycle_tasks.add(task)
        task.add_done_callback(self._lifecycle_tasks.discard)
        return task

    def _start_monitor(self, name: str, runtime: RuntimeGeneration) -> None:
        self._cancel_monitor(runtime)
        task = self._spawn_lifecycle(self._monitor(name, runtime))
        self._monitor_tasks[runtime] = task

        def remove(completed: asyncio.Task[None]) -> None:
            if self._monitor_tasks.get(runtime) is completed:
                del self._monitor_tasks[runtime]

        task.add_done_callback(remove)

    def _cancel_monitor(self, runtime: RuntimeGeneration) -> None:
        task = self._monitor_tasks.pop(runtime, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def wait_background(self) -> None:
        while self._transient_tasks:
            await asyncio.gather(*tuple(self._transient_tasks), return_exceptions=True)

    async def shutdown(self) -> None:
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(
                self._shutdown_impl(), name="server-mcp-supervisor-shutdown"
            )
        await await_future_cancellation_safe(self._shutdown_task)

    async def begin_shutdown(self) -> None:
        """Reject mutations/new admission without waiting for transport cleanup."""

        if self._begin_shutdown_task is None:
            self._begin_shutdown_task = asyncio.create_task(
                self._begin_shutdown_impl(), name="server-mcp-begin-shutdown"
            )
        await await_future_cancellation_safe(self._begin_shutdown_task)

    async def _begin_shutdown_impl(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            runtimes = {
                runtime
                for slot in self._slots.values()
                for runtime in (slot.active, slot.draining)
                if runtime is not None
            } | set(self._candidate_runtimes)
            for runtime in runtimes:
                await runtime.admission.retire()

    async def _shutdown_impl(self) -> None:
        await self.begin_shutdown()
        async with self._lock:
            if self._shutdown_complete:
                return
            runtimes = {
                runtime
                for slot in self._slots.values()
                for runtime in (slot.active, slot.draining)
                if runtime is not None
            } | set(self._candidate_runtimes)
            candidate_open_tasks = tuple(self._candidate_open_tasks.values())
            for runtime in runtimes:
                await runtime.admission.retire()
        for candidate_open_task in candidate_open_tasks:
            candidate_open_task.cancel()
        if candidate_open_tasks:
            await asyncio.gather(*candidate_open_tasks, return_exceptions=True)
        lifecycle = tuple(self._lifecycle_tasks)
        for lifecycle_task in lifecycle:
            lifecycle_task.cancel()
        if lifecycle:
            await asyncio.gather(*lifecycle, return_exceptions=True)

        async def close_runtime(runtime: RuntimeGeneration) -> None:
            if runtime.state is RuntimeState.CLEANUP_BLOCKED:
                await runtime.retry_cleanup()
            else:
                await runtime.close()
            if runtime.state is RuntimeState.CLEANUP_BLOCKED:
                await runtime.cancel_pending_cleanup()

        close_tasks = [asyncio.create_task(close_runtime(runtime)) for runtime in runtimes]
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        await self.wait_background()
        async with self._lock:
            retained_leases = tuple(
                lease for leases in self._retained_leases.values() for lease in leases
            )
            self._retained_leases.clear()
        if retained_leases:
            await asyncio.gather(*(lease.aclose() for lease in retained_leases))
        await self._coordinator.close()
        async with self._lock:
            self._slots.clear()
            self._transition_names.clear()
            self._candidate_runtimes.clear()
            self._candidate_open_tasks.clear()
            self._recovery_requested.clear()
            self._shutdown_complete = True


__all__ = [
    "CANDIDATE_TIMEOUT_SECONDS",
    "GENERATION_DRAIN_SECONDS",
    "PROCESS_RESERVED_NAME_MAX",
    "REMOTE_RESULT_DRAIN_SECONDS",
    "VALIDATION_PARALLELISM",
    "RuntimeSlotSnapshot",
    "ServerMcpSupervisor",
    "ValidatedServerMcpCandidate",
    "retry_backoff_seconds",
]
