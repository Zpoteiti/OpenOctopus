"""Process-lifetime MCP supervision and Protocol v3 registration state."""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from openoctopus_client.mcp.catalog import canonical_json_bytes
from openoctopus_client.mcp.models import (
    McpServerConfig,
    PersistedMcpCatalog,
    PersistedMcpServerCatalog,
    RemoteMcpServerConfigBase,
    StdioMcpServerConfig,
)
from openoctopus_client.mcp.runtime import (
    CandidateValidation,
    McpRuntimeError,
    McpRuntimeState,
    McpServerRuntime,
    validate_candidate,
)
from openoctopus_client.protocol import (
    AcceptedMcpRegistration,
    ConfigValidate,
    ConfigValidateResult,
    DeviceConfig,
    DriftedMcpRuntimeSnapshot,
    McpRuntimeSnapshot,
    ProtocolError,
    ReadyMcpRuntimeSnapshot,
    RegisterMcp,
    RegisterMcpAck,
    RuntimeMcpSourceCatalog,
    ToolCall,
    UnavailableMcpRuntimeSnapshot,
    new_uuid7,
)
from openoctopus_client.protocol import (
    McpValidationFailure as ProtocolValidationFailure,
)
from openoctopus_client.tools import ToolOutput
from openoctopus_client.tools.common import fail

_VALIDATION_TOMBSTONE_MAX = 64
_RECENT_VALIDATION_MAX = 64
_CANDIDATE_LEASE_SECONDS = 60.0
_DRAIN_SECONDS = 60.0
_LIST_CHANGED_DEBOUNCE_SECONDS = 0.2
_STARTUP_PARALLELISM = 4


type _CandidateValidator = Callable[..., Awaitable[CandidateValidation]]
type _SinkKey = tuple[str, str, str] | tuple[
    str,
    str,
    str,
    tuple[str, ...],
    str | None,
]


@dataclass(slots=True)
class _RuntimeSlot:
    config: McpServerConfig
    runtime: McpServerRuntime
    persisted: PersistedMcpServerCatalog
    calls: int = 0
    drained: asyncio.Event = field(default_factory=asyncio.Event)
    watcher: asyncio.Task[None] | None = None
    retry: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        self.drained.set()


@dataclass(slots=True)
class _CandidateLease:
    frame: ConfigValidate
    validation: CandidateValidation
    expiry: asyncio.Task[None]
    connection_epoch: int


@dataclass(frozen=True, slots=True)
class _RegistrationRequest:
    frame: RegisterMcp
    desired: bytes


@dataclass(slots=True)
class McpInvocationLease:
    """Keep one issued MCP call bound to its accepted runtime generation."""

    _supervisor: McpSupervisor
    _call: ToolCall
    _slot: _RuntimeSlot | None = None
    _entry_id: UUID | None = None
    _failure: ToolOutput | None = None
    _claimed: bool = False
    _released: bool = False

    async def invoke(self) -> ToolOutput:
        if self._claimed or self._released:
            raise RuntimeError("MCP invocation lease was already consumed")
        self._claimed = True
        try:
            if self._failure is not None:
                return self._failure
            assert self._slot is not None
            assert self._entry_id is not None
            return await self._supervisor._invoke_reserved(
                self._call,
                self._slot,
                self._entry_id,
            )
        finally:
            self.release()

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        if self._slot is not None:
            self._supervisor._release_invocation(self._slot)


def _config_projection(configs: Sequence[McpServerConfig]) -> bytes:
    return canonical_json_bytes([config.storage_dict() for config in configs])


def _device_config_projection(config: DeviceConfig) -> bytes:
    payload = config.model_dump(mode="json", exclude={"mcp_servers"})
    payload["mcp_servers"] = [server.storage_dict() for server in config.mcp_servers]
    return canonical_json_bytes(payload)


def _server_projection(server: PersistedMcpServerCatalog) -> bytes:
    return canonical_json_bytes(server)


def _has_secrets(configs: Sequence[McpServerConfig]) -> bool:
    return any(
        (
            isinstance(config, StdioMcpServerConfig)
            and any(secret.get_secret_value() for secret in config.env.values())
        )
        or (
            isinstance(config, RemoteMcpServerConfigBase)
            and any(secret.get_secret_value() for secret in config.headers.values())
        )
        for config in configs
    )


class McpSupervisor:
    """Own MCP sessions across ordinary OpenOctopus WebSocket reconnects."""

    def __init__(
        self,
        *,
        runtime_factory: Callable[[McpServerConfig], McpServerRuntime] = McpServerRuntime,
        candidate_validator: _CandidateValidator = validate_candidate,
        candidate_lease_seconds: float = _CANDIDATE_LEASE_SECONDS,
        drain_seconds: float = _DRAIN_SECONDS,
        list_changed_debounce_seconds: float = _LIST_CHANGED_DEBOUNCE_SECONDS,
        random_value: Callable[[], float] = random.random,
        secret_transport_safe: bool = True,
    ) -> None:
        if (
            candidate_lease_seconds <= 0
            or drain_seconds <= 0
            or list_changed_debounce_seconds < 0
        ):
            raise ValueError("MCP supervisor deadlines are invalid")
        self._runtime_factory = runtime_factory
        self._candidate_validator = candidate_validator
        self._candidate_lease_seconds = candidate_lease_seconds
        self._drain_seconds = drain_seconds
        self._list_changed_debounce_seconds = list_changed_debounce_seconds
        self._random_value = random_value
        self._secret_transport_safe = secret_transport_safe
        self._startup_semaphore = asyncio.Semaphore(_STARTUP_PARALLELISM)
        self._revision: int | None = None
        self._config: tuple[McpServerConfig, ...] = ()
        self._catalog: PersistedMcpCatalog | None = None
        self._slots: dict[str, _RuntimeSlot] = {}
        self._draining: dict[UUID, tuple[_RuntimeSlot, asyncio.Task[None]]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._validation_task: asyncio.Task[ConfigValidateResult | None] | None = None
        self._validation_id: UUID | None = None
        self._validation_epoch: int | None = None
        self._cancelled_validation_tasks: set[
            asyncio.Task[ConfigValidateResult | None]
        ] = set()
        self._candidates: dict[UUID, _CandidateLease] = {}
        self._tombstones: set[UUID] = set()
        self._tombstone_order: deque[UUID] = deque()
        self._recent_validations: set[UUID] = set()
        self._recent_validation_order: deque[UUID] = deque()
        self._connected = False
        self._connection_epoch = 0
        self._dirty = False
        self._changed = asyncio.Event()
        self._fatal_error: ProtocolError | None = None
        self._pending_registration: _RegistrationRequest | None = None
        self._accepted: dict[str, bytes] = {}
        self._refresh_dirty: set[str] = set()
        self._refresh_tasks: dict[str, asyncio.Task[None]] = {}
        self._cleanup_blocked: dict[UUID, McpServerRuntime] = {}
        self._closed = False

    @property
    def candidate_count(self) -> int:
        return len(self._candidates)

    @property
    def draining_count(self) -> int:
        return sum(not task.done() for _slot, task in self._draining.values())

    @property
    def has_pending_registration(self) -> bool:
        return self._pending_registration is not None

    @property
    def registration_changed(self) -> asyncio.Event:
        return self._changed

    def is_validation_tombstone(self, validation_id: UUID) -> bool:
        return validation_id in self._tombstones

    def _record_cleanup_state(self, runtime: McpServerRuntime) -> None:
        if runtime.state is McpRuntimeState.CLEANUP_BLOCKED:
            self._cleanup_blocked[runtime.generation] = runtime
        else:
            self._cleanup_blocked.pop(runtime.generation, None)

    def _retain_cleanup_blocked(
        self,
        runtimes: Sequence[McpServerRuntime],
    ) -> None:
        for runtime in runtimes:
            self._record_cleanup_state(runtime)

    @staticmethod
    def _sink_key(config: McpServerConfig) -> _SinkKey:
        if isinstance(config, StdioMcpServerConfig):
            return (
                config.name,
                config.transport,
                config.command,
                tuple(config.args),
                config.cwd,
            )
        return (config.name, config.transport, config.url)

    def _blocked_sink(
        self,
        config: McpServerConfig,
        *,
        exclude: McpServerRuntime | None = None,
    ) -> McpServerRuntime | None:
        sink_key = self._sink_key(config)
        return next(
            (
                runtime
                for runtime in self._cleanup_blocked.values()
                if runtime is not exclude
                and self._sink_key(runtime.config) == sink_key
                and runtime.state is McpRuntimeState.CLEANUP_BLOCKED
            ),
            None,
        )

    async def _close_runtime(self, runtime: McpServerRuntime) -> None:
        try:
            await runtime.close()
        finally:
            self._record_cleanup_state(runtime)

    async def _close_candidate(self, validation: CandidateValidation) -> None:
        try:
            await validation.close()
        finally:
            self._retain_cleanup_blocked(tuple(validation.runtimes.values()))

    def _publish_tombstone(self, validation_id: UUID, connection_epoch: int) -> None:
        if self._connected and connection_epoch == self._connection_epoch:
            self._add_tombstone(validation_id)

    def attach_connection(self) -> None:
        if self._closed:
            raise RuntimeError("MCP supervisor is closed")
        self._connection_epoch += 1
        self._connected = True
        self._fatal_error = None
        self._pending_registration = None
        self._accepted.clear()
        for slot in self._slots.values():
            if slot.runtime.state is McpRuntimeState.READY:
                slot.runtime.state = McpRuntimeState.AWAITING_ACK
        self._mark_dirty()

    def detach_connection(self) -> None:
        self._connected = False
        self._connection_epoch += 1
        self._pending_registration = None
        self._accepted.clear()
        self._changed.clear()
        validation_task = self._validation_task
        self._validation_task = None
        self._validation_id = None
        self._validation_epoch = None
        if validation_task is not None:
            self._cancelled_validation_tasks.add(validation_task)
            validation_task.cancel()
        leases = tuple(self._candidates.values())
        self._candidates.clear()
        for lease in leases:
            lease.expiry.cancel()
            self._track(asyncio.create_task(self._close_candidate(lease.validation)))
        self._tombstones.clear()
        self._tombstone_order.clear()
        self._recent_validations.clear()
        self._recent_validation_order.clear()
        self._fatal_error = None
        for slot in self._slots.values():
            if slot.runtime.state is McpRuntimeState.READY:
                slot.runtime.state = McpRuntimeState.AWAITING_ACK

    def _mark_dirty(self) -> None:
        self._dirty = True
        if self._connected:
            self._changed.set()

    def consume_registration_signal(self) -> None:
        self._changed.clear()
        if (self._dirty or self._fatal_error is not None) and self._connected:
            self._changed.set()

    def _track(self, task: asyncio.Task[Any]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        was_cancelled_validation = task in self._cancelled_validation_tasks
        self._cancelled_validation_tasks.discard(task)
        if not task.cancelled():
            with contextlib.suppress(BaseException):
                error = task.exception()
                if isinstance(error, ProtocolError) and not was_cancelled_validation:
                    self._fatal_error = error
                    if self._connected:
                        self._changed.set()

    def _add_tombstone(self, validation_id: UUID) -> None:
        if validation_id in self._tombstones:
            return
        if len(self._tombstones) >= _VALIDATION_TOMBSTONE_MAX:
            raise ProtocolError("MCP validation tombstone capacity was exhausted")
        self._tombstones.add(validation_id)
        self._tombstone_order.append(validation_id)

    def _consume_tombstone(self, validation_id: UUID) -> bool:
        if validation_id not in self._tombstones:
            return False
        self._tombstones.remove(validation_id)
        with contextlib.suppress(ValueError):
            self._tombstone_order.remove(validation_id)
        return True

    def _remember_recent_validation(self, validation_id: UUID) -> None:
        if validation_id in self._recent_validations:
            return
        self._recent_validations.add(validation_id)
        self._recent_validation_order.append(validation_id)
        while len(self._recent_validations) > _RECENT_VALIDATION_MAX:
            expired = self._recent_validation_order.popleft()
            self._recent_validations.discard(expired)

    def begin_validation(
        self,
        frame: ConfigValidate,
    ) -> asyncio.Task[ConfigValidateResult | None]:
        if self._closed or not self._connected:
            raise ProtocolError("MCP validation requires the current connection")
        if self._revision != frame.base_config_revision:
            raise ProtocolError("MCP validation base revision is stale")
        if self._validation_task is not None or self._candidates:
            raise ProtocolError("Concurrent MCP validations are not allowed")
        task = asyncio.create_task(
            self._run_validation(frame, self._connection_epoch),
            name=f"mcp-validate-{frame.id}",
        )
        self._validation_task = task
        self._validation_id = frame.id
        self._validation_epoch = self._connection_epoch
        self._track(task)
        return task

    async def validate(self, frame: ConfigValidate) -> ConfigValidateResult | None:
        return await self.begin_validation(frame)

    async def _run_validation(
        self,
        frame: ConfigValidate,
        connection_epoch: int,
    ) -> ConfigValidateResult | None:
        try:
            result = await self._validate_candidate_frame(frame, connection_epoch)
            if result is not None and not result.ok:
                self._remember_recent_validation(frame.id)
            return result
        finally:
            if self._validation_task is asyncio.current_task():
                self._validation_task = None
                self._validation_id = None
                self._validation_epoch = None

    async def _validate_candidate_frame(
        self,
        frame: ConfigValidate,
        connection_epoch: int,
    ) -> ConfigValidateResult | None:
        configs = {config.name: config for config in frame.candidate_config.mcp_servers}
        selected = [configs[name] for name in frame.validate_servers if name in configs]
        if len(selected) != len(frame.validate_servers):
            raise ProtocolError("MCP validation server selection is inconsistent")
        if self.draining_count:
            return ConfigValidateResult(
                id=frame.id,
                ok=False,
                failures=[
                    ProtocolValidationFailure(
                        name=config.name,
                        stage="candidate",
                        code="device_config_conflict",
                        message="A previous MCP runtime generation is still draining",
                    )
                    for config in selected
                ],
            )
        cleanup_blocked = [
            config for config in selected if self._blocked_sink(config) is not None
        ]
        if cleanup_blocked:
            return ConfigValidateResult(
                id=frame.id,
                ok=False,
                failures=[
                    ProtocolValidationFailure(
                        name=config.name,
                        stage="cleanup",
                        code="mcp_cleanup_incomplete",
                        message=(
                            f"MCP server '{config.name}' has incomplete process cleanup"
                        ),
                    )
                    for config in cleanup_blocked
                ],
            )
        if not self._secret_transport_safe and _has_secrets(
            frame.candidate_config.mcp_servers
        ):
            return ConfigValidateResult(
                id=frame.id,
                ok=False,
                failures=[
                    ProtocolValidationFailure(
                        name=config.name,
                        stage="candidate",
                        code="mcp_secret_transport_insecure",
                        message=(
                            f"MCP server '{config.name}' secrets require an HTTPS Server origin"
                        ),
                    )
                    for config in selected
                ],
            )
        try:
            validation = await self._candidate_validator(
                selected,
                cleanup_sink=self._retain_cleanup_blocked,
            )
        except asyncio.CancelledError:
            return None
        except Exception:
            return ConfigValidateResult(
                id=frame.id,
                ok=False,
                failures=[
                    ProtocolValidationFailure(
                        name=config.name,
                        stage="candidate",
                        code="config_validation_failed",
                        message=f"MCP server '{config.name}' failed candidate validation",
                    )
                    for config in selected
                ],
            )
        current_task = asyncio.current_task()
        if (
            current_task in self._cancelled_validation_tasks
            or not self._connected
            or connection_epoch != self._connection_epoch
        ):
            await self._close_candidate(validation)
            return None
        if not validation.ok:
            self._retain_cleanup_blocked(tuple(validation.runtimes.values()))
            return ConfigValidateResult(
                id=frame.id,
                ok=False,
                failures=[
                    ProtocolValidationFailure(
                        name=failure.server,
                        stage=failure.stage,
                        code=failure.code,
                        message=failure.message,
                    )
                    for failure in validation.failures
                ],
            )
        assert validation.source_catalog is not None
        discovered_names = {server.name for server in validation.source_catalog.servers}
        if discovered_names != set(frame.validate_servers):
            await self._close_candidate(validation)
            raise ProtocolError("MCP validation catalog coverage is inconsistent")

        async def expire() -> None:
            await asyncio.sleep(self._candidate_lease_seconds)
            lease = self._candidates.get(frame.id)
            if lease is not None:
                tombstone_error: ProtocolError | None = None
                try:
                    self._publish_tombstone(frame.id, lease.connection_epoch)
                except ProtocolError as exc:
                    tombstone_error = exc
                self._candidates.pop(frame.id, None)
                await self._close_candidate(lease.validation)
                if tombstone_error is not None:
                    raise tombstone_error

        expiry = asyncio.create_task(expire(), name=f"mcp-candidate-lease-{frame.id}")
        self._track(expiry)
        self._candidates[frame.id] = _CandidateLease(
            frame=frame,
            validation=validation,
            expiry=expiry,
            connection_epoch=connection_epoch,
        )
        return ConfigValidateResult(
            id=frame.id,
            ok=True,
            source_catalog=validation.source_catalog,
            failures=[],
        )

    async def cancel_validation(self, validation_id: UUID) -> None:
        if self._validation_id == validation_id and self._validation_task is not None:
            task = self._validation_task
            connection_epoch = self._validation_epoch
            assert connection_epoch is not None
            tombstone_error: ProtocolError | None = None
            try:
                self._publish_tombstone(validation_id, connection_epoch)
            except ProtocolError as exc:
                tombstone_error = exc
            self._validation_task = None
            self._validation_id = None
            self._validation_epoch = None
            self._cancelled_validation_tasks.add(task)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            if tombstone_error is not None:
                raise tombstone_error
            return
        lease = self._candidates.get(validation_id)
        if lease is not None:
            tombstone_error = None
            try:
                self._publish_tombstone(validation_id, lease.connection_epoch)
            except ProtocolError as exc:
                tombstone_error = exc
            self._candidates.pop(validation_id, None)
            lease.expiry.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await lease.expiry
            await self._close_candidate(lease.validation)
            if tombstone_error is not None:
                raise tombstone_error
            return
        if validation_id in self._tombstones:
            return
        if validation_id in self._recent_validations:
            return
        raise ProtocolError("Unknown MCP validation cancellation")

    async def activate_authoritative(
        self,
        *,
        revision: int,
        config: DeviceConfig,
        catalog: PersistedMcpCatalog,
        validation_id: UUID | None = None,
    ) -> None:
        activation = asyncio.create_task(
            self._activate_authoritative(
                revision=revision,
                config=config,
                catalog=catalog,
                validation_id=validation_id,
            ),
            name=f"mcp-activate-authoritative-{revision}",
        )
        cancelled = False
        while not activation.done():
            try:
                await asyncio.shield(activation)
            except asyncio.CancelledError:
                cancelled = True
        activation.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _activate_authoritative(
        self,
        *,
        revision: int,
        config: DeviceConfig,
        catalog: PersistedMcpCatalog,
        validation_id: UUID | None,
    ) -> None:
        configs = tuple(config.mcp_servers)
        if not self._secret_transport_safe and _has_secrets(configs):
            raise ProtocolError("MCP secrets require an HTTPS Server origin")
        names = {item.name for item in configs}
        persisted = {server.name: server for server in catalog.servers}
        if names != set(persisted):
            raise ProtocolError("MCP config and persisted catalog coverage differ")
        candidate = self._candidates.get(validation_id) if validation_id is not None else None
        promoted_names = (
            set(candidate.validation.runtimes) if candidate is not None else set()
        )
        if self.draining_count and self._authoritative_will_retire(
            configs,
            persisted,
            promoted_names,
        ):
            await self._force_close_draining()
        promoted: dict[str, McpServerRuntime] = {}
        lease = self._candidates.pop(validation_id, None) if validation_id is not None else None
        if lease is not None:
            assert validation_id is not None
            if _device_config_projection(lease.frame.candidate_config) != _device_config_projection(
                config
            ):
                self._candidates[validation_id] = lease
                raise ProtocolError("MCP candidate does not match authoritative config")
            lease.expiry.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await lease.expiry
            promoted = dict(lease.validation.runtimes)
            lease.validation.runtimes.clear()
        elif validation_id is not None and validation_id in self._tombstones:
            self._consume_tombstone(validation_id)

        previous = dict(self._slots)
        next_slots: dict[str, _RuntimeSlot] = {}
        for server_config in configs:
            server_catalog = persisted[server_config.name]
            old = previous.pop(server_config.name, None)
            runtime = promoted.pop(server_config.name, None)
            blocker = self._blocked_sink(server_config, exclude=runtime)
            if runtime is not None and blocker is not None:
                await self._close_runtime(runtime)
                runtime = None
            if runtime is not None:
                if old is not None:
                    self._retire(old)
                slot = _RuntimeSlot(server_config, runtime, server_catalog)
                next_slots[server_config.name] = slot
                self._bind_runtime(slot)
                continue
            if (
                old is not None
                and _config_projection((old.config,)) == _config_projection((server_config,))
                and _server_projection(old.persisted) == _server_projection(server_catalog)
            ):
                old.config = server_config
                old.persisted = server_catalog
                if old.runtime.state is McpRuntimeState.READY:
                    old.runtime.state = McpRuntimeState.AWAITING_ACK
                next_slots[server_config.name] = old
                continue
            blocker = self._blocked_sink(server_config)
            if old is not None and old.runtime is blocker:
                old.config = server_config
                old.persisted = server_catalog
                next_slots[server_config.name] = old
                continue
            if old is not None:
                self._retire(old)
            if blocker is not None:
                next_slots[server_config.name] = _RuntimeSlot(
                    server_config,
                    blocker,
                    server_catalog,
                )
                continue
            runtime = self._runtime_factory(server_config)
            slot = _RuntimeSlot(server_config, runtime, server_catalog)
            next_slots[server_config.name] = slot
            self._start(slot)
        for old in previous.values():
            self._retire(old)
        for runtime in promoted.values():
            self._track(asyncio.create_task(self._close_runtime(runtime)))

        self._slots = next_slots
        self._config = configs
        self._catalog = catalog
        self._revision = revision
        self._accepted.clear()
        self._mark_dirty()

    def _authoritative_will_retire(
        self,
        configs: Sequence[McpServerConfig],
        persisted: Mapping[str, PersistedMcpServerCatalog],
        promoted_names: set[str],
    ) -> bool:
        next_configs = {config.name: config for config in configs}
        for name, slot in self._slots.items():
            next_config = next_configs.get(name)
            next_catalog = persisted.get(name)
            if next_config is None or next_catalog is None or name in promoted_names:
                return True
            if (
                _config_projection((slot.config,)) != _config_projection((next_config,))
                or _server_projection(slot.persisted) != _server_projection(next_catalog)
            ):
                return True
        return False

    def _bind_runtime(self, slot: _RuntimeSlot) -> None:
        try:
            slot.runtime.bind_persisted(slot.persisted)
        except McpRuntimeError:
            pass
        self._ensure_runtime_watcher(slot)
        self._mark_dirty()

    def _ensure_runtime_watcher(self, slot: _RuntimeSlot) -> None:
        if slot.watcher is not None and not slot.watcher.done():
            return

        async def watch() -> None:
            while self._slots.get(slot.config.name) is slot:
                event = await slot.runtime.next_event()
                if self._slots.get(slot.config.name) is not slot:
                    return
                if event.kind != "transport_failed":
                    self.notify_list_changed(slot.config.name)
                    continue
                self._accepted.pop(slot.config.name, None)
                await slot.runtime.mark_transport_unavailable()
                self._record_cleanup_state(slot.runtime)
                if self._slots.get(slot.config.name) is slot:
                    self._mark_dirty()
                    self._schedule_retry(slot)
                return

        task = asyncio.create_task(watch(), name=f"mcp-watch-{slot.config.name}")
        slot.watcher = task
        self._track(task)

    def _start(self, slot: _RuntimeSlot) -> None:
        async def run() -> None:
            try:
                async with self._startup_semaphore:
                    if self._blocked_sink(
                        slot.config,
                        exclude=slot.runtime,
                    ) is not None:
                        slot.runtime.state = McpRuntimeState.UNAVAILABLE
                        slot.runtime.code = "mcp_cleanup_incomplete"
                        self._mark_dirty()
                        return
                    await slot.runtime.start()
                if self._slots.get(slot.config.name) is not slot:
                    await self._close_runtime(slot.runtime)
                    return
                self._bind_runtime(slot)
            except McpRuntimeError:
                self._record_cleanup_state(slot.runtime)
                if self._slots.get(slot.config.name) is slot:
                    self._mark_dirty()
                    self._schedule_retry(slot)
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._slots.get(slot.config.name) is slot:
                    slot.runtime.state = McpRuntimeState.UNAVAILABLE
                    slot.runtime.code = "config_validation_failed"
                    self._mark_dirty()

        self._track(asyncio.create_task(run(), name=f"mcp-start-{slot.config.name}"))

    def _schedule_retry(self, slot: _RuntimeSlot) -> None:
        self._record_cleanup_state(slot.runtime)
        if slot.retry is not None and not slot.retry.done():
            return
        if self._blocked_sink(slot.config, exclude=slot.runtime) is not None:
            slot.runtime.state = McpRuntimeState.UNAVAILABLE
            slot.runtime.code = "mcp_cleanup_incomplete"
            self._mark_dirty()
            return
        delay = slot.runtime.enter_backoff(jitter=self._random_value())
        if delay is None:
            return

        async def retry() -> None:
            try:
                await asyncio.sleep(delay)
                if self._slots.get(slot.config.name) is not slot:
                    return
                watcher = slot.watcher
                slot.watcher = None
                if watcher is not None and not watcher.done():
                    watcher.cancel()
                    await asyncio.gather(watcher, return_exceptions=True)
                if self._slots.get(slot.config.name) is not slot:
                    return
                slot.runtime.begin_retry()
                self._mark_dirty()
                self._start(slot)
            finally:
                slot.retry = None

        task = asyncio.create_task(retry(), name=f"mcp-retry-{slot.config.name}")
        slot.retry = task
        self._track(task)

    def _retire(self, slot: _RuntimeSlot) -> None:
        if slot.watcher is not None and not slot.watcher.done():
            slot.watcher.cancel()
        if slot.retry is not None and not slot.retry.done():
            slot.retry.cancel()

        async def drain() -> None:
            cancelled = False
            try:
                try:
                    await asyncio.wait_for(slot.drained.wait(), timeout=self._drain_seconds)
                except TimeoutError:
                    pass
            except asyncio.CancelledError:
                cancelled = True
            finally:
                await self._close_runtime(slot.runtime)
            if cancelled:
                raise asyncio.CancelledError

        task = asyncio.create_task(drain(), name=f"mcp-drain-{slot.config.name}")
        generation = slot.runtime.generation
        self._draining[generation] = (slot, task)
        task.add_done_callback(lambda _task: self._draining.pop(generation, None))
        self._track(task)

    async def _force_close_draining(self) -> None:
        records = tuple(self._draining.values())
        if not records:
            return
        for _slot, task in records:
            if not task.done():
                task.cancel()
        await asyncio.gather(*(task for _slot, task in records), return_exceptions=True)
        await asyncio.gather(
            *(self._close_runtime(slot.runtime) for slot, _task in records),
            return_exceptions=True,
        )

    def _desired_snapshots(self) -> list[McpRuntimeSnapshot]:
        snapshots: list[McpRuntimeSnapshot] = []
        for config in self._config:
            slot = self._slots[config.name]
            runtime = slot.runtime
            source = runtime.source_catalog
            if source is not None and runtime.state in {
                McpRuntimeState.AWAITING_ACK,
                McpRuntimeState.READY,
            }:
                snapshots.append(
                    ReadyMcpRuntimeSnapshot(
                        name=config.name,
                        runtime_generation=runtime.generation,
                        state="ready",
                        code=None,
                        source_catalog=RuntimeMcpSourceCatalog.model_validate(
                            source.model_dump(exclude={"name"}), strict=True
                        ),
                    )
                )
            elif runtime.state is McpRuntimeState.DRIFTED:
                snapshots.append(
                    DriftedMcpRuntimeSnapshot(
                        name=config.name,
                        runtime_generation=runtime.generation,
                        state="drifted",
                        code=runtime.code or "mcp_schema_drift",
                    )
                )
            else:
                snapshots.append(
                    UnavailableMcpRuntimeSnapshot(
                        name=config.name,
                        runtime_generation=runtime.generation,
                        state="unavailable",
                        code=runtime.code or "tool_mcp_unavailable",
                    )
                )
        return snapshots

    def _desired_bytes(self, snapshots: Sequence[McpRuntimeSnapshot]) -> bytes:
        return canonical_json_bytes([snapshot.model_dump(mode="json") for snapshot in snapshots])

    def next_registration(self) -> RegisterMcp | None:
        if self._fatal_error is not None:
            raise self._fatal_error
        if (
            not self._connected
            or not self._dirty
            or self._pending_registration is not None
            or self._revision is None
            or self._catalog is None
        ):
            return None
        snapshots = self._desired_snapshots()
        frame = RegisterMcp(
            id=new_uuid7(),
            config_revision=self._revision,
            catalog_digest=self._catalog.digest,
            servers=snapshots,
        )
        self._pending_registration = _RegistrationRequest(
            frame=frame,
            desired=self._desired_bytes(snapshots),
        )
        self._dirty = False
        self._changed.clear()
        return frame

    def accept_registration(self, acknowledgement: RegisterMcpAck) -> None:
        pending = self._pending_registration
        if pending is None or acknowledgement.id != pending.frame.id:
            raise ProtocolError("Unknown MCP registration acknowledgement")
        if (
            acknowledgement.config_revision != pending.frame.config_revision
            or acknowledgement.catalog_digest != pending.frame.catalog_digest
        ):
            raise ProtocolError("MCP registration acknowledgement identity is inconsistent")
        catalog = self._catalog
        if (
            self._revision != pending.frame.config_revision
            or catalog is None
            or catalog.digest != pending.frame.catalog_digest
        ):
            self._pending_registration = None
            self._mark_dirty()
            return
        requested = {snapshot.name: snapshot for snapshot in pending.frame.servers}
        results = {result.name: result for result in acknowledgement.results}
        if set(requested) != set(results):
            raise ProtocolError("MCP registration acknowledgement coverage is inconsistent")
        for name, snapshot in requested.items():
            if results[name].runtime_generation != snapshot.runtime_generation:
                raise ProtocolError("MCP registration acknowledgement generation is stale")
        self._pending_registration = None
        latest = self._desired_snapshots()
        if self._desired_bytes(latest) != pending.desired:
            self._mark_dirty()
            return
        for name, snapshot in requested.items():
            result = results[name]
            slot = self._slots.get(name)
            if slot is None or slot.runtime.generation != result.runtime_generation:
                self._mark_dirty()
                continue
            projection = canonical_json_bytes(snapshot.model_dump(mode="json"))
            if isinstance(result, AcceptedMcpRegistration):
                if not isinstance(snapshot, ReadyMcpRuntimeSnapshot):
                    raise ProtocolError("Unavailable MCP runtime cannot be accepted")
                slot.runtime.mark_ready(result.runtime_generation)
                self._accepted[name] = projection
            else:
                self._accepted.pop(name, None)
                if isinstance(snapshot, ReadyMcpRuntimeSnapshot):
                    slot.runtime.state = McpRuntimeState.DRIFTED
                    slot.runtime.code = result.code
                    self._mark_dirty()
        if self._dirty:
            self._changed.set()

    def reserve_invocation(self, call: ToolCall) -> McpInvocationLease:
        def unavailable(message: str) -> McpInvocationLease:
            return McpInvocationLease(
                self,
                call,
                _failure=fail("tool_mcp_unavailable", message),
            )

        route = call.mcp_route
        catalog = self._catalog
        if route is None or self._revision is None or catalog is None:
            return unavailable("The MCP route is not currently available")
        if route.config_revision != self._revision or route.catalog_digest != catalog.digest:
            return unavailable("The MCP route is no longer current")
        entry = next(
            (
                entry
                for server in catalog.servers
                for entry in server.entries
                if entry.entry_id == route.entry_id
            ),
            None,
        )
        if entry is None or entry.final_name != call.name or not entry.enabled:
            return unavailable("The MCP entry is no longer current")
        slot = self._slots.get(entry.server)
        if slot is None or slot.runtime.generation != route.runtime_generation:
            return unavailable("The MCP runtime is no longer current")
        snapshot = next(
            (value for value in self._desired_snapshots() if value.name == entry.server), None
        )
        if snapshot is None or self._accepted.get(entry.server) != canonical_json_bytes(
            snapshot.model_dump(mode="json")
        ):
            return unavailable("The MCP runtime is awaiting registration")
        slot.calls += 1
        slot.drained.clear()
        return McpInvocationLease(self, call, slot, entry.entry_id)

    async def invoke(self, call: ToolCall) -> ToolOutput:
        return await self.reserve_invocation(call).invoke()

    async def _invoke_reserved(
        self,
        call: ToolCall,
        slot: _RuntimeSlot,
        entry_id: UUID,
    ) -> ToolOutput:
        route = call.mcp_route
        assert route is not None
        try:
            try:
                output = await slot.runtime.invoke(
                    entry_id,
                    call.args,
                    runtime_generation=route.runtime_generation,
                    request_id=call.id,
                    max_result_bytes=call.max_result_bytes,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                return fail("tool_mcp_error", "The MCP request failed safely")
            return output
        finally:
            if slot.runtime.state not in {McpRuntimeState.READY, McpRuntimeState.AWAITING_ACK}:
                self._record_cleanup_state(slot.runtime)
                if self._slots.get(slot.config.name) is slot:
                    self._accepted.pop(slot.config.name, None)
                    self._mark_dirty()
                    if slot.runtime.state is McpRuntimeState.UNAVAILABLE:
                        self._schedule_retry(slot)

    @staticmethod
    def _release_invocation(slot: _RuntimeSlot) -> None:
        slot.calls -= 1
        if slot.calls == 0:
            slot.drained.set()

    def notify_list_changed(self, server: str) -> None:
        if server not in self._slots:
            return
        self._refresh_dirty.add(server)
        if server in self._refresh_tasks:
            return

        async def refresh_loop() -> None:
            try:
                while server in self._refresh_dirty:
                    self._refresh_dirty.discard(server)
                    await asyncio.sleep(self._list_changed_debounce_seconds)
                    slot = self._slots.get(server)
                    if slot is None:
                        return
                    self._accepted.pop(server, None)
                    try:
                        unchanged = await slot.runtime.refresh()
                        if unchanged and slot.runtime.state is McpRuntimeState.READY:
                            slot.runtime.state = McpRuntimeState.AWAITING_ACK
                    except McpRuntimeError:
                        self._record_cleanup_state(slot.runtime)
                        if slot.runtime.state is McpRuntimeState.UNAVAILABLE:
                            self._schedule_retry(slot)
                    self._mark_dirty()
            finally:
                self._refresh_tasks.pop(server, None)

        task = asyncio.create_task(refresh_loop(), name=f"mcp-list-changed-{server}")
        self._refresh_tasks[server] = task
        self._track(task)

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._connected = False
        self._pending_registration = None
        self._accepted.clear()
        self._changed.clear()
        self._recent_validations.clear()
        self._recent_validation_order.clear()
        validation_task = self._validation_task
        self._validation_task = None
        self._validation_id = None
        self._validation_epoch = None
        if validation_task is not None:
            self._cancelled_validation_tasks.add(validation_task)
            validation_task.cancel()
        for task in tuple(self._refresh_tasks.values()):
            task.cancel()
        leases = list(self._candidates.values())
        self._candidates.clear()
        for lease in leases:
            lease.expiry.cancel()
        await asyncio.gather(
            *(self._close_candidate(lease.validation) for lease in leases),
            return_exceptions=True,
        )
        pending = [task for task in self._tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.gather(
            *(self._close_runtime(slot.runtime) for slot in self._slots.values()),
            return_exceptions=True,
        )
        blocked = tuple(self._cleanup_blocked.values())
        if blocked:
            await asyncio.gather(
                *(self._close_runtime(runtime) for runtime in blocked),
                return_exceptions=True,
            )
        self._slots.clear()
        self._draining.clear()
