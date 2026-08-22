from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.mcp_catalog import EMPTY_CATALOG_DIGEST
from openctopus_server.devices.mcp_routes import (
    AcceptedMcpBinding,
    FrozenMcpEntryRoute,
    McpRegistrationCandidate,
)
from openctopus_server.devices.protocol import (
    MAX_TEXT_FRAME_BYTES,
    ConfigAppliedAckFrame,
    ConfigAppliedFrame,
    ConfigUpdateFrame,
    ConfigValidateCancelFrame,
    ConfigValidateFrame,
    ConfigValidateResultFrame,
    DeviceConfigFrame,
    HelloAckFrame,
    McpRoute,
    McpValidationFailure,
    PersistedMcpCatalog,
    RegisterMcpAckFrame,
    RejectedMcpRegistration,
    ShellMetadata,
    SourceMcpCatalog,
    ToolCallFrame,
    ToolResultFrame,
    encode_server_frame,
    new_uuid7,
)
from openctopus_server.devices.transfer import (
    FairTransferAdmission,
    TransferDisconnectedError,
    TransferManager,
)


class DeviceUnavailableError(RuntimeError):
    pass


class DeviceOutcomeUnknownError(RuntimeError):
    """A call may have reached the device, but its result is not known."""


class DeviceBusyError(RuntimeError):
    pass


class DeviceProtocolError(RuntimeError):
    pass


class DeviceValidationError(RuntimeError):
    def __init__(self, failures: tuple[McpValidationFailure, ...]) -> None:
        super().__init__("Device MCP validation failed")
        self.failures = failures


class DeviceSecretTransportError(RuntimeError):
    pass


class DeviceMcpUnavailableError(RuntimeError):
    pass


UNAUTHORIZED_CLOSE_REASON = '{"code":"unauthorized"}'
MCP_REGISTRATION_WAIT_SECONDS = 9.0


class DeviceTransport(Protocol):
    async def send_text(self, payload: str) -> None: ...

    async def send_binary(self, payload: bytes) -> None: ...

    async def close(self, code: int, reason: str) -> None: ...


@dataclass(frozen=True, slots=True)
class ConnectionHandle:
    device_id: UUID
    generation: int


@dataclass(frozen=True, slots=True)
class DeviceRouteSnapshot:
    handle: ConnectionHandle
    config_epoch: int
    device_name: str = ""


@dataclass(frozen=True, slots=True)
class DeviceLiveMetadata:
    os: str
    default_shell: str
    available_shells: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ConfigValidation:
    id: UUID
    handle: ConnectionHandle
    source_catalog: SourceMcpCatalog


@dataclass(slots=True)
class _Connection:
    handle: ConnectionHandle
    user_id: UUID
    device_name: str
    transport: DeviceTransport
    operating_system: str | None = None
    default_shell: str | None = None
    available_shells: tuple[str, ...] | None = None
    last_pong: float = field(default_factory=time.monotonic)
    expected_pong: UUID | None = None
    ready: bool = True
    config_revision: int = 1
    catalog_digest: str = EMPTY_CATALOG_DIGEST
    config_epoch: int = 0
    mcp_epoch: int = 0
    config_update_in_flight: str | None = None
    config_update_done: asyncio.Event | None = None
    config_apply: _PendingConfigApply | None = None
    secret_transport_safe: bool = False
    config_validation: _PendingValidation | None = None
    validation_tombstones: OrderedDict[UUID, None] = field(default_factory=OrderedDict)
    pending: dict[UUID, _PendingCall] = field(default_factory=dict)
    call_tombstones: OrderedDict[UUID, int] = field(default_factory=OrderedDict)
    mcp_registration_in_flight: UUID | None = None
    mcp_registration_done: asyncio.Event | None = None
    mcp_registration_deadline: float | None = None
    accepted_mcp_bindings: dict[str, AcceptedMcpBinding] = field(default_factory=dict)
    lifecycle_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(slots=True)
class _PendingCall:
    future: asyncio.Future[ToolResultFrame]
    byte_weight: int
    max_result_bytes: int
    issued: bool = False


@dataclass(slots=True)
class _PendingValidation:
    id: UUID
    future: asyncio.Future[ConfigValidateResultFrame]
    validate_servers: tuple[str, ...]
    issued: bool = False


@dataclass(slots=True)
class _PendingConfigApply:
    id: UUID
    config_revision: int
    future: asyncio.Future[ConfigAppliedFrame]
    issued: bool = False
    ack_issuing: bool = False


@dataclass(slots=True)
class _PublicationFence:
    user_id: UUID
    handle: ConnectionHandle
    released: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(slots=True)
class _PendingUsage:
    calls: int = 0
    bytes: int = 0


@dataclass(slots=True)
class _ConfigLockEntry:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


class DeviceRegistry:
    def __init__(
        self,
        *,
        pending_calls_max: int = 4096,
        pending_calls_max_per_user: int = 64,
        pending_bytes_max: int = 256 * 1024 * 1024,
        pending_bytes_max_per_user: int = 32 * 1024 * 1024,
        transfer_max_concurrency: int = 32,
        transfer_max_concurrency_per_user: int = 2,
        transfer_queue_timeout_seconds: float = 5.0,
        transfer_idle_timeout_seconds: float = 30.0,
        revocation_epoch_max_entries: int = 4096,
        revocation_epoch_ttl_seconds: float = 300.0,
    ) -> None:
        if revocation_epoch_max_entries < 1:
            raise ValueError("revocation epoch cache size must be positive")
        if revocation_epoch_ttl_seconds <= 0:
            raise ValueError("revocation epoch TTL must be positive")
        self._lock = asyncio.Lock()
        self._register_lock = asyncio.Lock()
        self._closed = False
        self._connections: dict[UUID, _Connection] = {}
        self._generations: dict[UUID, int] = {}
        self._pending_calls_max = pending_calls_max
        self._pending_calls_max_per_user = pending_calls_max_per_user
        self._pending_bytes_max = pending_bytes_max
        self._pending_bytes_max_per_user = pending_bytes_max_per_user
        self._pending_calls = 0
        self._pending_bytes = 0
        self._pending_by_user: dict[UUID, _PendingUsage] = {}
        self._revocation_epoch_max_entries = revocation_epoch_max_entries
        self._revocation_epoch_ttl_seconds = revocation_epoch_ttl_seconds
        self._revocation_epochs: OrderedDict[UUID, tuple[int, float]] = OrderedDict()
        self._config_locks: dict[UUID, _ConfigLockEntry] = {}
        self._publication_fences: dict[UUID, _PublicationFence] = {}
        self._cleanup_tasks: set[asyncio.Task[bool]] = set()
        self._retirement_tasks: dict[UUID, set[asyncio.Task[None]]] = {}
        self.transfers = TransferManager(
            self,
            admission=FairTransferAdmission(
                max_concurrency=transfer_max_concurrency,
                max_concurrency_per_user=transfer_max_concurrency_per_user,
                queue_timeout_seconds=transfer_queue_timeout_seconds,
            ),
            idle_timeout_seconds=transfer_idle_timeout_seconds,
        )

    @property
    def pending_count(self) -> int:
        return sum(len(connection.pending) for connection in self._connections.values())

    async def register(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        device_name: str,
        transport: DeviceTransport,
        expected_revocation_epoch: int | None = None,
        ready: bool = True,
        operating_system: str | None = None,
        shells: ShellMetadata | None = None,
        secret_transport_safe: bool = False,
        config_revision: int = 1,
        catalog_digest: str = EMPTY_CATALOG_DIGEST,
    ) -> ConnectionHandle | None:
        async with self._publication_open(device_id):
            async with self._lock:
                if self._closed:
                    return None
                previous = self._connections.get(device_id)
            if previous is not None:
                async with previous.lifecycle_lock:
                    async with previous.send_lock:
                        result = await self._publish_registration(
                            device_id=device_id,
                            user_id=user_id,
                            device_name=device_name,
                            transport=transport,
                            expected_revocation_epoch=expected_revocation_epoch,
                            ready=ready,
                            operating_system=operating_system,
                            shells=shells,
                            secret_transport_safe=secret_transport_safe,
                            config_revision=config_revision,
                            catalog_digest=catalog_digest,
                        )
            else:
                result = await self._publish_registration(
                    device_id=device_id,
                    user_id=user_id,
                    device_name=device_name,
                    transport=transport,
                    expected_revocation_epoch=expected_revocation_epoch,
                    ready=ready,
                    operating_system=operating_system,
                    shells=shells,
                    secret_transport_safe=secret_transport_safe,
                    config_revision=config_revision,
                    catalog_digest=catalog_digest,
                )
            if result is None:
                return None
            handle, previous = result
        if previous is not None:
            retirement = self._track_retirement(
                previous.handle.device_id,
                self._retire(
                    previous,
                    close_code=4000,
                    close_reason="connection_replaced",
                ),
            )
            try:
                await await_future_cancellation_safe(retirement)
            except BaseException:
                cleanup = asyncio.create_task(self._discard_registration(handle))
                try:
                    await await_future_cancellation_safe(cleanup)
                except asyncio.CancelledError:
                    pass
                raise
        return handle

    @asynccontextmanager
    async def _publication_open(self, device_id: UUID) -> AsyncIterator[None]:
        """Serialize handle publication after any short durable transition."""
        while True:
            await self._register_lock.acquire()
            try:
                async with self._lock:
                    fence = self._publication_fences.get(device_id)
            except BaseException:
                self._register_lock.release()
                raise
            if fence is None:
                break
            self._register_lock.release()
            await fence.released.wait()
        try:
            yield
        finally:
            self._register_lock.release()

    async def _publish_registration(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        device_name: str,
        transport: DeviceTransport,
        expected_revocation_epoch: int | None,
        ready: bool,
        operating_system: str | None,
        shells: ShellMetadata | None,
        secret_transport_safe: bool,
        config_revision: int,
        catalog_digest: str,
    ) -> tuple[ConnectionHandle, _Connection | None] | None:
        async with self._lock:
            if self._closed or (
                expected_revocation_epoch is not None
                and self._revocation_epoch_locked(device_id) != expected_revocation_epoch
            ):
                return None
            generation = self._generations.get(device_id, 0) + 1
            self._generations[device_id] = generation
            handle = ConnectionHandle(device_id=device_id, generation=generation)
            replacement = _Connection(
                handle=handle,
                user_id=user_id,
                device_name=device_name,
                transport=transport,
                operating_system=operating_system,
                default_shell=shells.default if shells is not None else None,
                available_shells=tuple(shells.available) if shells is not None else None,
                ready=ready,
                config_revision=config_revision,
                catalog_digest=catalog_digest,
                secret_transport_safe=secret_transport_safe,
            )
            previous = self._connections.get(device_id)
            self._connections[device_id] = replacement
            if not ready:
                self._publication_fences[device_id] = _PublicationFence(
                    user_id=user_id,
                    handle=handle,
                )
            if previous is not None:
                self.transfers.fence_handle(previous.handle)
            return handle, previous

    async def get_live_metadata(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
    ) -> DeviceLiveMetadata | None:
        """Return hello metadata only for the current ready generation."""
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
                or connection.operating_system is None
                or connection.default_shell is None
                or connection.available_shells is None
            ):
                return None
            return DeviceLiveMetadata(
                os=connection.operating_system,
                default_shell=connection.default_shell,
                available_shells=connection.available_shells,
            )

    async def activate(
        self,
        handle: ConnectionHandle,
        frame: HelloAckFrame,
        *,
        timeout_seconds: float = 10.0,
    ) -> bool:
        """Complete hello config apply before publishing a routable generation."""
        future: asyncio.Future[ConfigAppliedFrame] = asyncio.get_running_loop().create_future()
        future.add_done_callback(_consume_future_exception)
        pending = _PendingConfigApply(frame.id, frame.config_revision, future)
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if (
                self._closed
                or connection is None
                or connection.handle != handle
                or connection.ready
                or connection.config_apply is not None
                or connection.config_revision != frame.config_revision
                or connection.catalog_digest != frame.mcp_catalog.digest
                or (
                    _config_contains_secrets(frame.config)
                    and not connection.secret_transport_safe
                )
            ):
                return False
            connection.config_apply = pending
        try:
            async with connection.send_lock:
                async with self._lock:
                    if (
                        self._connections.get(handle.device_id) is not connection
                        or connection.config_apply is not pending
                    ):
                        return False
                    pending.issued = True
                await connection.transport.send_text(encode_server_frame(frame))
            async with asyncio.timeout(timeout_seconds):
                await asyncio.shield(future)
            ack = ConfigAppliedAckFrame(id=frame.id, config_revision=frame.config_revision)
            async with connection.send_lock:
                async with self._lock:
                    if (
                        self._connections.get(handle.device_id) is not connection
                        or connection.config_apply is not pending
                    ):
                        return False
                    pending.ack_issuing = True
                await connection.transport.send_text(encode_server_frame(ack))
                async with self._lock:
                    if (
                        self._connections.get(handle.device_id) is not connection
                        or connection.config_apply is not pending
                    ):
                        return False
                    connection.device_name = frame.device_name
                    connection.config_apply = None
                    connection.ready = True
                    self._release_publication_fence_locked(
                        handle.device_id,
                        user_id=connection.user_id,
                        expected_handle=handle,
                    )
            return True
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(self._retire_ambiguous_config(connection, handle))
            try:
                await await_future_cancellation_safe(cleanup)
            except asyncio.CancelledError:
                pass
            raise
        except Exception:
            await self._retire_ambiguous_config(connection, handle)
            return False

    async def resolve_config_applied(
        self,
        handle: ConnectionHandle,
        frame: ConfigAppliedFrame,
    ) -> bool:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if self._closed or connection is None or connection.handle != handle:
                return False
            pending = connection.config_apply
            if (
                pending is None
                or pending.id != frame.id
                or pending.config_revision != frame.config_revision
                or not pending.issued
                or pending.future.done()
            ):
                return False
        pending.future.set_result(frame)
        return True

    async def registration_epoch(self, device_id: UUID) -> int:
        async with self._lock:
            return self._revocation_epoch_locked(device_id)

    async def unregister(self, handle: ConnectionHandle) -> bool:
        async with self._lock:
            current = self._connections.get(handle.device_id)
            if current is not None and current.handle == handle and not current.ready:
                self._release_publication_fence_locked(
                    handle.device_id,
                    user_id=current.user_id,
                    expected_handle=handle,
                )
        async with self._publication_open(handle.device_id):
            async with self._lock:
                connection = self._connections.get(handle.device_id)
                if connection is None or connection.handle != handle:
                    return False
            async with connection.lifecycle_lock:
                async with connection.send_lock:
                    async with self._lock:
                        current = self._connections.get(handle.device_id)
                        if current is None or current.handle != handle:
                            return False
                        self._connections.pop(handle.device_id)
        retirement = self._track_retirement(
            handle.device_id,
            self._retire(connection),
        )
        await await_future_cancellation_safe(retirement)
        return True

    async def revoke(self, device_id: UUID) -> bool:
        async with self._publication_open(device_id):
            async with self._lock:
                connection = self._connections.get(device_id)
            if connection is None:
                async with self._lock:
                    self._bump_revocation_epoch_locked(device_id)
                removed = False
            else:
                async with connection.lifecycle_lock:
                    async with connection.send_lock:
                        async with self._lock:
                            current = self._connections.get(device_id)
                            if current is None:
                                self._bump_revocation_epoch_locked(device_id)
                                removed = False
                                connection = None
                            else:
                                self._connections.pop(device_id)
                                connection = current
                                self._bump_revocation_epoch_locked(device_id)
                                removed = True
        if connection is not None:
            retirement = self._track_retirement(
                device_id,
                self._retire(
                    connection,
                    close_code=4401,
                    close_reason=UNAUTHORIZED_CLOSE_REASON,
                ),
            )
            await await_future_cancellation_safe(retirement)
        await self._wait_for_retirements(device_id)
        return removed

    async def remove_device(self, device_id: UUID) -> bool:
        removed = await self.revoke(device_id)
        async with self._lock:
            self._generations.pop(device_id, None)
        return removed

    async def remove_devices(self, device_ids: tuple[UUID, ...]) -> None:
        for device_id in device_ids:
            await self.remove_device(device_id)

    async def is_online(self, device_id: UUID, *, user_id: UUID) -> bool:
        async with self._lock:
            connection = self._connections.get(device_id)
            return (
                not self._closed
                and connection is not None
                and connection.ready
                and connection.user_id == user_id
            )

    async def is_current(self, handle: ConnectionHandle) -> bool:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            return (
                not self._closed
                and connection is not None
                and connection.ready
                and connection.handle == handle
            )

    async def can_register_mcp(self, handle: ConnectionHandle) -> bool:
        """Accept registration while the config ACK is serialized, but not yet published."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            pending = connection.config_apply if connection is not None else None
            return (
                not self._closed
                and connection is not None
                and connection.handle == handle
                and (
                    connection.ready
                    or (pending is not None and pending.ack_issuing)
                )
            )

    async def is_registered(self, handle: ConnectionHandle) -> bool:
        """Return whether this exact generation is still published, ready or not."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            return (
                not self._closed
                and connection is not None
                and connection.handle == handle
            )

    async def validate_config(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        expected_device_name: str,
        base_config_revision: int,
        candidate_config: DeviceConfigFrame,
        validate_servers: tuple[str, ...],
        timeout_seconds: float = 300.0,
    ) -> ConfigValidation:
        """Run one generation-scoped candidate validation without changing active config."""
        validation_id = new_uuid7()
        future: asyncio.Future[ConfigValidateResultFrame] = (
            asyncio.get_running_loop().create_future()
        )
        pending = _PendingValidation(
            id=validation_id,
            future=future,
            validate_servers=validate_servers,
        )
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
                or connection.device_name != expected_device_name
                or connection.config_update_in_flight is not None
            ):
                raise DeviceUnavailableError("Device is not connected")
            if _config_contains_secrets(candidate_config) and not connection.secret_transport_safe:
                raise DeviceSecretTransportError("Secret-bearing MCP config requires WSS")
            if connection.config_validation is not None:
                raise DeviceBusyError("Device MCP validation is already in progress")
            connection.config_validation = pending
            handle = connection.handle

        frame = ConfigValidateFrame(
            id=validation_id,
            base_config_revision=base_config_revision,
            candidate_config=candidate_config,
            validate_servers=list(validate_servers),
            deadline_ms=300_000,
        )
        payload = encode_server_frame(frame)
        if len(payload.encode("utf-8")) > MAX_TEXT_FRAME_BYTES:
            await self._expire_validation(connection, pending)
            raise DeviceProtocolError("MCP validation frame exceeds the text-frame limit")
        try:
            async with connection.send_lock:
                async with self._lock:
                    current = self._connections.get(device_id)
                    if (
                        self._closed
                        or current is not connection
                        or current.handle != handle
                        or current.config_validation is not pending
                    ):
                        raise DeviceUnavailableError("Device connection was replaced")
                    pending.issued = True
                await connection.transport.send_text(payload)
            try:
                async with asyncio.timeout(timeout_seconds):
                    result = await asyncio.shield(future)
            except TimeoutError:
                await self._expire_validation(connection, pending)
                await self._send_validation_cancel(connection, handle, validation_id)
                raise
        except asyncio.CancelledError:
            await self._expire_validation(connection, pending)
            if pending.issued:
                cleanup = asyncio.create_task(
                    self._send_validation_cancel(connection, handle, validation_id)
                )
                try:
                    await await_future_cancellation_safe(cleanup)
                except asyncio.CancelledError:
                    pass
            raise
        except (DeviceUnavailableError, TimeoutError):
            raise
        except Exception as exc:
            await self._expire_validation(connection, pending)
            self._schedule_unregister(handle)
            raise DeviceOutcomeUnknownError("MCP validation outcome is unknown") from exc
        finally:
            await self._clear_validation(connection, pending)

        if not result.ok:
            raise DeviceValidationError(tuple(result.failures))
        source_catalog = result.source_catalog
        if source_catalog is None:
            raise DeviceProtocolError("Successful MCP validation omitted its source catalog")
        source_names = {server.name for server in source_catalog.servers}
        if source_names != set(validate_servers):
            raise DeviceValidationError(
                (
                    McpValidationFailure(
                        name=validate_servers[0],
                        stage="discovery",
                        code="config_validation_failed",
                        message="MCP validation returned the wrong server set",
                    ),
                )
            )
        return ConfigValidation(
            id=validation_id,
            handle=handle,
            source_catalog=source_catalog,
        )

    async def resolve_config_validate_result(
        self,
        handle: ConnectionHandle,
        result: ConfigValidateResultFrame,
    ) -> bool:
        """Resolve a current candidate or consume a bounded late-result tombstone."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if (
                self._closed
                or connection is None
                or connection.handle != handle
                or not connection.ready
            ):
                return False
            pending = connection.config_validation
            if pending is None or pending.id != result.id:
                if result.id in connection.validation_tombstones:
                    connection.validation_tombstones.pop(result.id)
                    return True
                return False
            connection.config_validation = None
        if not pending.future.done():
            pending.future.set_result(result)
        return True

    async def discard_validated_config(self, validation: ConfigValidation) -> None:
        """Tell the exact still-current generation to close its temporary runtimes."""
        async with self._lock:
            connection = self._connections.get(validation.handle.device_id)
            if connection is None or connection.handle != validation.handle:
                return
        await self._send_validation_cancel(
            connection,
            validation.handle,
            validation.id,
        )

    async def _expire_validation(
        self,
        connection: _Connection,
        pending: _PendingValidation,
    ) -> None:
        retire = False
        async with self._lock:
            if connection.config_validation is pending:
                connection.config_validation = None
            if pending.issued:
                retire = not self._remember_validation_tombstone(connection, pending.id)
                if retire and self._connections.get(connection.handle.device_id) is connection:
                    connection.ready = False
        if not pending.future.done():
            pending.future.cancel()
        if retire:
            self._schedule_unregister(connection.handle)

    async def _clear_validation(
        self,
        connection: _Connection,
        pending: _PendingValidation,
    ) -> None:
        async with self._lock:
            if connection.config_validation is pending:
                connection.config_validation = None

    def _remember_validation_tombstone(self, connection: _Connection, frame_id: UUID) -> bool:
        if frame_id not in connection.validation_tombstones and len(
            connection.validation_tombstones
        ) >= 64:
            return False
        connection.validation_tombstones[frame_id] = None
        connection.validation_tombstones.move_to_end(frame_id)
        return True

    async def _send_validation_cancel(
        self,
        connection: _Connection,
        handle: ConnectionHandle,
        validation_id: UUID,
    ) -> None:
        frame = ConfigValidateCancelFrame(id=validation_id)
        try:
            async with connection.send_lock:
                async with self._lock:
                    if self._connections.get(handle.device_id) is not connection:
                        return
                await connection.transport.send_text(frame.model_dump_json())
        except Exception:
            self._schedule_unregister(handle)

    @asynccontextmanager
    async def config_update_lock(
        self,
        *,
        user_id: UUID,
        device_name: str,
        device_id: UUID | None = None,
    ) -> AsyncIterator[None]:
        """Serialize one Device's validate/commit/push cycle across renames.

        API callers provide the immutable Device ID.  ``user_id`` remains the
        fallback for internal callers that have not resolved a Device yet.
        The lock never represents a database lock and is removed when idle.
        """
        del device_name
        key = device_id or user_id
        async with self._lock:
            entry = self._config_locks.get(key)
            if entry is None:
                entry = _ConfigLockEntry()
                self._config_locks[key] = entry
            entry.users += 1
        try:
            await entry.lock.acquire()
            try:
                yield
            finally:
                entry.lock.release()
        finally:
            async with self._lock:
                entry.users -= 1
                if entry.users == 0 and not entry.lock.locked():
                    self._config_locks.pop(key, None)

    async def get_handle(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str | None = None,
    ) -> ConnectionHandle | None:
        """Return the current generation for an owned online device.

        Callers use the handle as a generation fence for transfer slots; no
        database session or transport lock is retained by this lookup.
        """
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
                or connection.config_update_in_flight is not None
                or (
                    expected_device_name is not None
                    and connection.device_name != expected_device_name
                )
            ):
                return None
            return connection.handle

    async def get_route_snapshot(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str,
    ) -> DeviceRouteSnapshot | None:
        """Capture the exact online generation and installed config revision."""
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
                or connection.device_name != expected_device_name
                or connection.config_update_in_flight is not None
            ):
                return None
            return DeviceRouteSnapshot(
                connection.handle,
                connection.config_epoch,
                connection.device_name,
            )

    async def handle_transfer_frame(self, handle: ConnectionHandle, frame: object) -> bool:
        """Handle one inbound transfer frame only for the current generation."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if (
                connection is None
                or connection.handle != handle
                or not connection.ready
                or self._closed
            ):
                return False
        try:
            await self.transfers.handle_frame(handle, frame)
        except TransferDisconnectedError:
            return False
        async with self._lock:
            current = self._connections.get(handle.device_id)
            return (
                not self._closed
                and current is connection
                and current.ready
                and current.handle == handle
            )

    async def handle_transfer_binary(self, handle: ConnectionHandle, payload: bytes) -> bool:
        """Handle one inbound transfer chunk only for the current generation."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if (
                connection is None
                or connection.handle != handle
                or not connection.ready
                or self._closed
            ):
                return False
        try:
            await self.transfers.handle_binary(handle, payload)
        except TransferDisconnectedError:
            return False
        async with self._lock:
            current = self._connections.get(handle.device_id)
            return (
                not self._closed
                and current is connection
                and current.ready
                and current.handle == handle
            )

    async def publish_mcp_registration(
        self,
        handle: ConnectionHandle,
        candidate: McpRegistrationCandidate,
        *,
        timeout_seconds: float = 10.0,
    ) -> bool:
        """ACK one aggregate registration before atomically publishing its bindings."""
        frame_id = candidate.ack.id
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        connection: _Connection | None = None
        try:
            async with asyncio.timeout_at(deadline):
                while True:
                    config_done: asyncio.Event | None = None
                    async with self._lock:
                        connection = self._connections.get(handle.device_id)
                        pending = connection.config_apply if connection is not None else None
                        if (
                            self._closed
                            or connection is None
                            or connection.handle != handle
                            or not (
                                connection.ready
                                or (pending is not None and pending.ack_issuing)
                            )
                        ):
                            return False
                        if connection.config_update_in_flight is not None:
                            config_done = connection.config_update_done
                            assert config_done is not None
                        else:
                            if connection.mcp_registration_in_flight is not None:
                                raise DeviceBusyError(
                                    "Device MCP registration is already in progress"
                                )
                            registration_done = asyncio.Event()
                            connection.mcp_registration_in_flight = frame_id
                            connection.mcp_registration_done = registration_done
                            connection.mcp_registration_deadline = deadline
                            break
                    assert config_done is not None
                    await config_done.wait()

                assert connection is not None
                async with connection.send_lock:
                    async with self._lock:
                        current = self._connections.get(handle.device_id)
                        if (
                            current is not connection
                            or not current.ready
                            or current.config_update_in_flight is not None
                            or current.mcp_registration_in_flight != frame_id
                        ):
                            return False
                        stale = (
                            current.config_revision != candidate.ack.config_revision
                            or current.catalog_digest != candidate.ack.catalog_digest
                            or any(
                                result.code == "mcp_registration_stale"
                                for result in candidate.ack.results
                            )
                        )
                        acknowledgement = (
                            _stale_mcp_registration_ack(candidate.ack)
                            if stale
                            else candidate.ack
                        )
                    await connection.transport.send_text(
                        encode_server_frame(acknowledgement)
                    )
                    async with self._lock:
                        current = self._connections.get(handle.device_id)
                        if (
                            current is not connection
                            or not current.ready
                            or current.mcp_registration_in_flight != frame_id
                        ):
                            return False
                        current.accepted_mcp_bindings = {
                            binding.name: binding
                            for binding in (() if stale else candidate.bindings)
                        }
                        current.mcp_epoch += 1
                        self._finish_mcp_registration_locked(current, frame_id)
            return True
        except asyncio.CancelledError:
            if connection is not None:
                cleanup = asyncio.create_task(
                    self._retire_ambiguous_config(connection, handle)
                )
                try:
                    await await_future_cancellation_safe(cleanup)
                except asyncio.CancelledError:
                    pass
            raise
        except Exception:
            if connection is not None:
                await self._retire_ambiguous_config(connection, handle)
            return False
        finally:
            async with self._lock:
                if connection is not None:
                    self._finish_mcp_registration_locked(connection, frame_id)

    async def dispatch_mcp_tool(
        self,
        *,
        route: FrozenMcpEntryRoute,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        chat_session_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResultFrame:
        return await self._dispatch_tool(
            device_id=route.device_id,
            user_id=user_id,
            name=name,
            args=args,
            max_result_bytes=max_result_bytes,
            timeout_seconds=timeout_seconds,
            expected_device_name=route.device_name,
            route=None,
            frozen_mcp_route=route,
            chat_session_id=chat_session_id,
            on_issued=on_issued,
        )

    async def dispatch_tool(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        expected_device_name: str | None = None,
        chat_session_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResultFrame:
        return await self._dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name=name,
            args=args,
            max_result_bytes=max_result_bytes,
            timeout_seconds=timeout_seconds,
            expected_device_name=expected_device_name,
            route=None,
            frozen_mcp_route=None,
            chat_session_id=chat_session_id,
            on_issued=on_issued,
        )

    async def dispatch_tool_on_snapshot(
        self,
        *,
        route: DeviceRouteSnapshot,
        user_id: UUID,
        expected_device_name: str,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        chat_session_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResultFrame:
        """Dispatch one private operation only through a captured route/config."""
        return await self._dispatch_tool(
            device_id=route.handle.device_id,
            user_id=user_id,
            name=name,
            args=args,
            max_result_bytes=max_result_bytes,
            timeout_seconds=timeout_seconds,
            expected_device_name=expected_device_name,
            route=route,
            frozen_mcp_route=None,
            chat_session_id=chat_session_id,
            on_issued=on_issued,
        )

    async def _dispatch_tool(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        expected_device_name: str | None,
        route: DeviceRouteSnapshot | None,
        frozen_mcp_route: FrozenMcpEntryRoute | None,
        chat_session_id: UUID | None,
        on_issued: Callable[[], None] | None,
    ) -> ToolResultFrame:
        call_id = new_uuid7()
        future = asyncio.get_running_loop().create_future()
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
                or (
                    route is not None
                    and (
                        connection.handle != route.handle
                        or connection.config_epoch != route.config_epoch
                    )
                )
            ):
                raise DeviceUnavailableError("Device is not connected")
            if (
                connection.config_update_in_flight is not None
                or (
                    expected_device_name is not None
                    and connection.device_name != expected_device_name
                )
            ):
                if frozen_mcp_route is not None:
                    raise DeviceMcpUnavailableError("MCP route is not currently available")
                raise DeviceUnavailableError("Device is not connected")
            expected_mcp_epoch: int | None = None
            wire_mcp_route: McpRoute | None = None
            if frozen_mcp_route is not None:
                binding = connection.accepted_mcp_bindings.get(frozen_mcp_route.server)
                if (
                    name != frozen_mcp_route.final_name
                    or frozen_mcp_route.device_id != device_id
                    or binding is None
                    or binding.config_revision != frozen_mcp_route.config_revision
                    or binding.catalog_digest != frozen_mcp_route.catalog_digest
                    or frozen_mcp_route.entry_id not in binding.entry_ids
                ):
                    raise DeviceMcpUnavailableError("MCP route is not currently available")
                wire_mcp_route = McpRoute(
                    entry_id=frozen_mcp_route.entry_id,
                    config_revision=binding.config_revision,
                    catalog_digest=binding.catalog_digest,
                    runtime_generation=binding.runtime_generation,
                )
                expected_mcp_epoch = connection.mcp_epoch
            frame = ToolCallFrame(
                id=call_id,
                name=name,
                args=args,
                max_result_bytes=max_result_bytes,
                chat_session_id=chat_session_id,
                mcp_route=wire_mcp_route,
            )
            payload = encode_server_frame(frame)
            if len(payload.encode("utf-8")) > MAX_TEXT_FRAME_BYTES:
                raise DeviceProtocolError("Tool call exceeds the 12 MiB text-frame limit")
            byte_weight = len(payload.encode("utf-8")) + max_result_bytes
            self._reserve_pending_locked(user_id, byte_weight)
            connection.pending[call_id] = _PendingCall(
                future=future,
                byte_weight=byte_weight,
                max_result_bytes=max_result_bytes,
            )
            handle = connection.handle

        try:
            sent = await self.send_text(
                handle,
                payload,
                expected_device_name=expected_device_name,
                expected_config_epoch=(route.config_epoch if route is not None else None),
                expected_mcp_epoch=expected_mcp_epoch,
                issued_call_id=call_id,
                on_issued=on_issued,
            )
        except asyncio.CancelledError:
            await self._remove_pending(handle, call_id, future, remember_expired=True)
            raise
        except Exception as exc:
            await self.unregister(handle)
            if future.done() and not future.cancelled():
                future.exception()
            raise DeviceOutcomeUnknownError("Device call outcome is unknown") from exc
        if not sent:
            await self._remove_pending(handle, call_id, future)
            if frozen_mcp_route is not None:
                async with self._lock:
                    current = self._connections.get(device_id)
                    if current is connection and current.ready:
                        raise DeviceMcpUnavailableError(
                            "MCP route changed before the call was issued"
                        )
            raise DeviceUnavailableError("Device connection was replaced")

        try:
            try:
                async with asyncio.timeout(timeout_seconds):
                    return await asyncio.shield(future)
            except TimeoutError as exc:
                raise DeviceOutcomeUnknownError("Device call outcome is unknown") from exc
        finally:
            await self._remove_pending(handle, call_id, future, remember_expired=True)

    async def resolve_tool_result(
        self,
        handle: ConnectionHandle,
        result: ToolResultFrame,
        *,
        encoded_bytes: int | None = None,
    ) -> bool:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.handle != handle
            ):
                return False
            pending = connection.pending.get(result.id)
            if pending is not None:
                result_bytes = encoded_bytes
                if result_bytes is None:
                    result_bytes = len(result.model_dump_json(exclude_none=True).encode("utf-8"))
                if result_bytes > pending.max_result_bytes:
                    raise DeviceProtocolError("Tool result exceeded its reserved response credit")
                connection.pending.pop(result.id)
                self._release_pending_locked(connection.user_id, pending.byte_weight)
            else:
                credit = connection.call_tombstones.pop(result.id, None)
                if credit is None:
                    return False
                result_bytes = encoded_bytes
                if result_bytes is None:
                    result_bytes = len(result.model_dump_json(exclude_none=True).encode("utf-8"))
                if result_bytes > credit:
                    raise DeviceProtocolError("Tool result exceeded its reserved response credit")
                return True
        if pending.future.done():
            return False
        pending.future.set_result(result)
        return True

    async def send_text(
        self,
        handle: ConnectionHandle,
        payload: str,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
        expected_mcp_epoch: int | None = None,
        issued_call_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> bool:
        """Send only while ``handle`` remains the current device generation."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if self._closed or connection is None or connection.handle != handle:
                return False
        try:
            async with connection.send_lock:
                if issued_call_id is not None:
                    if not await self._mark_call_issued(
                        connection,
                        handle,
                        issued_call_id,
                        expected_device_name=expected_device_name,
                        expected_config_epoch=expected_config_epoch,
                        expected_mcp_epoch=expected_mcp_epoch,
                        on_issued=on_issued,
                    ):
                        return False
                else:
                    if not await self._can_send(
                        connection,
                        handle,
                        expected_device_name=expected_device_name,
                        expected_config_epoch=expected_config_epoch,
                        expected_mcp_epoch=expected_mcp_epoch,
                    ):
                        return False
                    if on_issued is not None:
                        on_issued()
                await connection.transport.send_text(payload)
        except Exception:
            self._schedule_unregister(handle)
            raise
        return True

    async def _mark_call_issued(
        self,
        connection: _Connection,
        handle: ConnectionHandle,
        call_id: UUID,
        *,
        expected_device_name: str | None,
        expected_config_epoch: int | None,
        expected_mcp_epoch: int | None,
        on_issued: Callable[[], None] | None,
    ) -> bool:
        async with self._lock:
            pending = connection.pending.get(call_id)
            if (
                not self._can_send_locked(
                    connection,
                    handle,
                    expected_device_name=expected_device_name,
                    expected_config_epoch=expected_config_epoch,
                    expected_mcp_epoch=expected_mcp_epoch,
                )
                or pending is None
            ):
                return False
            pending.issued = True
            if on_issued is not None:
                on_issued()
            return True

    async def _can_send(
        self,
        connection: _Connection,
        handle: ConnectionHandle,
        *,
        expected_device_name: str | None,
        expected_config_epoch: int | None = None,
        expected_mcp_epoch: int | None = None,
    ) -> bool:
        async with self._lock:
            return self._can_send_locked(
                connection,
                handle,
                expected_device_name=expected_device_name,
                expected_config_epoch=expected_config_epoch,
                expected_mcp_epoch=expected_mcp_epoch,
            )

    def _can_send_locked(
        self,
        connection: _Connection,
        handle: ConnectionHandle,
        *,
        expected_device_name: str | None,
        expected_config_epoch: int | None,
        expected_mcp_epoch: int | None,
    ) -> bool:
        current = self._connections.get(handle.device_id)
        return not (
            self._closed
            or current is not connection
            or not current.ready
            or current.handle != handle
            or (expected_config_epoch is not None and current.config_epoch != expected_config_epoch)
            or (expected_mcp_epoch is not None and current.mcp_epoch != expected_mcp_epoch)
            or (
                expected_device_name is not None
                and (
                    current.device_name != expected_device_name
                    or current.config_update_in_flight is not None
                )
            )
        )

    async def send_binary(
        self,
        handle: ConnectionHandle,
        payload: bytes,
        *,
        expected_device_name: str | None = None,
        expected_config_epoch: int | None = None,
    ) -> bool:
        """Send one transfer frame while fencing stale generations."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if self._closed or connection is None or connection.handle != handle:
                return False
        sender = getattr(connection.transport, "send_binary", None)
        if sender is None:
            raise DeviceUnavailableError("Device transport does not support binary frames")
        try:
            async with connection.send_lock:
                if not await self._can_send(
                    connection,
                    handle,
                    expected_device_name=expected_device_name,
                    expected_config_epoch=expected_config_epoch,
                ):
                    return False
                await sender(payload)
        except Exception:
            self._schedule_unregister(handle)
            raise
        return True

    async def send_ping(self, handle: ConnectionHandle, ping_id: UUID, payload: str) -> bool:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if self._closed or connection is None or connection.handle != handle:
                return False
        try:
            async with connection.send_lock:
                if not await self._can_send(
                    connection,
                    handle,
                    expected_device_name=None,
                ):
                    return False
                async with self._lock:
                    current = self._connections.get(handle.device_id)
                    if self._closed or current is not connection or current.handle != handle:
                        return False
                    connection.expected_pong = ping_id
                await connection.transport.send_text(payload)
        except Exception:
            async with self._lock:
                if self._connections.get(handle.device_id) is connection:
                    connection.expected_pong = None
            await self.unregister(handle)
            raise
        return True

    async def push_config(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        device_name: str,
        config: DeviceConfigFrame,
        config_revision: int = 1,
        mcp_catalog: PersistedMcpCatalog | None = None,
        frame_id: UUID | None = None,
        expected_handle: ConnectionHandle | None = None,
        timeout_seconds: float = 10.0,
    ) -> bool:
        catalog = mcp_catalog or PersistedMcpCatalog(
            version=1,
            digest=EMPTY_CATALOG_DIGEST,
            servers=[],
        )
        frame = ConfigUpdateFrame(
            id=frame_id or new_uuid7(),
            device_name=device_name,
            config_revision=config_revision,
            config=config,
            mcp_catalog=catalog,
        )
        future: asyncio.Future[ConfigAppliedFrame] = asyncio.get_running_loop().create_future()
        future.add_done_callback(_consume_future_exception)
        pending = _PendingConfigApply(frame.id, frame.config_revision, future)
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
                or (expected_handle is not None and connection.handle != expected_handle)
            ):
                self._release_publication_fence_locked(
                    device_id,
                    user_id=user_id,
                    expected_handle=expected_handle,
                )
                return False
            if connection.config_apply is not None:
                raise DeviceBusyError("Device configuration apply is already in progress")
            handle = connection.handle
            insecure_secret_transport = (
                _config_contains_secrets(config) and not connection.secret_transport_safe
            )
            if not insecure_secret_transport:
                if connection.config_update_in_flight is None:
                    connection.config_update_in_flight = device_name
                    connection.config_update_done = asyncio.Event()
                connection.config_apply = pending
            self._release_publication_fence_locked(
                device_id,
                user_id=user_id,
                expected_handle=handle,
            )
        if insecure_secret_transport:
            await self._retire_ambiguous_config(connection, handle)
            return False
        try:
            async with connection.send_lock:
                async with self._lock:
                    current = self._connections.get(device_id)
                    if (
                        current is not connection
                        or current.handle != handle
                        or connection.config_apply is not pending
                    ):
                        return False
                    pending.issued = True
                await connection.transport.send_text(encode_server_frame(frame))
            async with asyncio.timeout(timeout_seconds):
                await asyncio.shield(future)
            ack = ConfigAppliedAckFrame(id=frame.id, config_revision=frame.config_revision)
            async with connection.send_lock:
                async with self._lock:
                    current = self._connections.get(device_id)
                    if (
                        current is not connection
                        or current.handle != handle
                        or connection.config_apply is not pending
                    ):
                        return False
                    pending.ack_issuing = True
                await connection.transport.send_text(encode_server_frame(ack))
                async with self._lock:
                    current = self._connections.get(device_id)
                    if (
                        current is not connection
                        or current.handle != handle
                        or connection.config_apply is not pending
                    ):
                        return False
                    previous_route = DeviceRouteSnapshot(
                        handle,
                        connection.config_epoch,
                        connection.device_name,
                    )
                    self.transfers.fence_route(previous_route)
                    connection.device_name = device_name
                    connection.config_revision = config_revision
                    connection.catalog_digest = catalog.digest
                    connection.config_epoch += 1
                    connection.mcp_epoch += 1
                    connection.accepted_mcp_bindings.clear()
                    connection.config_apply = None
                    connection.config_update_in_flight = None
                    self._finish_config_update_locked(connection)
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(self._retire_ambiguous_config(connection, handle))
            try:
                await await_future_cancellation_safe(cleanup)
            except asyncio.CancelledError:
                pass
            raise
        except Exception:
            await self._retire_ambiguous_config(connection, handle)
            return False
        return True

    async def begin_config_update(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        expected_handle: ConnectionHandle | None = None,
    ) -> bool:
        """Fence new tool calls before the corresponding DB policy commit."""
        wait_budget_deadline = (
            asyncio.get_running_loop().time() + MCP_REGISTRATION_WAIT_SECONDS
        )
        while True:
            registration_done: asyncio.Event | None = None
            registration_deadline: float | None = None
            registration_handle: ConnectionHandle | None = None
            async with self._publication_open(device_id):
                async with self._lock:
                    connection = self._connections.get(device_id)
                    if (
                        self._closed
                        or connection is None
                        or not connection.ready
                        or connection.user_id != user_id
                        or (expected_handle is not None and connection.handle != expected_handle)
                    ):
                        return False
                    if (
                        connection.config_update_in_flight is not None
                        or device_id in self._publication_fences
                    ):
                        raise DeviceBusyError("Device configuration is already updating")
                    if connection.mcp_registration_in_flight is not None:
                        registration_done = connection.mcp_registration_done
                        registration_deadline = connection.mcp_registration_deadline
                        registration_handle = connection.handle
                        assert registration_done is not None
                        assert registration_deadline is not None
                    else:
                        connection.config_update_in_flight = "__precommit__"
                        connection.config_update_done = asyncio.Event()
                        self._publication_fences[device_id] = _PublicationFence(
                            user_id=user_id,
                            handle=connection.handle,
                        )
                        return True
            assert registration_done is not None
            assert registration_deadline is not None
            assert registration_handle is not None
            try:
                async with asyncio.timeout_at(
                    min(registration_deadline, wait_budget_deadline)
                ):
                    await registration_done.wait()
            except TimeoutError:
                async with self._lock:
                    current = self._connections.get(device_id)
                    if (
                        current is not None
                        and current.handle == registration_handle
                        and current.mcp_registration_done is registration_done
                    ):
                        current.ready = False
                        self._schedule_unregister(current.handle)
                return False

    async def abort_config_update(self, *, device_id: UUID, user_id: UUID) -> None:
        """Release a pre-commit fence when the database mutation rolls back."""
        async with self._lock:
            connection = self._connections.get(device_id)
            if connection is not None and connection.user_id == user_id:
                if connection.config_update_in_flight == "__precommit__":
                    connection.config_update_in_flight = None
                    self._finish_config_update_locked(connection)
            self._release_publication_fence_locked(device_id, user_id=user_id)

    async def retire_config_update(self, *, device_id: UUID, user_id: UUID) -> None:
        """Retire a fenced generation when the policy commit outcome is unknown."""
        async with self._lock:
            connection = self._connections.get(device_id)
            self._release_publication_fence_locked(device_id, user_id=user_id)
            if connection is None or connection.user_id != user_id:
                return
            handle = connection.handle
        await self._retire_ambiguous_config(connection, handle)

    def _release_publication_fence_locked(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_handle: ConnectionHandle | None = None,
    ) -> None:
        fence = self._publication_fences.get(device_id)
        if (
            fence is None
            or fence.user_id != user_id
            or (expected_handle is not None and fence.handle != expected_handle)
        ):
            return
        self._publication_fences.pop(device_id, None)
        fence.released.set()

    @staticmethod
    def _finish_config_update_locked(connection: _Connection) -> None:
        done = connection.config_update_done
        connection.config_update_done = None
        if done is not None:
            done.set()

    @staticmethod
    def _finish_mcp_registration_locked(connection: _Connection, frame_id: UUID) -> None:
        if connection.mcp_registration_in_flight != frame_id:
            return
        done = connection.mcp_registration_done
        connection.mcp_registration_in_flight = None
        connection.mcp_registration_done = None
        connection.mcp_registration_deadline = None
        if done is not None:
            done.set()

    @staticmethod
    def _finish_all_mcp_registration_locked(connection: _Connection) -> None:
        done = connection.mcp_registration_done
        connection.mcp_registration_in_flight = None
        connection.mcp_registration_done = None
        connection.mcp_registration_deadline = None
        if done is not None:
            done.set()

    async def mark_pong(
        self,
        handle: ConnectionHandle,
        pong_id: UUID,
        *,
        at: float | None = None,
    ) -> bool:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if (
                self._closed
                or connection is None
                or connection.handle != handle
                or connection.expected_pong != pong_id
            ):
                return False
            connection.last_pong = time.monotonic() if at is None else at
            connection.expected_pong = None
            return True

    async def last_pong(self, handle: ConnectionHandle) -> float | None:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if self._closed or connection is None or connection.handle != handle:
                return None
            return connection.last_pong

    def _schedule_unregister(self, handle: ConnectionHandle) -> None:
        task = asyncio.create_task(self.unregister(handle))
        self._cleanup_tasks.add(task)

        def _consume_cleanup(completed: asyncio.Task[bool]) -> None:
            self._cleanup_tasks.discard(completed)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(_consume_cleanup)

    async def close(self) -> None:
        async with self._register_lock:
            async with self._lock:
                if self._closed:
                    return
                self._closed = True
                publication_fences = tuple(self._publication_fences.values())
                self._publication_fences.clear()
                for fence in publication_fences:
                    fence.released.set()
                connections = list(self._connections.values())
            for connection in connections:
                async with connection.lifecycle_lock:
                    async with connection.send_lock:
                        async with self._lock:
                            if self._connections.get(connection.handle.device_id) is connection:
                                self._connections.pop(connection.handle.device_id)
        await self.transfers.close()
        await asyncio.gather(
            *(
                self._retire(connection, close_code=1001, close_reason="server_shutdown")
                for connection in connections
            ),
            return_exceptions=True,
        )
        cleanup_tasks = tuple(self._cleanup_tasks)
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        await self._wait_for_all_retirements()

    async def _remove_pending(
        self,
        handle: ConnectionHandle,
        call_id: UUID,
        future: asyncio.Future[ToolResultFrame],
        *,
        remember_expired: bool = False,
    ) -> None:
        retire = False
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if connection is not None and connection.handle == handle:
                current = connection.pending.get(call_id)
                if current is not None and current.future is future:
                    if remember_expired and current.issued:
                        connection.pending.pop(call_id)
                        self._release_pending_locked(connection.user_id, current.byte_weight)
                        if len(connection.call_tombstones) >= 256:
                            retire = True
                            connection.ready = False
                        else:
                            connection.call_tombstones[call_id] = current.max_result_bytes
                    else:
                        connection.pending.pop(call_id)
                        self._release_pending_locked(connection.user_id, current.byte_weight)
        if not future.done():
            future.cancel()
        if retire:
            self._schedule_unregister(handle)

    async def _discard_registration(self, handle: ConnectionHandle) -> None:
        connection: _Connection | None = None
        async with self._register_lock:
            async with self._lock:
                current = self._connections.get(handle.device_id)
                if current is not None and current.handle == handle:
                    connection = self._connections.pop(handle.device_id)
                    self._release_publication_fence_locked(
                        handle.device_id,
                        user_id=current.user_id,
                        expected_handle=handle,
                    )
        if connection is not None:
            retirement = self._track_retirement(
                handle.device_id,
                self._retire(
                    connection,
                    close_code=1001,
                    close_reason="registration_cancelled",
                ),
            )
            await await_future_cancellation_safe(retirement)

    async def _retire_ambiguous_config(
        self,
        connection: _Connection,
        handle: ConnectionHandle,
    ) -> None:
        removed = False
        async with self._lock:
            connection.config_update_in_flight = None
            self._finish_config_update_locked(connection)
            self._finish_all_mcp_registration_locked(connection)
            self._release_publication_fence_locked(
                handle.device_id,
                user_id=connection.user_id,
                expected_handle=handle,
            )
            if self._connections.get(handle.device_id) is connection:
                self._connections.pop(handle.device_id)
                removed = True
        if removed:
            retirement = self._track_retirement(
                handle.device_id,
                self._retire(
                    connection,
                    close_code=1011,
                    close_reason="config_update_failed",
                ),
            )
            await await_future_cancellation_safe(retirement)

    def _track_retirement(
        self,
        device_id: UUID,
        retirement: Coroutine[Any, Any, None],
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(retirement)
        tasks = self._retirement_tasks.setdefault(device_id, set())
        tasks.add(task)

        def _completed(completed: asyncio.Task[None]) -> None:
            current = self._retirement_tasks.get(device_id)
            if current is None:
                return
            current.discard(completed)
            if not current:
                self._retirement_tasks.pop(device_id, None)

        task.add_done_callback(_completed)
        return task

    async def _wait_for_retirements(self, device_id: UUID) -> None:
        while tasks := tuple(self._retirement_tasks.get(device_id, ())):
            waiter = asyncio.ensure_future(asyncio.gather(*tasks))
            await await_future_cancellation_safe(waiter)

    async def _wait_for_all_retirements(self) -> None:
        while tasks := tuple(
            task
            for device_tasks in self._retirement_tasks.values()
            for task in device_tasks
            if not task.done()
        ):
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _retire(
        self,
        connection: _Connection,
        *,
        close_code: int | None = None,
        close_reason: str = "",
    ) -> None:
        await self.transfers.disconnect(connection.handle)
        async with self._lock:
            connection.config_update_in_flight = None
            self._finish_config_update_locked(connection)
            self._finish_all_mcp_registration_locked(connection)
            validation = connection.config_validation
            connection.config_validation = None
            config_apply = connection.config_apply
            connection.config_apply = None
            pending = list(connection.pending.values())
            connection.pending.clear()
            connection.call_tombstones.clear()
            connection.validation_tombstones.clear()
            connection.accepted_mcp_bindings.clear()
            for call in pending:
                self._release_pending_locked(connection.user_id, call.byte_weight)
        for future in pending:
            if not future.future.done():
                error: Exception
                if future.issued:
                    error = DeviceOutcomeUnknownError("Device call outcome is unknown")
                else:
                    error = DeviceUnavailableError("Device disconnected")
                future.future.set_exception(error)
        if validation is not None and not validation.future.done():
            error = (
                DeviceOutcomeUnknownError("MCP validation outcome is unknown")
                if validation.issued
                else DeviceUnavailableError("Device disconnected")
            )
            validation.future.set_exception(error)
        if config_apply is not None and not config_apply.future.done():
            config_apply.future.set_exception(DeviceUnavailableError("Device disconnected"))
        if close_code is not None:
            try:
                await connection.transport.close(close_code, close_reason)
            except Exception:
                pass

    def _reserve_pending_locked(self, user_id: UUID, byte_weight: int) -> None:
        usage = self._pending_by_user.get(user_id)
        user_calls = usage.calls if usage is not None else 0
        user_bytes = usage.bytes if usage is not None else 0
        if (
            self._pending_calls >= self._pending_calls_max
            or user_calls >= self._pending_calls_max_per_user
            or self._pending_bytes + byte_weight > self._pending_bytes_max
            or user_bytes + byte_weight > self._pending_bytes_max_per_user
        ):
            raise DeviceBusyError("Device pending-call capacity is exhausted")
        if usage is None:
            usage = _PendingUsage()
            self._pending_by_user[user_id] = usage
        self._pending_calls += 1
        self._pending_bytes += byte_weight
        usage.calls += 1
        usage.bytes += byte_weight

    def _release_pending_locked(self, user_id: UUID, byte_weight: int) -> None:
        usage = self._pending_by_user[user_id]
        self._pending_calls -= 1
        self._pending_bytes -= byte_weight
        usage.calls -= 1
        usage.bytes -= byte_weight
        if usage.calls == 0:
            self._pending_by_user.pop(user_id)

    def _revocation_epoch_locked(self, device_id: UUID) -> int:
        now = time.monotonic()
        for cached_device, (_, expires_at) in tuple(self._revocation_epochs.items()):
            if expires_at <= now:
                self._revocation_epochs.pop(cached_device, None)
        entry = self._revocation_epochs.get(device_id)
        if entry is None:
            return 0
        self._revocation_epochs.move_to_end(device_id)
        return entry[0]

    def _bump_revocation_epoch_locked(self, device_id: UUID) -> int:
        current = self._revocation_epoch_locked(device_id)
        epoch = current + 1
        self._revocation_epochs[device_id] = (
            epoch,
            time.monotonic() + self._revocation_epoch_ttl_seconds,
        )
        self._revocation_epochs.move_to_end(device_id)
        while len(self._revocation_epochs) > self._revocation_epoch_max_entries:
            self._revocation_epochs.popitem(last=False)
        return epoch


def _config_contains_secrets(config: DeviceConfigFrame) -> bool:
    for server in config.mcp_servers:
        payload = server.storage_dict()
        values = payload.get("env", payload.get("headers", {}))
        if isinstance(values, dict) and values:
            return True
    return False


def _stale_mcp_registration_ack(frame: RegisterMcpAckFrame) -> RegisterMcpAckFrame:
    return RegisterMcpAckFrame(
        id=frame.id,
        config_revision=frame.config_revision,
        catalog_digest=frame.catalog_digest,
        results=[
            RejectedMcpRegistration(
                name=result.name,
                runtime_generation=result.runtime_generation,
                accepted=False,
                code="mcp_registration_stale",
            )
            for result in frame.results
        ],
    )


def _consume_future_exception(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()
