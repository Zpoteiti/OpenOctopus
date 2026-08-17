from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.protocol import (
    MAX_TEXT_FRAME_BYTES,
    ConfigUpdateFrame,
    DeviceConfigFrame,
    ToolCallFrame,
    ToolResultFrame,
    new_uuid7,
)
from openctopus_server.devices.transfer import (
    FairTransferAdmission,
    TransferDisconnectedError,
    TransferManager,
)


class DeviceUnavailableError(RuntimeError):
    pass


class DeviceBusyError(RuntimeError):
    pass


class DeviceProtocolError(RuntimeError):
    pass


UNAUTHORIZED_CLOSE_REASON = '{"code":"unauthorized"}'
_EXPIRED_CALL_TOMBSTONE_MAX_ENTRIES = 4096
_EXPIRED_CALL_TOMBSTONE_TTL_SECONDS = 300.0


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


@dataclass(slots=True)
class _Connection:
    handle: ConnectionHandle
    user_id: UUID
    device_name: str
    transport: DeviceTransport
    last_pong: float = field(default_factory=time.monotonic)
    expected_pong: UUID | None = None
    ready: bool = True
    config_epoch: int = 0
    config_update_in_flight: str | None = None
    pending: dict[UUID, _PendingCall] = field(default_factory=dict)
    expired_calls: OrderedDict[UUID, float] = field(default_factory=OrderedDict)
    lifecycle_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(slots=True)
class _PendingCall:
    future: asyncio.Future[ToolResultFrame]
    byte_weight: int
    max_result_bytes: int


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
    ) -> ConnectionHandle | None:
        async with self._register_lock:
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
                        )
            else:
                result = await self._publish_registration(
                    device_id=device_id,
                    user_id=user_id,
                    device_name=device_name,
                    transport=transport,
                    expected_revocation_epoch=expected_revocation_epoch,
                    ready=ready,
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

    async def _publish_registration(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        device_name: str,
        transport: DeviceTransport,
        expected_revocation_epoch: int | None,
        ready: bool,
    ) -> tuple[ConnectionHandle, _Connection | None] | None:
        async with self._lock:
            if (
                self._closed
                or (
                    expected_revocation_epoch is not None
                    and self._revocation_epoch_locked(device_id) != expected_revocation_epoch
                )
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
                ready=ready,
            )
            previous = self._connections.get(device_id)
            self._connections[device_id] = replacement
            if previous is not None:
                self.transfers.fence_handle(previous.handle)
            return handle, previous

    async def activate(self, handle: ConnectionHandle, payload: str) -> bool:
        """Write ``hello_ack`` before making a handshake generation routable."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if (
                self._closed
                or connection is None
                or connection.handle != handle
                or connection.ready
            ):
                return False
        try:
            async with connection.send_lock:
                async with self._lock:
                    current = self._connections.get(handle.device_id)
                    if (
                        self._closed
                        or current is not connection
                        or current.handle != handle
                        or current.ready
                    ):
                        return False
                await connection.transport.send_text(payload)
                async with self._lock:
                    current = self._connections.get(handle.device_id)
                    if self._closed or current is not connection or current.handle != handle:
                        return False
                    connection.ready = True
        except Exception:
            self._schedule_unregister(handle)
            raise
        return True

    async def registration_epoch(self, device_id: UUID) -> int:
        async with self._lock:
            return self._revocation_epoch_locked(device_id)

    async def unregister(self, handle: ConnectionHandle) -> bool:
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
        async with self._register_lock:
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

    @asynccontextmanager
    async def config_update_lock(
        self,
        *,
        user_id: UUID,
        device_name: str,
    ) -> AsyncIterator[None]:
        """Serialize commit-and-push cycles for one user's device configs.

        A rename changes the URL name, so a name-keyed lock would let a second
        PATCH overtake the first under the new name.  The per-user lock keeps
        commit and wire order aligned across that boundary.  It never holds a
        DB transaction while waiting on the transport and is removed when idle.
        """
        del device_name
        key = user_id
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
    ) -> ToolResultFrame:
        call_id = new_uuid7()
        future = asyncio.get_running_loop().create_future()
        frame = ToolCallFrame(
            id=call_id,
            name=name,
            args=args,
            max_result_bytes=max_result_bytes,
        )
        payload = frame.model_dump_json()
        if len(payload.encode("utf-8")) > MAX_TEXT_FRAME_BYTES:
            raise DeviceProtocolError("Tool call exceeds the 12 MiB text-frame limit")
        byte_weight = len(payload.encode("utf-8")) + max_result_bytes
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
                or connection.config_update_in_flight is not None
                or (
                    route is not None
                    and (
                        connection.handle != route.handle
                        or connection.config_epoch != route.config_epoch
                    )
                )
                or (
                    expected_device_name is not None
                    and connection.device_name != expected_device_name
                )
            ):
                raise DeviceUnavailableError("Device is not connected")
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
            )
        except asyncio.CancelledError:
            await self._remove_pending(handle, call_id, future, remember_expired=True)
            raise
        except Exception as exc:
            await self.unregister(handle)
            if future.done() and not future.cancelled():
                future.exception()
            raise DeviceUnavailableError("Device connection failed") from exc
        if not sent:
            await self._remove_pending(handle, call_id, future)
            raise DeviceUnavailableError("Device connection was replaced")

        try:
            async with asyncio.timeout(timeout_seconds):
                return await asyncio.shield(future)
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
            self._expire_call_tombstones_locked(connection)
            pending = connection.pending.get(result.id)
            if pending is not None:
                result_bytes = encoded_bytes
                if result_bytes is None:
                    result_bytes = len(result.model_dump_json(exclude_none=True).encode("utf-8"))
                if result_bytes > pending.max_result_bytes:
                    raise DeviceProtocolError("Tool result exceeded its reserved response credit")
                connection.pending.pop(result.id)
            if pending is not None:
                self._release_pending_locked(connection.user_id, pending.byte_weight)
            elif result.id in connection.expired_calls:
                return True
        if pending is None or pending.future.done():
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
    ) -> bool:
        """Send only while ``handle`` remains the current device generation."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if self._closed or connection is None or connection.handle != handle:
                return False
        try:
            async with connection.send_lock:
                if not await self._can_send(
                    connection,
                    handle,
                    expected_device_name=expected_device_name,
                    expected_config_epoch=expected_config_epoch,
                ):
                    return False
                await connection.transport.send_text(payload)
        except Exception:
            self._schedule_unregister(handle)
            raise
        return True

    async def _can_send(
        self,
        connection: _Connection,
        handle: ConnectionHandle,
        *,
        expected_device_name: str | None,
        expected_config_epoch: int | None = None,
    ) -> bool:
        async with self._lock:
            current = self._connections.get(handle.device_id)
            return not (
                self._closed
                or current is not connection
                or not current.ready
                or current.handle != handle
                or (
                    expected_config_epoch is not None
                    and current.config_epoch != expected_config_epoch
                )
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
    ) -> bool:
        async with self._lock:
            connection = self._connections.get(device_id)
            if (
                self._closed
                or connection is None
                or not connection.ready
                or connection.user_id != user_id
            ):
                return False
            handle = connection.handle
        frame = ConfigUpdateFrame(
            id=new_uuid7(),
            device_name=device_name,
            config=config,
        )
        try:
            async with self._lock:
                current = self._connections.get(device_id)
                if current is not connection or current.handle != handle:
                    return False
                connection.config_update_in_flight = device_name
            async with connection.send_lock:
                async with self._lock:
                    current = self._connections.get(device_id)
                    if (
                        current is not connection
                        or current.handle != handle
                        or connection.config_update_in_flight != device_name
                    ):
                        return False
                await connection.transport.send_text(frame.model_dump_json())
                async with self._lock:
                    current = self._connections.get(device_id)
                    if (
                        current is not connection
                        or current.handle != handle
                        or connection.config_update_in_flight != device_name
                    ):
                        return False
                    previous_route = DeviceRouteSnapshot(
                        handle,
                        connection.config_epoch,
                        connection.device_name,
                    )
                    self.transfers.fence_route(previous_route)
                    connection.device_name = device_name
                    connection.config_epoch += 1
                    connection.config_update_in_flight = None
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(
                self._retire_ambiguous_config(connection, handle)
            )
            try:
                await await_future_cancellation_safe(cleanup)
            except asyncio.CancelledError:
                pass
            raise
        except Exception:
            await self._retire_ambiguous_config(connection, handle)
            return False
        return True

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
                connections = list(self._connections.values())
            for connection in connections:
                async with connection.lifecycle_lock:
                    async with connection.send_lock:
                        async with self._lock:
                            if self._connections.get(connection.handle.device_id) is connection:
                                self._connections.pop(connection.handle.device_id)
        await self.transfers.close()
        await asyncio.gather(
            *(self._retire(connection, close_code=1001, close_reason="server_shutdown") for connection in connections),
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
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if connection is not None and connection.handle == handle:
                current = connection.pending.get(call_id)
                if current is not None and current.future is future:
                    connection.pending.pop(call_id)
                    self._release_pending_locked(connection.user_id, current.byte_weight)
                    if remember_expired:
                        self._remember_expired_call_locked(connection, call_id)
        if not future.done():
            future.cancel()

    async def _discard_registration(self, handle: ConnectionHandle) -> None:
        connection: _Connection | None = None
        async with self._register_lock:
            async with self._lock:
                current = self._connections.get(handle.device_id)
                if current is not None and current.handle == handle:
                    connection = self._connections.pop(handle.device_id)
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
            pending = list(connection.pending.values())
            connection.pending.clear()
            for call in pending:
                self._release_pending_locked(connection.user_id, call.byte_weight)
        for future in pending:
            if not future.future.done():
                future.future.set_exception(DeviceUnavailableError("Device disconnected"))
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

    def _remember_expired_call_locked(self, connection: _Connection, call_id: UUID) -> None:
        self._expire_call_tombstones_locked(connection)
        connection.expired_calls[call_id] = (
            time.monotonic() + _EXPIRED_CALL_TOMBSTONE_TTL_SECONDS
        )
        connection.expired_calls.move_to_end(call_id)
        while len(connection.expired_calls) > _EXPIRED_CALL_TOMBSTONE_MAX_ENTRIES:
            connection.expired_calls.popitem(last=False)

    @staticmethod
    def _expire_call_tombstones_locked(connection: _Connection) -> None:
        now = time.monotonic()
        for call_id, expires_at in tuple(connection.expired_calls.items()):
            if expires_at > now:
                break
            connection.expired_calls.pop(call_id, None)

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
