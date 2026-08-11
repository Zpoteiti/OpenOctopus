from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

from openctopus_server.devices.protocol import (
    MAX_TEXT_FRAME_BYTES,
    ConfigUpdateFrame,
    DeviceConfigFrame,
    ToolCallFrame,
    ToolResultFrame,
    new_uuid7,
)
from openctopus_server.devices.transfer import FairTransferAdmission, TransferManager


class DeviceUnavailableError(RuntimeError):
    pass


class DeviceBusyError(RuntimeError):
    pass


class DeviceProtocolError(RuntimeError):
    pass


class DeviceTransport(Protocol):
    async def send_text(self, payload: str) -> None: ...

    async def send_binary(self, payload: bytes) -> None: ...

    async def close(self, code: int, reason: str) -> None: ...


@dataclass(frozen=True, slots=True)
class ConnectionHandle:
    device_id: UUID
    generation: int


@dataclass(slots=True)
class _Connection:
    handle: ConnectionHandle
    user_id: UUID
    device_name: str
    transport: DeviceTransport
    last_pong: float = field(default_factory=time.monotonic)
    expected_pong: UUID | None = None
    config_update_in_flight: str | None = None
    pending: dict[UUID, _PendingCall] = field(default_factory=dict)
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
                        )
            else:
                result = await self._publish_registration(
                    device_id=device_id,
                    user_id=user_id,
                    device_name=device_name,
                    transport=transport,
                    expected_revocation_epoch=expected_revocation_epoch,
                )
            if result is None:
                return None
            handle, previous = result
        if previous is not None:
            await self._retire(
                previous,
                close_code=4000,
                close_reason="connection_replaced",
            )
        return handle

    async def _publish_registration(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        device_name: str,
        transport: DeviceTransport,
        expected_revocation_epoch: int | None,
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
            )
            previous = self._connections.get(device_id)
            self._connections[device_id] = replacement
            return handle, previous

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
        await self._retire(connection)
        return True

    async def revoke(self, device_id: UUID) -> bool:
        async with self._register_lock:
            async with self._lock:
                connection = self._connections.get(device_id)
            if connection is None:
                async with self._lock:
                    self._bump_revocation_epoch_locked(device_id)
                return False
            async with connection.lifecycle_lock:
                async with connection.send_lock:
                    async with self._lock:
                        current = self._connections.get(device_id)
                        if current is None:
                            self._bump_revocation_epoch_locked(device_id)
                            return False
                        self._connections.pop(device_id)
                        connection = current
                        self._bump_revocation_epoch_locked(device_id)
        await self._retire(connection, close_code=4401, close_reason="unauthorized")
        return True

    async def remove_device(self, device_id: UUID) -> bool:
        removed = await self.revoke(device_id)
        async with self._lock:
            self._generations.pop(device_id, None)
        return removed

    async def is_online(self, device_id: UUID, *, user_id: UUID) -> bool:
        async with self._lock:
            connection = self._connections.get(device_id)
            return not self._closed and connection is not None and connection.user_id == user_id

    async def is_current(self, handle: ConnectionHandle) -> bool:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            return not self._closed and connection is not None and connection.handle == handle

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
                or connection.user_id != user_id
                or (
                    expected_device_name is not None
                    and connection.device_name != expected_device_name
                )
            ):
                return None
            return connection.handle

    async def handle_transfer_frame(self, handle: ConnectionHandle, frame: object) -> bool:
        """Handle one inbound transfer frame only for the current generation."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if connection is None or connection.handle != handle or self._closed:
                return False
        async with connection.lifecycle_lock:
            async with self._lock:
                current = self._connections.get(handle.device_id)
                if current is not connection or current.handle != handle or self._closed:
                    return False
            await self.transfers.handle_frame(handle, frame)
        return True

    async def handle_transfer_binary(self, handle: ConnectionHandle, payload: bytes) -> bool:
        """Handle one inbound transfer chunk only for the current generation."""
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if connection is None or connection.handle != handle or self._closed:
                return False
        async with connection.lifecycle_lock:
            async with self._lock:
                current = self._connections.get(handle.device_id)
                if current is not connection or current.handle != handle or self._closed:
                    return False
            await self.transfers.handle_binary(handle, payload)
        return True

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
                or connection.user_id != user_id
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
            )
        except asyncio.CancelledError:
            await self._remove_pending(handle, call_id, future)
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
            await self._remove_pending(handle, call_id, future)

    async def resolve_tool_result(
        self,
        handle: ConnectionHandle,
        result: ToolResultFrame,
        *,
        encoded_bytes: int | None = None,
    ) -> bool:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if self._closed or connection is None or connection.handle != handle:
                return False
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
    ) -> bool:
        async with self._lock:
            current = self._connections.get(handle.device_id)
            return not (
                self._closed
                or current is not connection
                or current.handle != handle
                or (
                    expected_device_name is not None
                    and (
                        current.device_name != expected_device_name
                        or current.config_update_in_flight is not None
                    )
                )
            )

    async def send_binary(self, handle: ConnectionHandle, payload: bytes) -> bool:
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
                    expected_device_name=None,
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
            if self._closed or connection is None or connection.user_id != user_id:
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
                    connection.device_name = device_name
                    connection.config_update_in_flight = None
        except Exception:
            async with self._lock:
                if self._connections.get(device_id) is connection:
                    connection.config_update_in_flight = None
            await self.unregister(handle)
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

    async def _remove_pending(
        self,
        handle: ConnectionHandle,
        call_id: UUID,
        future: asyncio.Future[ToolResultFrame],
    ) -> None:
        async with self._lock:
            connection = self._connections.get(handle.device_id)
            if connection is not None and connection.handle == handle:
                current = connection.pending.get(call_id)
                if current is not None and current.future is future:
                    connection.pending.pop(call_id)
                    self._release_pending_locked(connection.user_id, current.byte_weight)
        if not future.done():
            future.cancel()

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
