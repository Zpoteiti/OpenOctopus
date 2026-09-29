"""Fair admission and transferable operation leases for device transfers."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID


class TransferBusyError(TimeoutError):
    code = "workspace_transfer_busy"


@dataclass(slots=True)
class TransferLease:
    """Idempotent ownership of one global and one per-user transfer slot."""

    user_id: UUID
    _release: Callable[[], Awaitable[None]]
    _owner: object
    _closed: bool = False

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        release_task: asyncio.Future[None] = asyncio.ensure_future(self._release())
        try:
            await asyncio.shield(release_task)
        except asyncio.CancelledError:
            await asyncio.shield(release_task)
            raise

    async def __aenter__(self) -> TransferLease:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def validate(
        self,
        admission: FairTransferAdmission,
        *,
        user_id: UUID,
        slot_id: UUID,
    ) -> None:
        if self._owner is not admission or self.user_id != user_id or self._closed:
            raise ValueError("transfer operation lease is not active for this user")
        if slot_id.version != 7:
            raise ValueError("transfer slot id must be UUIDv7")


@dataclass(slots=True)
class _Waiter:
    user_id: UUID
    future: asyncio.Future[TransferLease]
    queued: bool = True


class FairTransferAdmission:
    """Global/per-user transfer admission with round-robin user fairness.

    Waiters are kept in one FIFO per user.  Once a user has an admitted slot,
    that user is rotated behind other non-empty users, so a slow user cannot
    monopolize the global service.  The queue has no semaphore waiter ordering
    dependency and every timeout/cancellation removes its waiter.
    """

    def __init__(
        self,
        *,
        max_concurrency: int,
        max_concurrency_per_user: int,
        queue_timeout_seconds: float,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("global transfer concurrency must be positive")
        if not 1 <= max_concurrency_per_user <= max_concurrency:
            raise ValueError("per-user transfer concurrency is invalid")
        if queue_timeout_seconds <= 0:
            raise ValueError("transfer queue timeout must be positive")
        self.max_concurrency = max_concurrency
        self.max_concurrency_per_user = max_concurrency_per_user
        self.queue_timeout_seconds = queue_timeout_seconds
        self._lock = asyncio.Lock()
        self._active = 0
        self._active_by_user: dict[UUID, int] = {}
        self._waiters: dict[UUID, deque[_Waiter]] = {}
        self._round_robin: deque[UUID] = deque()

    @property
    def active_count(self) -> int:
        return self._active

    @property
    def waiting_count(self) -> int:
        return sum(len(waiters) for waiters in self._waiters.values())

    @property
    def active_by_user(self) -> dict[UUID, int]:
        return dict(self._active_by_user)

    async def acquire(self, user_id: UUID) -> TransferLease:
        loop = asyncio.get_running_loop()
        waiter = _Waiter(user_id=user_id, future=loop.create_future())
        async with self._lock:
            if self._can_grant_locked(user_id) and not self._round_robin:
                lease = self._grant_locked(user_id)
                return lease
            queue = self._waiters.get(user_id)
            if (
                queue is not None and len(queue) >= self.max_concurrency_per_user
            ) or self.waiting_count >= self.max_concurrency:
                raise TransferBusyError
            queue = self._waiters.setdefault(user_id, deque())
            queue.append(waiter)
            if user_id not in self._round_robin:
                self._round_robin.append(user_id)
            self._drain_locked()
        try:
            async with asyncio.timeout(self.queue_timeout_seconds):
                return await asyncio.shield(waiter.future)
        except TimeoutError as exc:
            await self._cleanup_waiter(waiter)
            raise TransferBusyError from exc
        except asyncio.CancelledError:
            await self._cleanup_waiter(waiter)
            raise

    async def _cleanup_waiter(self, waiter: _Waiter) -> None:
        """Remove a waiter and close a lease granted at timeout/cancel boundary."""

        lease: TransferLease | None = None
        async with self._lock:
            if waiter.queued:
                waiter.queued = False
                wait_queue = self._waiters.get(waiter.user_id)
                if wait_queue is not None:
                    try:
                        wait_queue.remove(waiter)
                    except ValueError:
                        pass
                    if not wait_queue:
                        self._waiters.pop(waiter.user_id, None)
                        try:
                            self._round_robin.remove(waiter.user_id)
                        except ValueError:
                            pass
                self._drain_locked()
            if waiter.future.done() and not waiter.future.cancelled():
                lease = waiter.future.result()
            elif not waiter.future.done():
                waiter.future.cancel()
        if lease is not None:
            close_task = asyncio.create_task(lease.aclose())
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError:
                await asyncio.shield(close_task)
                raise

    def _can_grant_locked(self, user_id: UUID) -> bool:
        return self._active < self.max_concurrency and self._active_by_user.get(user_id, 0) < (
            self.max_concurrency_per_user
        )

    def _grant_locked(self, user_id: UUID) -> TransferLease:
        self._active += 1
        self._active_by_user[user_id] = self._active_by_user.get(user_id, 0) + 1
        return TransferLease(user_id, lambda: self._release(user_id), self)

    def _drain_locked(self) -> None:
        if not self._round_robin:
            return
        # At most one full rotation can be blocked by per-user limits.
        blocked = 0
        while (
            self._active < self.max_concurrency
            and self._round_robin
            and blocked < len(self._round_robin)
        ):
            user_id = self._round_robin.popleft()
            queue = self._waiters.get(user_id)
            if not queue:
                self._waiters.pop(user_id, None)
                blocked = 0
                continue
            if not self._can_grant_locked(user_id):
                self._round_robin.append(user_id)
                blocked += 1
                continue
            waiter = queue.popleft()
            waiter.queued = False
            lease = self._grant_locked(user_id)
            if queue:
                self._round_robin.append(user_id)
            else:
                self._waiters.pop(user_id, None)
            blocked = 0
            if not waiter.future.done():
                waiter.future.set_result(lease)

    async def _release(self, user_id: UUID) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            current = self._active_by_user.get(user_id, 0)
            if current <= 1:
                self._active_by_user.pop(user_id, None)
            else:
                self._active_by_user[user_id] = current - 1
            self._drain_locked()
