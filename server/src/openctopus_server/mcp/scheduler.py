from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum, StrEnum
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe

GLOBAL_MAX_RESERVED = 32
PER_USER_MAX_RESERVED = 4
QUEUE_DEADLINE_SECONDS = 5.0
PUBLIC_DEADLINE_SECONDS = 60.0


class AdmissionClock(Protocol):
    """The small clock surface needed for deterministic admission deadlines."""

    def now(self) -> float: ...

    async def sleep_until(self, deadline: float) -> None: ...


class EventLoopAdmissionClock:
    def now(self) -> float:
        return asyncio.get_running_loop().time()

    async def sleep_until(self, deadline: float) -> None:
        delay = deadline - self.now()
        if delay > 0:
            await asyncio.sleep(delay)


class AdmissionFailure(StrEnum):
    BUSY = "busy"
    UNAVAILABLE = "unavailable"


class ServerMcpAdmissionError(RuntimeError):
    code: AdmissionFailure


class ServerMcpBusyError(ServerMcpAdmissionError):
    code = AdmissionFailure.BUSY

    def __init__(self) -> None:
        super().__init__("server MCP admission is busy")


class ServerMcpUnavailableError(ServerMcpAdmissionError):
    code = AdmissionFailure.UNAVAILABLE

    def __init__(self) -> None:
        super().__init__("server MCP runtime is unavailable")


def _validate_runtime_limit(max_concurrent_calls: int) -> None:
    if isinstance(max_concurrent_calls, bool) or not 1 <= max_concurrent_calls <= 32:
        raise ValueError("max_concurrent_calls must be an integer in 1..32")


def runtime_waiting_capacity(max_concurrent_calls: int) -> int:
    _validate_runtime_limit(max_concurrent_calls)
    return min(128, max(8, 4 * max_concurrent_calls))


class _LeasePhase(Enum):
    ACTIVE = "active"
    DRAINING = "draining"
    RELEASED = "released"


class _CallState(Enum):
    QUEUED = "queued"
    ISSUED = "issued"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class CoordinatorSnapshot:
    active: int
    draining: int
    reserved: int
    reserved_by_user: Mapping[UUID, int]


@dataclass(frozen=True, slots=True)
class IssuedAdmission:
    invocation: object
    lease: AdmissionLease
    enqueued_at: float
    issued_at: float
    public_deadline: float

    def remaining_public_seconds(self, now: float) -> float:
        return max(0.0, self.public_deadline - now)


class AdmissionLease:
    """One atomic runtime/global/user reservation.

    Remote invocations call ``mark_draining`` at their public timeout and keep
    this lease until the late result is consumed or the generation is closed.
    """

    __slots__ = ("_coordinator", "_phase", "_runtime", "_user_id")

    def __init__(
        self,
        coordinator: ServerMcpCoordinator,
        runtime: RuntimeAdmission,
        user_id: UUID,
    ) -> None:
        self._coordinator = coordinator
        self._runtime = runtime
        self._user_id = user_id
        self._phase = _LeasePhase.ACTIVE

    @property
    def active(self) -> bool:
        return self._phase is _LeasePhase.ACTIVE

    @property
    def draining(self) -> bool:
        return self._phase is _LeasePhase.DRAINING

    @property
    def released(self) -> bool:
        return self._phase is _LeasePhase.RELEASED

    async def mark_draining(self) -> None:
        await _finish_cancellation_safely(self._coordinator._mark_draining(self))

    async def aclose(self) -> None:
        await _finish_cancellation_safely(self._coordinator._release(self))

    async def __aenter__(self) -> AdmissionLease:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


async def _finish_cancellation_safely[T](awaitable: Awaitable[T]) -> T:
    task = asyncio.ensure_future(awaitable)
    return await await_future_cancellation_safe(task)


@dataclass(slots=True)
class _QueuedCall:
    user_id: UUID
    sequence: int
    enqueued_at: float
    queue_deadline: float
    public_deadline: float
    start: Callable[[AdmissionLease], object]
    future: asyncio.Future[IssuedAdmission]
    state: _CallState = _CallState.QUEUED


class AdmissionTicket:
    """A submitted call which is either already issued or waiting boundedly."""

    __slots__ = ("_call", "_runtime")

    def __init__(self, runtime: RuntimeAdmission, call: _QueuedCall) -> None:
        self._runtime = runtime
        self._call = call

    @property
    def issued(self) -> bool:
        return self._call.state is _CallState.ISSUED

    @property
    def enqueued_at(self) -> float:
        return self._call.enqueued_at

    @property
    def public_deadline(self) -> float:
        return self._call.public_deadline

    async def wait(self) -> IssuedAdmission:
        if self._call.future.done():
            return self._call.future.result()

        deadline_task = asyncio.create_task(
            self._runtime._coordinator.clock.sleep_until(self._call.queue_deadline)
        )
        try:
            done, _ = await asyncio.wait(
                (self._call.future, deadline_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._call.future in done:
                return self._call.future.result()
            await self._runtime._expire(self._call)
            return self._call.future.result()
        except asyncio.CancelledError:
            await _finish_cancellation_safely(self._runtime._cancel(self._call))
            raise
        finally:
            if not deadline_task.done():
                deadline_task.cancel()
            try:
                await deadline_task
            except asyncio.CancelledError:
                pass

    async def cancel(self) -> bool:
        """Remove a call only while it is provably still queued."""

        return await _finish_cancellation_safely(self._runtime._cancel(self._call))


class ServerMcpCoordinator:
    """Atomically owns all process-wide Server MCP admission counters."""

    def __init__(self, *, clock: AdmissionClock | None = None) -> None:
        self.clock = clock or EventLoopAdmissionClock()
        self._lock = asyncio.Lock()
        self._active = 0
        self._draining = 0
        self._reserved_by_user: dict[UUID, int] = {}
        self._slots: set[RuntimeAdmission] = set()
        self._ready_slots: deque[RuntimeAdmission] = deque()
        self._closed = False

    def create_runtime(self, *, max_concurrent_calls: int) -> RuntimeAdmission:
        _validate_runtime_limit(max_concurrent_calls)
        runtime = RuntimeAdmission(
            coordinator=self,
            max_concurrent_calls=max_concurrent_calls,
            accepting=not self._closed,
        )
        if not self._closed:
            self._slots.add(runtime)
        return runtime

    def snapshot(self) -> CoordinatorSnapshot:
        return CoordinatorSnapshot(
            active=self._active,
            draining=self._draining,
            reserved=self._active + self._draining,
            reserved_by_user=MappingProxyType(dict(self._reserved_by_user)),
        )

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            for runtime in tuple(self._slots):
                runtime._retire_locked()
            self._slots.clear()
            self._ready_slots.clear()

    def _mark_ready_locked(self, runtime: RuntimeAdmission) -> None:
        if runtime._ready or not runtime._accepting or runtime._waiting_count == 0:
            return
        runtime._ready = True
        self._ready_slots.append(runtime)

    def _remove_ready_locked(self, runtime: RuntimeAdmission) -> None:
        if not runtime._ready:
            return
        runtime._ready = False
        try:
            self._ready_slots.remove(runtime)
        except ValueError:
            pass

    def _can_reserve_for_user_locked(self, user_id: UUID) -> bool:
        return self._reserved_by_user.get(user_id, 0) < PER_USER_MAX_RESERVED

    def _reserve_locked(self, runtime: RuntimeAdmission, user_id: UUID) -> AdmissionLease:
        self._active += 1
        self._reserved_by_user[user_id] = self._reserved_by_user.get(user_id, 0) + 1
        runtime._active_count += 1
        return AdmissionLease(self, runtime, user_id)

    def _rollback_start_locked(self, lease: AdmissionLease) -> None:
        self._release_counters_locked(lease)

    def _release_counters_locked(self, lease: AdmissionLease) -> None:
        if lease._phase is _LeasePhase.RELEASED:
            return
        if lease._phase is _LeasePhase.ACTIVE:
            self._active -= 1
            lease._runtime._active_count -= 1
        else:
            self._draining -= 1
            lease._runtime._draining_count -= 1
        current = self._reserved_by_user[lease._user_id]
        if current == 1:
            del self._reserved_by_user[lease._user_id]
        else:
            self._reserved_by_user[lease._user_id] = current - 1
        lease._phase = _LeasePhase.RELEASED

    async def _mark_draining(self, lease: AdmissionLease) -> None:
        async with self._lock:
            if lease._phase is not _LeasePhase.ACTIVE:
                return
            self._active -= 1
            self._draining += 1
            lease._runtime._active_count -= 1
            lease._runtime._draining_count += 1
            lease._phase = _LeasePhase.DRAINING

    async def _release(self, lease: AdmissionLease) -> None:
        async with self._lock:
            if lease._phase is _LeasePhase.RELEASED:
                return
            self._release_counters_locked(lease)
            self._drain_locked()

    def _drain_locked(self) -> None:
        while self._active + self._draining < GLOBAL_MAX_RESERVED and self._ready_slots:
            attempts = len(self._ready_slots)
            granted = False
            for _ in range(attempts):
                runtime = self._ready_slots.popleft()
                runtime._ready = False
                granted = runtime._try_issue_one_locked()
                self._mark_ready_locked(runtime)
                if granted:
                    break
            if not granted:
                return


class RuntimeAdmission:
    """One generation's bounded per-user queue and runtime reservation count."""

    def __init__(
        self,
        *,
        coordinator: ServerMcpCoordinator,
        max_concurrent_calls: int,
        accepting: bool,
    ) -> None:
        self._coordinator = coordinator
        self.max_concurrent_calls = max_concurrent_calls
        self.waiting_capacity = runtime_waiting_capacity(max_concurrent_calls)
        self._accepting = accepting
        self._active_count = 0
        self._draining_count = 0
        self._waiting_count = 0
        self._next_sequence = 0
        self._waiters: dict[UUID, deque[_QueuedCall]] = {}
        self._ready_users: deque[UUID] = deque()
        self._ready = False

    @property
    def accepting(self) -> bool:
        return self._accepting

    @property
    def active_count(self) -> int:
        return self._active_count

    @property
    def draining_count(self) -> int:
        return self._draining_count

    @property
    def reserved_count(self) -> int:
        return self._active_count + self._draining_count

    @property
    def waiting_count(self) -> int:
        return self._waiting_count

    async def submit(
        self,
        user_id: UUID,
        start: Callable[[AdmissionLease], object],
    ) -> AdmissionTicket:
        """Submit bounded metadata and synchronously start when permits are available.

        ``start`` runs under the coordinator lock. It must only create and register
        the invocation task, must not block, and becomes the lease owner once it
        returns. That ownership preserves an issued call if caller cancellation
        wins the race with delivery of the returned ticket.
        """

        loop = asyncio.get_running_loop()
        async with self._coordinator._lock:
            if not self._accepting or self._coordinator._closed:
                raise ServerMcpUnavailableError
            if self._waiting_count >= self.waiting_capacity:
                raise ServerMcpBusyError
            now = self._coordinator.clock.now()
            call = _QueuedCall(
                user_id=user_id,
                sequence=self._next_sequence,
                enqueued_at=now,
                queue_deadline=now + QUEUE_DEADLINE_SECONDS,
                public_deadline=now + PUBLIC_DEADLINE_SECONDS,
                start=start,
                future=loop.create_future(),
            )
            self._next_sequence += 1
            queue = self._waiters.get(user_id)
            if queue is None:
                queue = deque()
                self._waiters[user_id] = queue
                self._ready_users.append(user_id)
            queue.append(call)
            self._waiting_count += 1
            self._coordinator._mark_ready_locked(self)
            self._coordinator._drain_locked()
            return AdmissionTicket(self, call)

    async def admit(
        self,
        user_id: UUID,
        start: Callable[[AdmissionLease], object],
    ) -> IssuedAdmission:
        """Submit and wait, removing the queued call if this task is cancelled."""

        ticket = await self.submit(user_id, start)
        return await ticket.wait()

    async def retire(self) -> None:
        async with self._coordinator._lock:
            if not self._accepting:
                return
            self._retire_locked()
            self._coordinator._slots.discard(self)
            self._coordinator._drain_locked()

    def _retire_locked(self) -> None:
        self._accepting = False
        self._coordinator._remove_ready_locked(self)
        for queue in self._waiters.values():
            for call in queue:
                call.state = _CallState.FAILED
                if not call.future.done():
                    call.future.set_exception(ServerMcpUnavailableError())
        self._waiters.clear()
        self._ready_users.clear()
        self._waiting_count = 0

    def _try_issue_one_locked(self) -> bool:
        if not self._accepting or self.reserved_count >= self.max_concurrent_calls:
            return False
        if self._coordinator._active + self._coordinator._draining >= GLOBAL_MAX_RESERVED:
            return False

        checked_users = 0
        while self._ready_users and checked_users < len(self._ready_users):
            user_id = self._ready_users.popleft()
            queue = self._waiters[user_id]
            self._expire_queue_head_locked(queue)
            if not queue:
                del self._waiters[user_id]
                checked_users = 0
                continue
            if not self._coordinator._can_reserve_for_user_locked(user_id):
                self._ready_users.append(user_id)
                checked_users += 1
                continue

            call = queue.popleft()
            self._waiting_count -= 1
            if queue:
                self._ready_users.append(user_id)
            else:
                del self._waiters[user_id]

            lease = self._coordinator._reserve_locked(self, user_id)
            issued_at = self._coordinator.clock.now()
            try:
                invocation = call.start(lease)
            except BaseException as exc:
                call.state = _CallState.FAILED
                self._coordinator._rollback_start_locked(lease)
                if not call.future.done():
                    call.future.set_exception(exc)
                checked_users = 0
                continue

            call.state = _CallState.ISSUED
            call.future.set_result(
                IssuedAdmission(
                    invocation=invocation,
                    lease=lease,
                    enqueued_at=call.enqueued_at,
                    issued_at=issued_at,
                    public_deadline=call.public_deadline,
                )
            )
            return True
        return False

    def _expire_queue_head_locked(self, queue: deque[_QueuedCall]) -> None:
        now = self._coordinator.clock.now()
        while queue and queue[0].queue_deadline <= now:
            call = queue.popleft()
            self._waiting_count -= 1
            call.state = _CallState.FAILED
            if not call.future.done():
                call.future.set_exception(ServerMcpBusyError())

    async def _expire(self, call: _QueuedCall) -> None:
        async with self._coordinator._lock:
            if call.state is not _CallState.QUEUED:
                return
            if self._coordinator.clock.now() < call.queue_deadline:
                return
            self._remove_queued_locked(call)
            call.state = _CallState.FAILED
            if not call.future.done():
                call.future.set_exception(ServerMcpBusyError())
            self._coordinator._drain_locked()

    async def _cancel(self, call: _QueuedCall) -> bool:
        async with self._coordinator._lock:
            if call.state is not _CallState.QUEUED:
                return False
            self._remove_queued_locked(call)
            call.state = _CallState.CANCELLED
            if not call.future.done():
                call.future.cancel()
            self._coordinator._drain_locked()
            return True

    def _remove_queued_locked(self, call: _QueuedCall) -> None:
        queue = self._waiters.get(call.user_id)
        if queue is None:
            return
        try:
            queue.remove(call)
        except ValueError:
            return
        self._waiting_count -= 1
        if not queue:
            del self._waiters[call.user_id]
            try:
                self._ready_users.remove(call.user_id)
            except ValueError:
                pass
        if self._waiting_count == 0:
            self._coordinator._remove_ready_locked(self)
