from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import UUID

import pytest

from openctopus_server.mcp.scheduler import (
    GLOBAL_MAX_RESERVED,
    PER_USER_MAX_RESERVED,
    PUBLIC_DEADLINE_SECONDS,
    QUEUE_DEADLINE_SECONDS,
    AdmissionClock,
    AdmissionLease,
    IssuedAdmission,
    RuntimeAdmission,
    ServerMcpBusyError,
    ServerMcpCoordinator,
    ServerMcpUnavailableError,
    runtime_waiting_capacity,
)

_USER_A = UUID("01890f7c-bb80-7000-8000-000000000001")
_USER_B = UUID("01890f7c-bb80-7000-8000-000000000002")
_USER_C = UUID("01890f7c-bb80-7000-8000-000000000003")
_USER_HOLDER = UUID("01890f7c-bb80-7000-8000-000000000004")


class FatalStart(BaseException):
    pass


class FakeClock(AdmissionClock):
    def __init__(self) -> None:
        self.current = 0.0
        self.sleep_entered = asyncio.Event()
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def now(self) -> float:
        return self.current

    async def sleep_until(self, deadline: float) -> None:
        if deadline <= self.current:
            return
        future = asyncio.get_running_loop().create_future()
        item = (deadline, future)
        self._sleepers.append(item)
        self.sleep_entered.set()
        try:
            await future
        finally:
            if item in self._sleepers:
                self._sleepers.remove(item)

    def advance(self, seconds: float) -> None:
        self.current += seconds
        for deadline, future in tuple(self._sleepers):
            if deadline <= self.current and not future.done():
                future.set_result(None)


def _start_recorder(started: list[str], label: str) -> Callable[[AdmissionLease], object]:
    def start(_lease: AdmissionLease) -> object:
        started.append(label)
        return label

    return start


async def _issue(
    runtime: RuntimeAdmission,
    user_id: UUID,
    started: list[str],
    label: str,
) -> IssuedAdmission:
    return await runtime.admit(user_id, _start_recorder(started, label))


def test_fixed_limits_and_runtime_waiting_capacity() -> None:
    assert GLOBAL_MAX_RESERVED == 32
    assert PER_USER_MAX_RESERVED == 4
    assert QUEUE_DEADLINE_SECONDS == 5.0
    assert PUBLIC_DEADLINE_SECONDS == 60.0
    assert runtime_waiting_capacity(1) == 8
    assert runtime_waiting_capacity(2) == 8
    assert runtime_waiting_capacity(8) == 32
    assert runtime_waiting_capacity(32) == 128

    coordinator = ServerMcpCoordinator(clock=FakeClock())
    with pytest.raises(ValueError, match="1..32"):
        coordinator.create_runtime(max_concurrent_calls=0)
    with pytest.raises(ValueError, match="1..32"):
        coordinator.create_runtime(max_concurrent_calls=33)
    with pytest.raises(ValueError, match="1..32"):
        coordinator.create_runtime(max_concurrent_calls=True)


async def test_same_user_fifo_and_user_round_robin() -> None:
    clock = FakeClock()
    coordinator = ServerMcpCoordinator(clock=clock)
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holder = await _issue(runtime, _USER_HOLDER, started, "holder")

    tickets = [
        await runtime.submit(_USER_A, _start_recorder(started, "a1")),
        await runtime.submit(_USER_A, _start_recorder(started, "a2")),
        await runtime.submit(_USER_B, _start_recorder(started, "b1")),
        await runtime.submit(_USER_B, _start_recorder(started, "b2")),
    ]
    assert started == ["holder"]

    await holder.lease.aclose()
    issued = await tickets[0].wait()
    assert started == ["holder", "a1"]
    await issued.lease.aclose()

    issued = await tickets[2].wait()
    assert started == ["holder", "a1", "b1"]
    await issued.lease.aclose()

    issued = await tickets[1].wait()
    assert started == ["holder", "a1", "b1", "a2"]
    await issued.lease.aclose()

    issued = await tickets[3].wait()
    assert started == ["holder", "a1", "b1", "a2", "b2"]
    await issued.lease.aclose()


async def test_queue_full_is_immediate_and_bounded() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holder = await _issue(runtime, _USER_HOLDER, started, "holder")
    queued = [
        await runtime.submit(_USER_A, _start_recorder(started, f"q{index}")) for index in range(8)
    ]

    assert runtime.waiting_count == 8
    with pytest.raises(ServerMcpBusyError):
        await runtime.submit(_USER_B, _start_recorder(started, "overflow"))
    assert runtime.waiting_count == 8
    assert started == ["holder"]

    await runtime.retire()
    for ticket in queued:
        with pytest.raises(ServerMcpUnavailableError):
            await ticket.wait()
    await holder.lease.aclose()


async def test_queue_deadline_expires_without_issuing() -> None:
    clock = FakeClock()
    coordinator = ServerMcpCoordinator(clock=clock)
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holder = await _issue(runtime, _USER_HOLDER, started, "holder")
    ticket = await runtime.submit(_USER_A, _start_recorder(started, "late"))

    wait_task = asyncio.create_task(ticket.wait())
    await clock.sleep_entered.wait()
    clock.advance(QUEUE_DEADLINE_SECONDS)
    with pytest.raises(ServerMcpBusyError):
        await wait_task
    assert runtime.waiting_count == 0
    assert started == ["holder"]

    await holder.lease.aclose()


async def test_queue_time_reduces_the_public_invocation_budget() -> None:
    clock = FakeClock()
    coordinator = ServerMcpCoordinator(clock=clock)
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holder = await _issue(runtime, _USER_HOLDER, started, "holder")
    ticket = await runtime.submit(_USER_A, _start_recorder(started, "issued"))

    clock.advance(4.0)
    await holder.lease.aclose()
    issued = await ticket.wait()

    assert issued.enqueued_at == 0.0
    assert issued.issued_at == 4.0
    assert issued.public_deadline == PUBLIC_DEADLINE_SECONDS
    assert issued.remaining_public_seconds(clock.now()) == 56.0
    await issued.lease.aclose()


async def test_cancelling_a_queued_wait_atomically_removes_it() -> None:
    clock = FakeClock()
    coordinator = ServerMcpCoordinator(clock=clock)
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holder = await _issue(runtime, _USER_HOLDER, started, "holder")
    ticket = await runtime.submit(_USER_A, _start_recorder(started, "cancelled"))

    wait_task = asyncio.create_task(ticket.wait())
    await clock.sleep_entered.wait()
    wait_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait_task

    assert runtime.waiting_count == 0
    assert started == ["holder"]
    await holder.lease.aclose()


async def test_repeated_ticket_cancel_cannot_leave_a_queued_call_to_issue() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holder = await _issue(runtime, _USER_HOLDER, started, "holder")
    ticket = await runtime.submit(_USER_A, _start_recorder(started, "cancelled"))

    await coordinator._lock.acquire()
    cancel_task = asyncio.create_task(ticket.cancel())
    try:
        await asyncio.sleep(0)
        cancel_task.cancel()
        await asyncio.sleep(0)
        cancel_task.cancel()
        await asyncio.sleep(0)
        assert cancel_task.done() is False
    finally:
        coordinator._lock.release()

    with pytest.raises(asyncio.CancelledError):
        await cancel_task
    assert runtime.waiting_count == 0
    await holder.lease.aclose()
    assert started == ["holder"]


async def test_retire_rejects_new_calls_and_fails_only_waiting_calls() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holder = await _issue(runtime, _USER_HOLDER, started, "holder")
    queued = await runtime.submit(_USER_A, _start_recorder(started, "queued"))

    await runtime.retire()

    with pytest.raises(ServerMcpUnavailableError):
        await queued.wait()
    with pytest.raises(ServerMcpUnavailableError):
        await runtime.submit(_USER_B, _start_recorder(started, "new"))
    assert runtime.reserved_count == 1
    assert coordinator.snapshot().reserved == 1

    await holder.lease.aclose()
    assert runtime.reserved_count == 0


async def test_one_user_cap_is_atomic_across_runtimes_and_release_wakes_peer() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    first = coordinator.create_runtime(max_concurrent_calls=8)
    second = coordinator.create_runtime(max_concurrent_calls=8)
    started: list[str] = []

    tickets = await asyncio.gather(
        *(first.submit(_USER_A, _start_recorder(started, f"a{index}")) for index in range(3)),
        second.submit(_USER_A, _start_recorder(started, "a3")),
        second.submit(_USER_A, _start_recorder(started, "a4")),
    )
    issued_tickets = [ticket for ticket in tickets if ticket.issued]
    queued_tickets = [ticket for ticket in tickets if not ticket.issued]
    issued = [await ticket.wait() for ticket in issued_tickets]

    assert len(started) == PER_USER_MAX_RESERVED
    assert coordinator.snapshot().reserved_by_user == {_USER_A: PER_USER_MAX_RESERVED}
    assert first.waiting_count + second.waiting_count == 1
    assert len(queued_tickets) == 1

    await issued[0].lease.aclose()
    fifth = await queued_tickets[0].wait()
    assert len(started) == PER_USER_MAX_RESERVED + 1

    await fifth.lease.aclose()
    for call in issued[1:]:
        await call.lease.aclose()


async def test_global_release_notifies_a_different_runtime() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    first = coordinator.create_runtime(max_concurrent_calls=32)
    second = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    holders = []
    for index in range(GLOBAL_MAX_RESERVED):
        user_id = UUID(int=100 + index)
        holders.append(await _issue(first, user_id, started, f"holder-{index}"))

    queued = await second.submit(_USER_C, _start_recorder(started, "peer"))
    assert second.waiting_count == 1
    assert coordinator.snapshot().reserved == GLOBAL_MAX_RESERVED

    await holders[0].lease.aclose()
    peer = await queued.wait()
    assert started[-1] == "peer"
    assert coordinator.snapshot().reserved == GLOBAL_MAX_RESERVED

    await peer.lease.aclose()
    for holder in holders[1:]:
        await holder.lease.aclose()


async def test_five_hundred_call_pressure_retains_only_active_and_queue_capacity() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=8)
    started: list[str] = []

    async def submit(index: int):
        try:
            return await runtime.submit(
                UUID(int=1_000 + index),
                _start_recorder(started, f"call-{index}"),
            )
        except ServerMcpBusyError:
            return None

    results = await asyncio.gather(*(submit(index) for index in range(500)))
    issued_tickets = [ticket for ticket in results if ticket is not None and ticket.issued]
    queued_tickets = [ticket for ticket in results if ticket is not None and not ticket.issued]

    assert len(issued_tickets) == 8
    assert len(queued_tickets) == runtime_waiting_capacity(8) == 32
    assert sum(ticket is None for ticket in results) == 460
    assert runtime.reserved_count == 8
    assert runtime.waiting_count == 32

    issued = [await ticket.wait() for ticket in issued_tickets]
    await runtime.retire()
    for ticket in queued_tickets:
        with pytest.raises(ServerMcpUnavailableError):
            await ticket.wait()
    for call in issued:
        await call.lease.aclose()


async def test_remote_drain_keeps_all_permits_until_the_lease_is_closed() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    issued = await _issue(runtime, _USER_A, started, "remote")
    queued = await runtime.submit(_USER_B, _start_recorder(started, "after-drain"))

    await issued.lease.mark_draining()

    assert runtime.active_count == 0
    assert runtime.draining_count == 1
    assert runtime.reserved_count == 1
    assert coordinator.snapshot().draining == 1
    assert queued.issued is False

    await issued.lease.aclose()
    after_drain = await queued.wait()
    assert started == ["remote", "after-drain"]
    assert runtime.draining_count == 0
    await after_drain.lease.aclose()


async def test_start_failure_rolls_back_all_counters_and_continues_queue() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=1)

    def fail(_lease: AdmissionLease) -> object:
        raise RuntimeError("start failed")

    failed = await runtime.submit(_USER_A, fail)
    with pytest.raises(RuntimeError, match="start failed"):
        await failed.wait()
    assert runtime.reserved_count == 0
    assert coordinator.snapshot().reserved == 0

    started: list[str] = []
    next_call = await _issue(runtime, _USER_B, started, "next")
    assert started == ["next"]
    await next_call.lease.aclose()


@pytest.mark.parametrize(
    "error",
    [SystemExit(7), FatalStart("fatal start")],
    ids=["system-exit", "custom-base-exception"],
)
async def test_start_base_exception_rolls_back_every_reservation(
    error: BaseException,
) -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=1)

    def fail(_lease: AdmissionLease) -> object:
        raise error

    failed = await runtime.submit(_USER_A, fail)
    with pytest.raises(type(error)) as raised:
        await failed.wait()

    assert raised.value is error
    assert runtime.reserved_count == 0
    assert coordinator.snapshot().reserved == 0

    started: list[str] = []
    next_call = await _issue(runtime, _USER_B, started, "next")
    await next_call.lease.aclose()


async def test_repeated_cancellation_waits_for_lease_release_before_propagating() -> None:
    coordinator = ServerMcpCoordinator(clock=FakeClock())
    runtime = coordinator.create_runtime(max_concurrent_calls=1)
    started: list[str] = []
    issued = await _issue(runtime, _USER_A, started, "issued")

    await coordinator._lock.acquire()
    close_task = asyncio.create_task(issued.lease.aclose())
    try:
        await asyncio.sleep(0)
        close_task.cancel()
        await asyncio.sleep(0)
        close_task.cancel()
        await asyncio.sleep(0)

        assert close_task.done() is False
        assert coordinator.snapshot().reserved == 1
    finally:
        coordinator._lock.release()

    with pytest.raises(asyncio.CancelledError):
        await close_task
    assert runtime.reserved_count == 0
    assert coordinator.snapshot().reserved == 0
