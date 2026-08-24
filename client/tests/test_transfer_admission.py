from __future__ import annotations

import asyncio

import pytest

from openoctopus_client.transfer_admission import (
    LocalTransferAdmission,
    LocalTransferDrainRegistry,
)


def test_local_transfer_admission_rejects_invalid_capacity() -> None:
    with pytest.raises(ValueError, match="positive"):
        LocalTransferAdmission(capacity=0)


def test_local_transfer_admission_bounds_and_idempotently_releases() -> None:
    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=2)
        first = admission.try_acquire()
        second = admission.try_acquire()

        assert first is not None
        assert second is not None
        assert admission.active_count == 2
        assert admission.try_acquire() is None

        first.release()
        first.release()
        assert admission.active_count == 1

        replacement = admission.try_acquire()
        assert replacement is not None
        assert admission.active_count == 2
        second.release()
        replacement.release()
        assert admission.active_count == 0

    asyncio.run(exercise())


def test_local_transfer_admission_waiters_are_fifo_and_not_bypassed() -> None:
    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=2)
        first = admission.try_acquire()
        second = admission.try_acquire()
        assert first is not None and second is not None

        first_waiter = asyncio.create_task(admission.acquire())
        second_waiter = asyncio.create_task(admission.acquire())
        while admission.waiting_count != 2:
            await asyncio.sleep(0)

        assert admission.try_acquire() is None
        first.release()
        first_waiter_lease = await asyncio.wait_for(first_waiter, timeout=1)
        assert second_waiter.done() is False

        second.release()
        second_waiter_lease = await asyncio.wait_for(second_waiter, timeout=1)
        first_waiter_lease.release()
        second_waiter_lease.release()
        assert admission.active_count == 0
        assert admission.waiting_count == 0

    asyncio.run(exercise())


def test_local_transfer_admission_cancellation_and_timeout_do_not_leak() -> None:
    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=1)
        active = admission.try_acquire()
        assert active is not None

        cancelled = asyncio.create_task(admission.acquire())
        while admission.waiting_count != 1:
            await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        assert admission.waiting_count == 0

        with pytest.raises(TimeoutError):
            await admission.acquire(timeout_seconds=0.01)
        assert admission.waiting_count == 0
        assert admission.active_count == 1

        active.release()
        replacement = admission.try_acquire()
        assert replacement is not None
        replacement.release()
        assert admission.active_count == 0

    asyncio.run(exercise())


def test_local_transfer_admission_cancel_after_assignment_returns_capacity() -> None:
    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=1)
        active = admission.try_acquire()
        assert active is not None
        waiter = asyncio.create_task(admission.acquire())
        while admission.waiting_count != 1:
            await asyncio.sleep(0)

        active.release()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert admission.active_count == 0

        replacement = admission.try_acquire()
        assert replacement is not None
        replacement.release()

    asyncio.run(exercise())


def test_local_transfer_lease_handoff_keeps_capacity_with_drain_owner() -> None:
    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=1)
        original = admission.try_acquire()
        assert original is not None

        drain_owner = original.handoff()
        original.release()
        assert admission.active_count == 1

        waiter = asyncio.create_task(admission.acquire())
        while admission.waiting_count != 1:
            await asyncio.sleep(0)
        drain_owner.release()
        admitted = await asyncio.wait_for(waiter, timeout=1)
        admitted.release()
        assert admission.active_count == 0

        with pytest.raises(RuntimeError, match="owned"):
            drain_owner.handoff()

    asyncio.run(exercise())


def test_local_transfer_drain_releases_only_after_work_and_cleanup_finish() -> None:
    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=1)
        registry = LocalTransferDrainRegistry()
        lease = admission.try_acquire()
        assert lease is not None
        work_release = asyncio.Event()
        cleanup_release = asyncio.Event()

        async def wait_for(event: asyncio.Event) -> None:
            await event.wait()

        work = asyncio.create_task(wait_for(work_release))
        cleanup = asyncio.create_task(wait_for(cleanup_release))
        registry.adopt(lease, (work, cleanup), owner=object())

        assert registry.pending_count == 1
        assert admission.active_count == 1
        assert admission.try_acquire() is None

        work_release.set()
        await work
        await asyncio.sleep(0)
        assert admission.active_count == 1

        cleanup_release.set()
        assert await registry.wait(timeout_seconds=1)
        assert registry.pending_count == 0
        assert admission.active_count == 0

    asyncio.run(exercise())


def test_local_transfer_drain_retains_unfinished_owner_task_without_cancelling() -> None:
    async def exercise() -> None:
        registry = LocalTransferDrainRegistry()
        release = asyncio.Event()

        async def blocked() -> None:
            await release.wait()

        task = asyncio.create_task(blocked())
        registry.retain(task, owner=object())

        assert not await registry.wait(timeout_seconds=0.01)
        assert task.cancelled() is False
        assert registry.pending_count == 1

        release.set()
        assert await registry.wait(timeout_seconds=1)
        assert registry.pending_count == 0

    asyncio.run(exercise())
