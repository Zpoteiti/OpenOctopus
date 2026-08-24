from __future__ import annotations

import asyncio

import pytest

from openoctopus_client.tools.locks import PathLockBusyError, PathLocks


def test_path_locks_release_unique_path_entries() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        for index in range(200):
            async with locks.hold(f"/workspace/{index}"):
                assert locks.entry_count == 1
        assert locks.entry_count == 0

    asyncio.run(exercise())


def test_path_locks_block_a_child_while_its_parent_is_held() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        parent_released = asyncio.Event()
        child_entered = asyncio.Event()

        async def hold_parent() -> None:
            async with locks.hold("/workspace/project"):
                await parent_released.wait()

        async def hold_child() -> None:
            async with locks.hold("/workspace/project/file.txt"):
                child_entered.set()

        parent_task = asyncio.create_task(hold_parent())
        while locks.entry_count != 1:
            await asyncio.sleep(0)
        child_task = asyncio.create_task(hold_child())
        while locks.entry_count != 2:
            await asyncio.sleep(0)
        assert child_entered.is_set() is False

        parent_released.set()
        await parent_task
        await child_task
        assert child_entered.is_set()
        assert locks.entry_count == 0

    asyncio.run(exercise())


def test_path_locks_block_a_parent_while_its_child_is_held() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        child_released = asyncio.Event()
        parent_entered = asyncio.Event()

        async def hold_child() -> None:
            async with locks.hold("/workspace/project/file.txt"):
                await child_released.wait()

        async def hold_parent() -> None:
            async with locks.hold("/workspace/project"):
                parent_entered.set()

        child_task = asyncio.create_task(hold_child())
        while locks.entry_count != 1:
            await asyncio.sleep(0)
        parent_task = asyncio.create_task(hold_parent())
        while locks.entry_count != 2:
            await asyncio.sleep(0)
        assert parent_entered.is_set() is False

        child_released.set()
        await child_task
        await parent_task
        assert parent_entered.is_set()
        assert locks.entry_count == 0

    asyncio.run(exercise())


def test_path_locks_allow_unrelated_paths_in_parallel() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        unrelated_entered = asyncio.Event()
        release_unrelated = asyncio.Event()

        async def hold_unrelated() -> None:
            async with locks.hold("/workspace/other/file.txt"):
                unrelated_entered.set()
                await release_unrelated.wait()

        task = asyncio.create_task(hold_unrelated())
        while locks.entry_count != 1:
            await asyncio.sleep(0)

        async with locks.hold("/workspace/project"):
            await asyncio.wait_for(unrelated_entered.wait(), timeout=1)

        release_unrelated.set()
        await task
        assert locks.entry_count == 0

    asyncio.run(exercise())


def test_path_locks_cancelled_waiter_does_not_leak_entries() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        release_parent = asyncio.Event()

        async def hold_parent() -> None:
            async with locks.hold("/workspace/project"):
                await release_parent.wait()

        async def wait_for_child() -> None:
            async with locks.hold("/workspace/project/file.txt"):
                raise AssertionError("cancelled waiter acquired the lock")

        parent_task = asyncio.create_task(hold_parent())
        while locks.entry_count != 1:
            await asyncio.sleep(0)
        waiter = asyncio.create_task(wait_for_child())
        while locks.entry_count != 2:
            await asyncio.sleep(0)

        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert locks.entry_count == 1
        assert locks.reservation_count == 1

        release_parent.set()
        await parent_task
        assert locks.entry_count == 0
        assert locks.reservation_count == 0

    asyncio.run(exercise())


def test_path_locks_preserve_fifo_between_ordinary_waiters() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        release_active = asyncio.Event()
        release_first = asyncio.Event()
        entered: list[str] = []

        async def hold_active() -> None:
            async with locks.hold("/workspace/project"):
                await release_active.wait()

        async def wait_for_path(
            label: str, path: str, release: asyncio.Event | None = None
        ) -> None:
            async with locks.hold(path):
                entered.append(label)
                if release is not None:
                    await release.wait()

        active_task = asyncio.create_task(hold_active())
        while locks.entry_count != 1:
            await asyncio.sleep(0)
        first = asyncio.create_task(
            wait_for_path("first", "/workspace/project/child", release_first)
        )
        while locks.entry_count != 2:
            await asyncio.sleep(0)
        second = asyncio.create_task(
            wait_for_path("second", "/workspace/project/child/file.txt")
        )
        while locks.entry_count != 3:
            await asyncio.sleep(0)

        release_active.set()
        await active_task
        while entered != ["first"]:
            await asyncio.sleep(0)
        assert second.done() is False
        release_first.set()
        await first
        await second
        assert entered == ["first", "second"]

    asyncio.run(exercise())


def test_path_locks_owner_can_join_its_subtree_reservation() -> None:
    async def exercise() -> None:
        locks = PathLocks()

        async with locks.reserve_subtree("operation-a", "/workspace/project"):
            async with locks.hold(
                "/workspace/project/child/file.txt", owner="operation-a"
            ):
                assert locks.reservation_count == 2
            async with locks.reserve_subtree(
                "operation-a", "/workspace/project/child"
            ):
                assert locks.reservation_count == 2

        assert locks.entry_count == 0
        assert locks.reservation_count == 0

    asyncio.run(exercise())


def test_path_locks_subtree_reservation_rejects_non_owner_without_waiting() -> None:
    async def exercise() -> None:
        locks = PathLocks()

        async with locks.reserve_subtree("operation-a", "/workspace/project"):
            with pytest.raises(PathLockBusyError):
                async with locks.hold("/workspace/project/file.txt"):
                    raise AssertionError("non-owner acquired a reserved subtree")
            with pytest.raises(PathLockBusyError):
                async with locks.reserve_subtree(
                    "operation-b", "/workspace/project/other"
                ):
                    raise AssertionError("another owner acquired a reserved subtree")
            async with locks.hold("/workspace/unrelated"):
                assert locks.reservation_count == 2

        assert locks.entry_count == 0
        assert locks.reservation_count == 0

    asyncio.run(exercise())


def test_path_locks_waiter_becomes_busy_when_earlier_reservation_activates() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        release_regular = asyncio.Event()
        reservation_entered = asyncio.Event()
        release_reservation = asyncio.Event()

        async def hold_regular() -> None:
            async with locks.hold("/workspace/project/active.txt"):
                await release_regular.wait()

        async def reserve() -> None:
            async with locks.reserve_subtree("operation-a", "/workspace/project"):
                reservation_entered.set()
                await release_reservation.wait()

        async def late_non_owner() -> None:
            with pytest.raises(PathLockBusyError):
                async with locks.hold("/workspace/project/file.txt"):
                    raise AssertionError("late non-owner acquired a reserved subtree")

        regular_task = asyncio.create_task(hold_regular())
        while locks.entry_count != 1:
            await asyncio.sleep(0)
        reservation_task = asyncio.create_task(reserve())
        while locks.entry_count != 2:
            await asyncio.sleep(0)
        late_task = asyncio.create_task(late_non_owner())
        while locks.entry_count != 3:
            await asyncio.sleep(0)

        release_regular.set()
        await regular_task
        await asyncio.wait_for(reservation_entered.wait(), timeout=1)
        await asyncio.wait_for(late_task, timeout=1)
        assert reservation_task.done() is False

        release_reservation.set()
        await reservation_task
        assert locks.entry_count == 0
        assert locks.reservation_count == 0

    asyncio.run(exercise())


def test_path_locks_cancelled_subtree_reservation_does_not_leak() -> None:
    async def exercise() -> None:
        locks = PathLocks()
        release_regular = asyncio.Event()

        async def hold_regular() -> None:
            async with locks.hold("/workspace/project"):
                await release_regular.wait()

        async def reserve() -> None:
            async with locks.reserve_subtree("operation-a", "/workspace/project/child"):
                raise AssertionError("cancelled reservation acquired the subtree")

        regular_task = asyncio.create_task(hold_regular())
        while locks.entry_count != 1:
            await asyncio.sleep(0)
        reservation_task = asyncio.create_task(reserve())
        while locks.entry_count != 2:
            await asyncio.sleep(0)

        reservation_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reservation_task
        assert locks.entry_count == 1
        assert locks.reservation_count == 1

        release_regular.set()
        await regular_task
        assert locks.entry_count == 0
        assert locks.reservation_count == 0

    asyncio.run(exercise())
