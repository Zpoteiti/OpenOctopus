from __future__ import annotations

import asyncio

import pytest

from openoctopus_client.tools.locks import PathLocks


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
