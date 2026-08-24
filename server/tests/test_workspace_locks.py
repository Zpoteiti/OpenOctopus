import asyncio

import pytest

from openctopus_server.workspace.locks import (
    KeyedLockManager,
    SubtreeLeaseBusyError,
    SubtreeLeaseManager,
)


async def test_hold_many_sorts_keys_and_avoids_opposite_order_deadlock() -> None:
    locks = KeyedLockManager()
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    order: list[str] = []

    async def first() -> None:
        async with locks.hold_many(("b", "a")):
            order.append("first")
            first_entered.set()
            await release_first.wait()

    async def second() -> None:
        await first_entered.wait()
        async with locks.hold_many(("a", "b")):
            order.append("second")

    tasks = [asyncio.create_task(first()), asyncio.create_task(second())]
    await asyncio.sleep(0)
    if tasks[0].done():
        await tasks[0]
    await asyncio.wait_for(first_entered.wait(), timeout=1)
    await asyncio.sleep(0)
    release_first.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

    assert order == ["first", "second"]
    assert locks.entry_count == 0


async def test_hold_many_deduplicates_keys() -> None:
    locks = KeyedLockManager()

    async with locks.hold_many(("same", "same")):
        assert locks.entry_count == 1

    assert locks.entry_count == 0


async def test_repeated_cancellation_cannot_interrupt_lease_cleanup() -> None:
    locks = KeyedLockManager()
    entered = asyncio.Event()
    release_holder = asyncio.Event()

    async def holder() -> None:
        async with locks.hold("workspace"):
            entered.set()
            await release_holder.wait()

    task = asyncio.create_task(holder())
    await entered.wait()
    await locks._guard.acquire()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)

    try:
        assert not task.done()
    finally:
        locks._guard.release()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert locks.entry_count == 0


@pytest.mark.parametrize(
    ("held_prefix", "waiting_prefix"),
    [
        (("archive",), ("archive", "reports")),
        (("archive", "reports"), ("archive",)),
        ((), ("archive",)),
    ],
)
async def test_subtree_lease_serializes_ancestor_and_descendant(
    held_prefix: tuple[str, ...],
    waiting_prefix: tuple[str, ...],
) -> None:
    leases = SubtreeLeaseManager()
    ancestor = await leases.acquire(
        target="workspace",
        prefix=held_prefix,
        owner="ancestor-operation",
    )
    acquired_descendant = asyncio.Event()

    async def acquire_descendant() -> None:
        descendant = await leases.acquire(
            target="workspace",
            prefix=waiting_prefix,
            owner="descendant-operation",
        )
        acquired_descendant.set()
        await descendant.release()

    task = asyncio.create_task(acquire_descendant())
    await asyncio.sleep(0)

    assert not acquired_descendant.is_set()
    assert leases.active_count == 1
    assert leases.pending_count == 1

    await ancestor.release()
    await asyncio.wait_for(task, timeout=1)

    assert acquired_descendant.is_set()
    assert leases.active_count == 0
    assert leases.pending_count == 0


@pytest.mark.parametrize(
    "prefix",
    [("",), (".",), ("..",), ("archive/reports",), ("archive\x00reports",)],
)
async def test_subtree_lease_rejects_noncanonical_prefixes(
    prefix: tuple[str, ...],
) -> None:
    leases = SubtreeLeaseManager()

    with pytest.raises(ValueError, match="canonical"):
        await leases.acquire(target="workspace", prefix=prefix, owner="operation")

    assert leases.active_count == 0
    assert leases.pending_count == 0


async def test_subtree_lease_allows_siblings_and_distinct_targets_in_parallel() -> None:
    leases = SubtreeLeaseManager()
    first = await leases.acquire(
        target="workspace-a",
        prefix=("archive", "one"),
        owner="first",
    )
    sibling = await asyncio.wait_for(
        leases.acquire(
            target="workspace-a",
            prefix=("archive", "two"),
            owner="second",
        ),
        timeout=1,
    )
    other_target = await asyncio.wait_for(
        leases.acquire(
            target="workspace-b",
            prefix=("archive", "one"),
            owner="third",
        ),
        timeout=1,
    )

    assert leases.active_count == 3

    await first.release()
    await sibling.release()
    await other_target.release()
    assert leases.active_count == 0


async def test_subtree_lease_owner_can_join_while_non_owner_waits() -> None:
    leases = SubtreeLeaseManager()
    owner_root = await leases.acquire(
        target="workspace",
        prefix=("destination",),
        owner="directory-operation",
    )

    non_owner_task = asyncio.create_task(
        leases.acquire(
            target="workspace",
            prefix=("destination", "child.txt"),
            owner="ordinary-write",
        )
    )
    await asyncio.sleep(0)
    assert leases.pending_count == 1

    owner_child = await asyncio.wait_for(
        leases.acquire(
            target="workspace",
            prefix=("destination", "child.txt"),
            owner="directory-operation",
        ),
        timeout=1,
    )
    assert not non_owner_task.done()

    await owner_child.release()
    await owner_root.release()
    non_owner = await asyncio.wait_for(non_owner_task, timeout=1)
    await non_owner.release()

    assert leases.active_count == 0
    assert leases.pending_count == 0


async def test_subtree_lease_can_fail_fast_without_leaving_pending_state() -> None:
    leases = SubtreeLeaseManager()
    holder = await leases.acquire(
        target="workspace",
        prefix=("destination",),
        owner="directory-operation",
    )

    with pytest.raises(SubtreeLeaseBusyError):
        await leases.acquire(
            target="workspace",
            prefix=("destination", "child.txt"),
            owner="ordinary-write",
            wait=False,
        )

    assert leases.active_count == 1
    assert leases.pending_count == 0
    await holder.release()


async def test_cancelled_subtree_waiter_leaves_no_ghost_reservation() -> None:
    leases = SubtreeLeaseManager()
    holder = await leases.acquire(
        target="workspace",
        prefix=("destination",),
        owner="directory-operation",
    )
    waiter = asyncio.create_task(
        leases.acquire(
            target="workspace",
            prefix=("destination", "child.txt"),
            owner="ordinary-write",
        )
    )
    await asyncio.sleep(0)
    assert leases.pending_count == 1

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert leases.active_count == 1
    assert leases.pending_count == 0

    await holder.release()
    replacement = await asyncio.wait_for(
        leases.acquire(
            target="workspace",
            prefix=("destination", "child.txt"),
            owner="ordinary-write",
        ),
        timeout=1,
    )
    await replacement.release()


async def test_subtree_lease_release_is_idempotent() -> None:
    leases = SubtreeLeaseManager()
    lease = await leases.acquire(
        target="workspace",
        prefix=("destination",),
        owner="directory-operation",
    )

    await lease.release()
    await lease.release()

    assert leases.active_count == 0
    assert leases.pending_count == 0


async def test_cancellation_cannot_interrupt_subtree_lease_release() -> None:
    leases = SubtreeLeaseManager()
    lease = await leases.acquire(
        target="workspace",
        prefix=("destination",),
        owner="directory-operation",
    )
    await leases._condition.acquire()
    release_task = asyncio.create_task(lease.release())
    await asyncio.sleep(0)
    release_task.cancel()
    await asyncio.sleep(0)
    release_task.cancel()
    await asyncio.sleep(0)

    try:
        assert not release_task.done()
    finally:
        leases._condition.release()

    with pytest.raises(asyncio.CancelledError):
        await release_task
    assert leases.active_count == 0

    await lease.release()
