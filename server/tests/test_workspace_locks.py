import asyncio

from openctopus_server.workspace.locks import KeyedLockManager


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
