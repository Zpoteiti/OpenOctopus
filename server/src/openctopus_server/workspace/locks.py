from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Hashable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field

from openctopus_server.async_utils import await_future_cancellation_safe


@dataclass
class _LockEntry:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    leases: int = 0


class KeyedLockManager:
    """Lease-counted keyed locks whose idle entries are safe to evict."""

    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._entries: dict[Hashable, _LockEntry] = {}

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    @asynccontextmanager
    async def hold(self, key: Hashable) -> AsyncIterator[None]:
        async with self._guard:
            entry = self._entries.get(key)
            if entry is None:
                entry = _LockEntry()
                self._entries[key] = entry
            entry.leases += 1

        try:
            async with entry.lock:
                yield
        finally:
            cleanup = asyncio.create_task(self._release(key, entry))
            await await_future_cancellation_safe(cleanup)

    async def _release(self, key: Hashable, entry: _LockEntry) -> None:
        async with self._guard:
            entry.leases -= 1
            if entry.leases == 0 and not entry.lock.locked():
                self._entries.pop(key, None)

    @asynccontextmanager
    async def hold_many(self, keys: tuple[Hashable, ...]) -> AsyncIterator[None]:
        ordered = sorted(set(keys), key=lambda key: (type(key).__qualname__, repr(key)))
        async with AsyncExitStack() as stack:
            for key in ordered:
                await stack.enter_async_context(self.hold(key))
            yield
