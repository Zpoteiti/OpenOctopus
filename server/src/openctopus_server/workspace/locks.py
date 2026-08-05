from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Hashable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field


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
            async with self._guard:
                entry.leases -= 1
                if entry.leases == 0 and not entry.lock.locked():
                    self._entries.pop(key, None)
