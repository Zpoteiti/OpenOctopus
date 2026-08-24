from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Hashable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field


class AdmissionTimeoutError(TimeoutError):
    """A bounded admission queue did not produce capacity in time."""


class AdmissionLease:
    """Transferable ownership of acquired admission capacity."""

    def __init__(self, release: Callable[[], None]) -> None:
        self._release = release
        self._closed = False

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._release()


@dataclass(slots=True)
class _Entry:
    capacity: int
    semaphore: asyncio.Semaphore = field(init=False)
    leases: int = 0

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(self.capacity)


class _KeyedPool:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._entries: dict[Hashable, _Entry] = {}

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    def lease(self, key: Hashable) -> _Entry:
        entry = self._entries.get(key)
        if entry is None:
            entry = _Entry(self._capacity)
            self._entries[key] = entry
        entry.leases += 1
        return entry

    def release(self, key: Hashable, entry: _Entry) -> None:
        entry.leases -= 1
        if entry.leases == 0 and self._entries.get(key) is entry:
            self._entries.pop(key, None)


class KeyedAdmission:
    """Bounded process admission with a keyed limit acquired before the global one."""

    def __init__(
        self,
        *,
        global_limit: int,
        per_key_limit: int,
        timeout_seconds: float,
    ) -> None:
        if global_limit < 1 or per_key_limit < 1:
            raise ValueError("admission limits must be positive")
        if per_key_limit > global_limit:
            raise ValueError("per-key admission limit cannot exceed the global limit")
        if timeout_seconds <= 0:
            raise ValueError("admission timeout must be positive")
        self._global = asyncio.Semaphore(global_limit)
        self._timeout_seconds = timeout_seconds
        self._keyed = _KeyedPool(per_key_limit)

    @property
    def entry_count(self) -> int:
        return self._keyed.entry_count

    @asynccontextmanager
    async def slot(self, key: Hashable) -> AsyncIterator[None]:
        lease = await self.acquire(key)
        try:
            yield
        finally:
            await lease.aclose()

    async def acquire(self, key: Hashable) -> AdmissionLease:
        """Acquire capacity whose ownership may outlive the current stack frame."""
        entry = self._keyed.lease(key)
        keyed_acquired = False
        global_acquired = False
        transferred = False
        try:
            try:
                async with asyncio.timeout(self._timeout_seconds):
                    await entry.semaphore.acquire()
                    keyed_acquired = True
                    await self._global.acquire()
                    global_acquired = True
            except TimeoutError as exc:
                raise AdmissionTimeoutError from exc
            transferred = True
            return AdmissionLease(lambda: self._release_capacity(key, entry))
        finally:
            if global_acquired and not transferred:
                self._global.release()
            if keyed_acquired and not transferred:
                entry.semaphore.release()
            if not transferred:
                self._keyed.release(key, entry)

    def _release_capacity(self, key: Hashable, entry: _Entry) -> None:
        self._global.release()
        entry.semaphore.release()
        self._keyed.release(key, entry)


class KeyedDirectionalAdmission:
    """A shared keyed limit in front of independent global direction limits."""

    def __init__(
        self,
        *,
        direction_limits: dict[str, int],
        per_key_limit: int,
        timeout_seconds: float,
    ) -> None:
        if not direction_limits or any(limit < 1 for limit in direction_limits.values()):
            raise ValueError("direction admission limits must be positive")
        if per_key_limit < 1:
            raise ValueError("per-key admission limit must be positive")
        if timeout_seconds <= 0:
            raise ValueError("admission timeout must be positive")
        self._directions = {
            name: asyncio.Semaphore(limit) for name, limit in direction_limits.items()
        }
        self._keyed = _KeyedPool(per_key_limit)
        self._timeout_seconds = timeout_seconds

    @property
    def entry_count(self) -> int:
        return self._keyed.entry_count

    @asynccontextmanager
    async def slot(self, key: Hashable, direction: str) -> AsyncIterator[None]:
        lease = await self.acquire(key, direction)
        try:
            yield
        finally:
            await lease.aclose()

    async def acquire(self, key: Hashable, direction: str) -> AdmissionLease:
        global_semaphore = self._directions.get(direction)
        if global_semaphore is None:
            raise ValueError(f"unknown admission direction: {direction}")
        entry = self._keyed.lease(key)
        keyed_acquired = False
        global_acquired = False
        transferred = False
        try:
            try:
                async with asyncio.timeout(self._timeout_seconds):
                    await entry.semaphore.acquire()
                    keyed_acquired = True
                    await global_semaphore.acquire()
                    global_acquired = True
            except TimeoutError as exc:
                raise AdmissionTimeoutError from exc
            transferred = True
            return AdmissionLease(lambda: self._release_capacity(key, entry, global_semaphore))
        finally:
            if global_acquired and not transferred:
                global_semaphore.release()
            if keyed_acquired and not transferred:
                entry.semaphore.release()
            if not transferred:
                self._keyed.release(key, entry)

    def _release_capacity(
        self,
        key: Hashable,
        entry: _Entry,
        global_semaphore: asyncio.Semaphore,
    ) -> None:
        global_semaphore.release()
        entry.semaphore.release()
        self._keyed.release(key, entry)
