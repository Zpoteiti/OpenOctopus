from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Hashable
from contextlib import asynccontextmanager


class PathLockBusyError(RuntimeError):
    """A non-owner attempted to enter an operation-reserved subtree."""


class PathLocks:
    """Cancellation-safe in-process locks for paths and their subtrees."""

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._locks: dict[str, _LockEntry] = {}
        self._pending: list[_Reservation] = []
        self._active: list[_Reservation] = []

    @property
    def entry_count(self) -> int:
        return len(self._locks)

    @property
    def reservation_count(self) -> int:
        """Number of paths currently held by a tool or transfer."""

        return sum(len(reservation.paths) for reservation in self._active)

    @asynccontextmanager
    async def hold(
        self, *paths: str, owner: Hashable | None = None
    ) -> AsyncIterator[None]:
        async with self._hold(paths, owner=owner, exclusive=False):
            yield

    @asynccontextmanager
    async def reserve_subtree(
        self, owner: Hashable, *paths: str
    ) -> AsyncIterator[None]:
        """Publish an owner-joinable subtree reservation with fast-busy conflicts."""

        async with self._hold(paths, owner=owner, exclusive=True):
            yield

    @asynccontextmanager
    async def _hold(
        self,
        paths: tuple[str, ...],
        *,
        owner: Hashable | None,
        exclusive: bool,
    ) -> AsyncIterator[None]:
        requested = _compact_paths(paths)
        if not requested:
            yield
            return

        reservation = _Reservation(requested, owner=owner, exclusive=exclusive)
        registered = False
        acquired = False
        try:
            async with self._condition:
                for path in requested:
                    entry = self._locks.setdefault(path, _LockEntry())
                    entry.references += 1
                self._pending.append(reservation)
                registered = True

                while self._blocked(reservation):
                    if self._has_exclusive_conflict(reservation):
                        raise PathLockBusyError("Path subtree is reserved by another operation")
                    await self._condition.wait()

                self._pending.remove(reservation)
                self._active.append(reservation)
                acquired = True
                self._condition.notify_all()

            yield
        finally:
            async with self._condition:
                if registered:
                    if acquired:
                        self._active.remove(reservation)
                    else:
                        self._pending.remove(reservation)
                    for path in requested:
                        entry = self._locks[path]
                        entry.references -= 1
                        if entry.references == 0:
                            self._locks.pop(path, None)
                    self._condition.notify_all()

    def _blocked(self, reservation: _Reservation) -> bool:
        if any(
            _reservations_overlap(reservation, active)
            and not _reservations_can_join(reservation, active)
            for active in self._active
        ):
            return True

        index = self._pending.index(reservation)
        return any(
            _reservations_overlap(reservation, earlier)
            and not _reservations_can_join(reservation, earlier)
            for earlier in self._pending[:index]
        )

    def _has_exclusive_conflict(self, reservation: _Reservation) -> bool:
        return any(
            active.exclusive
            and _reservations_overlap(reservation, active)
            and not _reservations_can_join(reservation, active)
            for active in self._active
        )


class _Reservation:
    def __init__(
        self,
        paths: tuple[str, ...],
        *,
        owner: Hashable | None,
        exclusive: bool,
    ) -> None:
        self.paths = paths
        self.owner = owner
        self.exclusive = exclusive


class _LockEntry:
    def __init__(self) -> None:
        self.references = 0


def _compact_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    normalized = sorted(
        {os.path.normcase(os.path.abspath(os.path.normpath(path))) for path in paths},
        key=lambda path: (path.count(os.sep), path),
    )
    compacted: list[str] = []
    for path in normalized:
        if not any(_is_ancestor(parent, path) for parent in compacted):
            compacted.append(path)
    return tuple(compacted)


def _reservations_overlap(first: _Reservation, second: _Reservation) -> bool:
    return any(
        _is_ancestor(first_path, second_path)
        or _is_ancestor(second_path, first_path)
        for first_path in first.paths
        for second_path in second.paths
    )


def _reservations_can_join(first: _Reservation, second: _Reservation) -> bool:
    return (
        first.owner is not None
        and first.owner == second.owner
        and (first.exclusive or second.exclusive)
    )


def _is_ancestor(parent: str, child: str) -> bool:
    try:
        return os.path.commonpath((parent, child)) == parent
    except ValueError:
        return False
