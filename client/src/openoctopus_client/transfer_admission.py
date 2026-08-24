from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import Iterable
from typing import Any

LOCAL_TRANSFER_CAPACITY = 2


class LocalTransferLease:
    """Idempotent ownership of one client-local transfer slot."""

    def __init__(self, admission: LocalTransferAdmission) -> None:
        self._admission = admission
        self._owned = True

    def handoff(self) -> LocalTransferLease:
        """Move the slot to a drain owner without returning capacity."""

        if not self._owned:
            raise RuntimeError("Local transfer lease is no longer owned")
        self._owned = False
        return LocalTransferLease(self._admission)

    def release(self) -> None:
        if not self._owned:
            return
        self._owned = False
        self._admission._release()


class LocalTransferAdmission:
    """FIFO, runtime-owned admission shared by all client transfer work."""

    def __init__(self, *, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("Local transfer admission capacity must be positive")
        self._capacity = capacity
        self._available = capacity
        self._waiters: deque[asyncio.Future[LocalTransferLease]] = deque()

    @property
    def active_count(self) -> int:
        return self._capacity - self._available

    @property
    def waiting_count(self) -> int:
        self._discard_finished_waiters()
        return len(self._waiters)

    def try_acquire(self) -> LocalTransferLease | None:
        """Acquire immediately without bypassing an already queued waiter."""

        self._discard_finished_waiters()
        if self._available == 0 or self._waiters:
            return None
        self._available -= 1
        return LocalTransferLease(self)

    async def acquire(
        self, *, timeout_seconds: float | None = None
    ) -> LocalTransferLease:
        """Wait in FIFO order for capacity, optionally with a bounded timeout."""

        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("Local transfer admission timeout must be non-negative")
        immediate = self.try_acquire()
        if immediate is not None:
            return immediate

        future: asyncio.Future[LocalTransferLease] = (
            asyncio.get_running_loop().create_future()
        )
        self._waiters.append(future)
        try:
            if timeout_seconds is None:
                return await future
            async with asyncio.timeout(timeout_seconds):
                return await future
        except BaseException:
            if future.done() and not future.cancelled():
                future.result().release()
            else:
                future.cancel()
            raise
        finally:
            with contextlib.suppress(ValueError):
                self._waiters.remove(future)

    def _release(self) -> None:
        self._discard_finished_waiters()
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.done():
                continue
            waiter.set_result(LocalTransferLease(self))
            return
        if self._available >= self._capacity:
            raise RuntimeError("Local transfer admission released excess capacity")
        self._available += 1

    def _discard_finished_waiters(self) -> None:
        if not self._waiters:
            return
        self._waiters = deque(waiter for waiter in self._waiters if not waiter.done())


class LocalTransferDrainRegistry:
    """Runtime-owned strong references for work that outlives its caller."""

    def __init__(self) -> None:
        self._owners: dict[asyncio.Task[Any], object] = {}

    @property
    def pending_count(self) -> int:
        self._purge_done()
        return len(self._owners)

    def adopt(
        self,
        lease: LocalTransferLease,
        drains: Iterable[asyncio.Future[Any]],
        *,
        owner: object,
    ) -> None:
        """Keep a handed-off lease until every abandoned drain is complete."""

        pending = tuple(drains)
        if not pending:
            lease.release()
            return
        drain_lease = lease.handoff()

        async def finish() -> None:
            try:
                await asyncio.gather(
                    *(asyncio.shield(task) for task in pending),
                    return_exceptions=True,
                )
            finally:
                drain_lease.release()

        self.retain(asyncio.create_task(finish()), owner=owner)

    def retain(self, task: asyncio.Task[Any], *, owner: object) -> None:
        """Retain an unfinished lifecycle task without taking ownership of cancel."""

        self._owners[task] = owner
        task.add_done_callback(self._finish)

    async def wait(self, *, timeout_seconds: float | None = None) -> bool:
        """Wait without cancelling retained work; return whether it quiesced."""

        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("Local transfer drain timeout must be non-negative")
        loop = asyncio.get_running_loop()
        deadline = (
            None if timeout_seconds is None else loop.time() + timeout_seconds
        )
        while True:
            self._purge_done()
            pending = {task for task in self._owners if not task.done()}
            if not pending:
                return True
            timeout = None if deadline is None else max(0.0, deadline - loop.time())
            if timeout == 0:
                return False
            _, unfinished = await asyncio.wait(pending, timeout=timeout)
            if unfinished and deadline is not None and loop.time() >= deadline:
                return False

    def _finish(self, task: asyncio.Task[Any]) -> None:
        self._owners.pop(task, None)
        if not task.cancelled():
            with contextlib.suppress(BaseException):
                task.exception()

    def _purge_done(self) -> None:
        for task in tuple(self._owners):
            if task.done():
                self._finish(task)
