from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any

_LOCAL_TRANSFER_CANCEL_GRACE_SECONDS = 0.1


class BlockingWork:
    """Own worker threads until they finish or transfer to a runtime drain."""

    def __init__(self) -> None:
        self.tasks: set[asyncio.Task[Any]] = set()

    def has_pending(self) -> bool:
        return any(not task.done() for task in self.tasks)

    async def wait(self) -> None:
        pending = tuple(task for task in self.tasks if not task.done())
        if pending:
            await asyncio.gather(
                *(asyncio.shield(task) for task in pending), return_exceptions=True
            )

    async def run[T](self, function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        return await _run_blocking(self.tasks, function, *args, **kwargs)

    async def mutate(self, function: Any, *args: Any, **kwargs: Any) -> Any:
        return await _run_mutation(function, *args, tracker=self.tasks, **kwargs)

    async def run_transfer[T](
        self,
        abandoned_drains: set[asyncio.Task[None]],
        function: Callable[..., T],
        *args: Any,
        on_abandoned: Callable[[T], Any] | None = None,
        **kwargs: Any,
    ) -> T:
        return await _run_blocking_with_drain(
            self.tasks,
            abandoned_drains,
            function,
            *args,
            on_abandoned=on_abandoned,
            **kwargs,
        )


def _track_blocking_task(tracker: set[asyncio.Task[Any]], task: asyncio.Task[Any]) -> None:
    tracker.add(task)

    def complete(done: asyncio.Task[Any]) -> None:
        tracker.discard(done)
        # A cancellation may leave the worker task un-awaited.  Consume its
        # exception so a blocked or failed filesystem call cannot produce a
        # "Task exception was never retrieved" warning after shutdown.
        if not done.cancelled():
            with contextlib.suppress(BaseException):
                done.exception()

    task.add_done_callback(complete)


async def _run_blocking[T](
    tracker: set[asyncio.Task[Any]],
    function: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> T:
    """Run local IO/CPU work without letting cancellation hide its thread."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    _track_blocking_task(tracker, task)
    return await asyncio.shield(task)


async def _run_blocking_with_drain[T](
    tracker: set[asyncio.Task[Any]],
    abandoned_drains: set[asyncio.Task[None]],
    function: Callable[..., T],
    *args: Any,
    on_abandoned: Callable[[T], Any] | None = None,
    **kwargs: Any,
) -> T:
    """Transfer blocking work to a runtime drain when its caller is cancelled."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    _track_blocking_task(tracker, task)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # The runtime drain now owns this operation; it must not hold the
        # connection's FIFO tool worker open while the OS syscall finishes.
        tracker.discard(task)
        drain = asyncio.create_task(_drain_blocking_result(task, on_abandoned))
        abandoned_drains.add(drain)

        def finish(completed: asyncio.Task[None]) -> None:
            abandoned_drains.discard(completed)
            if not completed.cancelled():
                with contextlib.suppress(BaseException):
                    completed.exception()

        drain.add_done_callback(finish)
        with contextlib.suppress(BaseException):
            await asyncio.wait({drain}, timeout=_LOCAL_TRANSFER_CANCEL_GRACE_SECONDS)
        raise


async def _drain_blocking_result[T](
    task: asyncio.Task[T], on_abandoned: Callable[[T], Any] | None
) -> None:
    try:
        result = await asyncio.shield(task)
    except BaseException:
        return
    if on_abandoned is None:
        return
    cleanup = asyncio.create_task(asyncio.to_thread(on_abandoned, result))
    try:
        await asyncio.shield(cleanup)
    except BaseException:
        if not cleanup.cancelled():
            with contextlib.suppress(BaseException):
                cleanup.exception()


async def _run_mutation(
    function: Any,
    *args: Any,
    tracker: set[asyncio.Task[Any]] | None = None,
    **kwargs: Any,
) -> Any:
    """Finish a worker-thread mutation before releasing its path lock."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    if tracker is not None:
        _track_blocking_task(tracker, task)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Cancellation of ``to_thread`` only cancels the asyncio wrapper; the
        # underlying filesystem operation keeps running.  Wait for it while
        # the caller still owns PathLocks, then propagate the cancellation.
        with contextlib.suppress(BaseException):
            await asyncio.shield(task)
        raise


async def _run_irreversible_mutation[T](
    tracker: set[asyncio.Task[Any]],
    function: Callable[..., T],
    *args: Any,
) -> T:
    """Return the true result once a no-rollback filesystem operation starts."""

    task = asyncio.create_task(asyncio.to_thread(function, *args))
    _track_blocking_task(tracker, task)
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
