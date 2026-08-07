from __future__ import annotations

import asyncio


async def await_future_cancellation_safe[T](future: asyncio.Future[T]) -> T:
    """Wait for a started future before propagating any caller cancellation."""
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(future)
        except asyncio.CancelledError:
            cancelled = True
            if future.done():
                break
        except BaseException:
            if not cancelled:
                raise
            break
        else:
            if cancelled:
                raise asyncio.CancelledError
            return result

    try:
        future.exception()
    except BaseException:
        pass
    raise asyncio.CancelledError
