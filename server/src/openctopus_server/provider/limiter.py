import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class ProviderLimiter:
    def __init__(self) -> None:
        self._limit = 0
        self._in_flight = 0
        self._condition = asyncio.Condition()

    async def configure(self, limit: int) -> None:
        async with self._condition:
            self._limit = limit
            self._condition.notify_all()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        async with self._condition:
            await self._condition.wait_for(
                lambda: self._limit == 0 or self._in_flight < self._limit
            )
            self._in_flight += 1
        try:
            yield
        finally:
            async with self._condition:
                self._in_flight -= 1
                self._condition.notify_all()
