import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

_STREAM_QUEUE_MAX_EVENTS = 256


@dataclass(slots=True)
class StreamSubscriber:
    message_id: UUID
    accepted_at: datetime
    queue: asyncio.Queue[dict[str, Any] | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=_STREAM_QUEUE_MAX_EVENTS)
    )
    closed: bool = False

    def send(self, event: dict[str, Any]) -> None:
        if self.closed:
            return
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self._disconnect_slow_consumer()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            self._discard_pending()
            self.queue.put_nowait(None)

    def _disconnect_slow_consumer(self) -> None:
        self.closed = True
        self._discard_pending()
        self.queue.put_nowait(None)

    def _discard_pending(self) -> None:
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    async def ndjson(self) -> AsyncIterator[bytes]:
        while True:
            event = await self.queue.get()
            if event is None:
                return
            yield (
                json.dumps(
                    event,
                    separators=(",", ":"),
                    default=str,
                )
                + "\n"
            ).encode()
