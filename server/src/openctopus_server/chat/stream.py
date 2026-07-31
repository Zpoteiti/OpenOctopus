import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class StreamSubscriber:
    message_id: UUID
    accepted_at: datetime
    queue: asyncio.Queue[dict[str, Any] | None] = field(default_factory=asyncio.Queue)
    closed: bool = False

    def send(self, event: dict[str, Any]) -> None:
        if not self.closed:
            self.queue.put_nowait(event)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.queue.put_nowait(None)

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
