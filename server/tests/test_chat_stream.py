from datetime import UTC, datetime
from uuid import uuid4

from openctopus_server.chat.stream import _STREAM_QUEUE_MAX_EVENTS, StreamSubscriber


async def test_close_preserves_events_for_a_healthy_subscriber() -> None:
    subscriber = StreamSubscriber(message_id=uuid4(), accepted_at=datetime.now(UTC))
    subscriber.send({"type": "token_delta", "text": "hello"})

    subscriber.close()

    chunks = [chunk async for chunk in subscriber.ndjson()]
    assert chunks == [b'{"type":"token_delta","text":"hello"}\n']


async def test_queue_overflow_disconnects_slow_subscriber() -> None:
    subscriber = StreamSubscriber(message_id=uuid4(), accepted_at=datetime.now(UTC))
    for index in range(_STREAM_QUEUE_MAX_EVENTS):
        subscriber.send({"type": "token_delta", "text": str(index)})

    subscriber.send({"type": "token_delta", "text": "overflow"})

    assert subscriber.closed is True
    assert subscriber.queue.qsize() == 1
    assert [chunk async for chunk in subscriber.ndjson()] == []
