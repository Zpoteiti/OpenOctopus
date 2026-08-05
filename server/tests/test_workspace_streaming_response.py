import pytest
from starlette.requests import ClientDisconnect

from openctopus_server.api.workspace_files import _ClosingStreamingResponse


async def test_stream_is_closed_when_client_disconnects_before_body_iteration() -> None:
    closed = False
    body_started = False

    async def body():
        nonlocal body_started
        body_started = True
        yield b"data"

    async def close() -> None:
        nonlocal closed
        closed = True

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        del message
        raise OSError("client disconnected")

    response = _ClosingStreamingResponse(
        body(),
        closer=close,
        media_type="application/octet-stream",
    )
    with pytest.raises(ClientDisconnect):
        await response(
            {"type": "http", "asgi": {"spec_version": "2.4"}},
            receive,
            send,
        )

    assert body_started is False
    assert closed is True
