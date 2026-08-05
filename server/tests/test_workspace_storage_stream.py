from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable

import pytest
import pytest_asyncio

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.storage import STREAM_CHUNK_SIZE, ObjectStorage


@pytest_asyncio.fixture(autouse=True)
async def _truncate_tables() -> AsyncIterator[None]:
    """These storage unit tests do not need PostgreSQL."""
    yield


class _StreamingResponse:
    def __init__(
        self,
        content: bytes,
        *,
        read_hook: Callable[[], None] | None = None,
        read_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self._content = content
        self._offset = 0
        self._read_hook = read_hook
        self._read_error = read_error
        self._close_error = close_error
        self.headers = {
            "Content-Length": str(len(content)),
            "ETag": '"revision-1"',
        }
        self.read_sizes: list[int] = []
        self.read_thread_ids: list[int] = []
        self.close_calls = 0
        self.release_calls = 0

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        self.read_thread_ids.append(threading.get_ident())
        if self._read_hook is not None:
            self._read_hook()
        if self._read_error is not None:
            raise self._read_error
        chunk = self._content[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def close(self) -> None:
        self.close_calls += 1
        if self._close_error is not None:
            raise self._close_error

    def release_conn(self) -> None:
        self.release_calls += 1


class _StreamingClient:
    def __init__(self, responses: list[_StreamingResponse | Exception]) -> None:
        self._responses = responses
        self.get_thread_ids: list[int] = []

    def get_object(self, bucket: str, object_name: str) -> _StreamingResponse:
        del bucket, object_name
        self.get_thread_ids.append(threading.get_ident())
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


async def test_stream_reads_bounded_chunks_and_exposes_metadata() -> None:
    content = b"x" * (STREAM_CHUNK_SIZE + 7)
    response = _StreamingResponse(content)
    client = _StreamingClient([response])
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    event_loop_thread = threading.get_ident()
    try:
        stream = await storage.open_stream("large.bin")
        assert stream.size == len(content)
        assert stream.etag == "revision-1"

        assert await stream.read() == content[:STREAM_CHUNK_SIZE]
        assert await stream.read() == content[STREAM_CHUNK_SIZE:]
        assert await stream.read() == b""
        await stream.aclose()
    finally:
        await storage.close()

    assert response.read_sizes == [STREAM_CHUNK_SIZE] * 3
    assert client.get_thread_ids[0] != event_loop_thread
    assert all(thread_id != event_loop_thread for thread_id in response.read_thread_ids)
    assert response.close_calls == 1
    assert response.release_calls == 1


async def test_stream_holds_capacity_until_idempotent_close() -> None:
    first_response = _StreamingResponse(b"first")
    second_response = _StreamingResponse(b"second")
    storage = ObjectStorage(
        _StreamingClient([first_response, second_response]),
        "openoctopus",
        max_connections=1,
    )
    try:
        first = await storage.open_stream("first.bin")
        second_task = asyncio.create_task(storage.open_stream("second.bin"))
        await asyncio.sleep(0)
        assert not second_task.done()

        await first.aclose()
        await first.aclose()
        second = await asyncio.wait_for(second_task, timeout=1)
        await second.aclose()
    finally:
        await storage.close()

    assert first_response.close_calls == 1
    assert first_response.release_calls == 1
    assert second_response.close_calls == 1
    assert second_response.release_calls == 1


async def test_open_error_releases_capacity_and_is_normalized() -> None:
    response = _StreamingResponse(b"ok")
    storage = ObjectStorage(
        _StreamingClient([OSError("connection lost"), response]),
        "openoctopus",
        max_connections=1,
    )
    try:
        with pytest.raises(WorkspaceError) as caught:
            await storage.open_stream("broken.bin")
        stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await stream.aclose()
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE


async def test_close_error_still_releases_response_and_capacity() -> None:
    failed_response = _StreamingResponse(b"data", close_error=OSError("close failed"))
    next_response = _StreamingResponse(b"ok")
    storage = ObjectStorage(
        _StreamingClient([failed_response, next_response]),
        "openoctopus",
        max_connections=1,
    )
    try:
        failed_stream = await storage.open_stream("failed.bin")
        with pytest.raises(WorkspaceError) as caught:
            await failed_stream.aclose()
        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    assert failed_response.close_calls == 1
    assert failed_response.release_calls == 1


async def test_read_error_closes_response_and_releases_capacity() -> None:
    failed_response = _StreamingResponse(b"", read_error=OSError("connection lost"))
    next_response = _StreamingResponse(b"ok")
    storage = ObjectStorage(
        _StreamingClient([failed_response, next_response]),
        "openoctopus",
        max_connections=1,
    )
    try:
        failed_stream = await storage.open_stream("broken.bin")
        with pytest.raises(WorkspaceError) as caught:
            await failed_stream.read()
        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    assert failed_response.close_calls == 1
    assert failed_response.release_calls == 1


async def test_cancelled_open_closes_late_response_and_releases_capacity() -> None:
    get_started = threading.Event()
    allow_get = threading.Event()
    late_response = _StreamingResponse(b"late")
    next_response = _StreamingResponse(b"ok")

    class _BlockingClient(_StreamingClient):
        def get_object(self, bucket: str, object_name: str) -> _StreamingResponse:
            if not get_started.is_set():
                get_started.set()
                assert allow_get.wait(timeout=1)
            return super().get_object(bucket, object_name)

    storage = ObjectStorage(
        _BlockingClient([late_response, next_response]),
        "openoctopus",
        max_connections=1,
    )
    try:
        open_task = asyncio.create_task(storage.open_stream("late.bin"))
        assert await asyncio.to_thread(get_started.wait, 1)
        open_task.cancel()
        allow_get.set()
        with pytest.raises(asyncio.CancelledError):
            await open_task

        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert late_response.close_calls == 1
    assert late_response.release_calls == 1


async def test_cancelled_read_closes_response_and_releases_capacity() -> None:
    read_started = threading.Event()
    allow_read = threading.Event()

    def block_read() -> None:
        read_started.set()
        assert allow_read.wait(timeout=1)

    blocked_response = _StreamingResponse(b"blocked", read_hook=block_read)
    next_response = _StreamingResponse(b"ok")
    storage = ObjectStorage(
        _StreamingClient([blocked_response, next_response]),
        "openoctopus",
        max_connections=1,
    )
    try:
        stream = await storage.open_stream("blocked.bin")
        read_task = asyncio.create_task(stream.read())
        assert await asyncio.to_thread(read_started.wait, 1)
        read_task.cancel()
        allow_read.set()
        with pytest.raises(asyncio.CancelledError):
            await read_task

        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert blocked_response.close_calls == 1
    assert blocked_response.release_calls == 1


async def test_malformed_stream_headers_close_response_and_release_capacity() -> None:
    malformed = _StreamingResponse(b"data")
    malformed.headers = {"Content-Length": "not-an-integer", "ETag": '"revision-1"'}
    next_response = _StreamingResponse(b"ok")
    storage = ObjectStorage(
        _StreamingClient([malformed, next_response]),
        "openoctopus",
        max_connections=1,
    )
    try:
        with pytest.raises(WorkspaceError) as caught:
            await storage.open_stream("malformed.bin")
        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_ERROR
    assert malformed.close_calls == 1
    assert malformed.release_calls == 1
