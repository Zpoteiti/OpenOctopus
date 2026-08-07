from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest
import pytest_asyncio
from minio import Minio

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.storage import (
    STREAM_CHUNK_SIZE,
    ObjectStorage,
    _PresignedGetTransport,
)


@pytest_asyncio.fixture(autouse=True)
async def _no_database_cleanup() -> AsyncIterator[None]:
    """These storage unit tests do not need PostgreSQL."""
    yield


class _Presigner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, timedelta]] = []

    def presigned_get_object(
        self,
        bucket: str,
        object_name: str,
        *,
        expires: timedelta,
    ) -> str:
        self.calls.append((bucket, object_name, expires))
        return f"https://rustfs.test/{object_name}?X-Amz-Signature=top-secret"


class _TrackingStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _BlockingStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.closed = asyncio.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.started.set()
        await asyncio.Event().wait()
        yield b"unreachable"

    async def aclose(self) -> None:
        self.closed.set()


class _CloseFailStream(_TrackingStream):
    async def aclose(self) -> None:
        self.closed = True
        raise OSError("close failed")


class _ReadFailStream(_TrackingStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        request = httpx.Request(
            "GET",
            "https://rustfs.test/file.bin?X-Amz-Signature=read-secret",
        )
        raise httpx.ReadError("read failed", request=request)
        yield b"unreachable"


class _BlockingCloseStream(_TrackingStream):
    def __init__(self, chunks: list[bytes]) -> None:
        super().__init__(chunks)
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()

    async def aclose(self) -> None:
        self.close_started.set()
        await self.allow_close.wait()
        self.closed = True


class _RecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.request: httpx.Request | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.request = request
        if self.fail:
            raise httpx.ConnectError("connect failed", request=request)
        return _response(b"ok")


def _response(
    content: bytes | None = None,
    *,
    stream: httpx.AsyncByteStream | None = None,
    headers: dict[str, str] | None = None,
    status: int = 200,
) -> httpx.Response:
    body = content or b""
    response_stream = stream if stream is not None else _TrackingStream([body])
    return httpx.Response(
        status,
        stream=response_stream,
        headers=headers
        or {
            "Content-Length": str(len(body)),
            "ETag": '"revision-1"',
        },
    )


def _storage(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    max_connections: int = 1,
) -> tuple[ObjectStorage, _Presigner]:
    presigner = _Presigner()
    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return (
        ObjectStorage(
            presigner,
            "openoctopus",
            max_connections=max_connections,
            async_client=async_client,
        ),
        presigner,
    )


async def test_stream_reads_bounded_chunks_and_exposes_metadata() -> None:
    content = b"x" * (STREAM_CHUNK_SIZE + 7)
    tracking = _TrackingStream([content])
    storage, presigner = _storage(
        lambda request: _response(
            stream=tracking,
            headers={"Content-Length": str(len(content)), "ETag": '"revision-1"'},
        )
    )
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

    assert tracking.closed is True
    assert presigner.calls[0][:2] == ("openoctopus", "large.bin")


async def test_stream_holds_capacity_until_idempotent_close() -> None:
    responses = iter((_response(b"first"), _response(b"second")))
    storage, _ = _storage(lambda request: next(responses))
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


async def test_async_stream_and_sync_operations_share_one_capacity_budget() -> None:
    storage, _ = _storage(lambda request: _response(b"stream"))
    sync_started = threading.Event()

    def sync_operation() -> str:
        sync_started.set()
        return "done"

    try:
        stream = await storage.open_stream("stream.bin")
        sync_task = asyncio.create_task(storage.execute(sync_operation))
        await asyncio.sleep(0)
        assert not sync_started.is_set()

        await stream.aclose()
        assert await asyncio.wait_for(sync_task, timeout=1) == "done"
    finally:
        await storage.close()


async def test_cancelled_read_closes_socket_and_releases_capacity() -> None:
    blocked = _BlockingStream()
    responses = iter(
        (
            _response(
                stream=blocked,
                headers={"Content-Length": "1", "ETag": '"blocked"'},
            ),
            _response(b"ok"),
        )
    )
    storage, _ = _storage(lambda request: next(responses))
    try:
        first = await storage.open_stream("blocked.bin")
        read_task = asyncio.create_task(first.read())
        await asyncio.wait_for(blocked.started.wait(), timeout=1)
        read_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read_task
        await asyncio.wait_for(blocked.closed.wait(), timeout=1)

        second = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await second.aclose()
    finally:
        await storage.close()


async def test_read_error_is_normalized_without_presigned_url_context() -> None:
    failed = _ReadFailStream([])
    responses = iter((_response(stream=failed), _response(b"ok")))
    storage, _ = _storage(lambda request: next(responses))
    try:
        stream = await storage.open_stream("broken.bin")
        with pytest.raises(WorkspaceError) as caught:
            await stream.read()
        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "read-secret" not in repr(caught.value)


async def test_cancelled_close_waits_for_socket_before_releasing_capacity() -> None:
    closing = _BlockingCloseStream([b"data"])
    responses = iter((_response(stream=closing), _response(b"ok")))
    storage, _ = _storage(lambda request: next(responses))
    try:
        stream = await storage.open_stream("closing.bin")
        close_task = asyncio.create_task(stream.aclose())
        await asyncio.wait_for(closing.close_started.wait(), timeout=1)
        close_task.cancel()
        next_task = asyncio.create_task(storage.open_stream("ok.bin"))
        await asyncio.sleep(0)

        assert not close_task.done()
        assert not next_task.done()
        assert closing.closed is False

        closing.allow_close.set()
        with pytest.raises(asyncio.CancelledError):
            await close_task
        assert closing.closed is True
        next_stream = await asyncio.wait_for(next_task, timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()


async def test_cancelled_error_response_cleanup_waits_before_releasing_capacity() -> None:
    closing = _BlockingCloseStream([b"error"])
    responses = iter((_response(stream=closing, status=500), _response(b"ok")))
    storage, _ = _storage(lambda request: next(responses))
    try:
        open_task = asyncio.create_task(storage.open_stream("failed.bin"))
        await asyncio.wait_for(closing.close_started.wait(), timeout=1)
        open_task.cancel()
        next_task = asyncio.create_task(storage.open_stream("ok.bin"))
        await asyncio.sleep(0)

        assert not open_task.done()
        assert not next_task.done()
        assert closing.closed is False

        closing.allow_close.set()
        with pytest.raises(asyncio.CancelledError):
            await open_task
        assert closing.closed is True
        next_stream = await asyncio.wait_for(next_task, timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()


async def test_open_error_retries_releases_capacity_and_redacts_url(caplog) -> None:
    attempts = 0
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        requests.append(request)
        if attempts <= 3:
            raise httpx.ConnectError("failed", request=request)
        return _response(b"ok")

    storage, _ = _storage(handler)
    caplog.set_level(logging.INFO)
    try:
        with pytest.raises(WorkspaceError) as caught:
            await storage.open_stream("broken.bin")
        stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await stream.aclose()
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "top-secret" not in repr(caught.value)
    assert all("openoctopus.presigned_url" not in request.extensions for request in requests)
    assert all("top-secret" not in repr(request.extensions) for request in requests)
    assert "X-Amz-" not in caplog.text
    assert "top-secret" not in caplog.text
    assert attempts == 4


@pytest.mark.parametrize("fail", [False, True])
async def test_presigned_transport_scrubs_signed_url_from_public_state(
    monkeypatch: pytest.MonkeyPatch,
    fail: bool,
) -> None:
    inner = _RecordingTransport(fail=fail)
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", lambda **kwargs: inner)
    transport = _PresignedGetTransport(max_connections=1)
    outer_request = httpx.Request(
        "GET",
        "https://rustfs.test/file.bin",
        extensions={
            "openoctopus.presigned_url": (
                "https://rustfs.test/file.bin?X-Amz-Signature=transport-secret"
            )
        },
    )

    if fail:
        with pytest.raises(httpx.RequestError) as caught:
            await transport.handle_async_request(outer_request)
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        assert "transport-secret" not in repr(caught.value)
    else:
        response = await transport.handle_async_request(outer_request)
        assert response.request is outer_request
        assert "transport-secret" not in repr(response.request)

    assert "openoctopus.presigned_url" not in outer_request.extensions
    assert "transport-secret" not in repr(outer_request.extensions)
    assert inner.request is not None
    assert "transport-secret" in str(inner.request.url)


async def test_malformed_stream_headers_close_response_and_release_capacity() -> None:
    malformed = _TrackingStream([b"data"])
    responses = iter(
        (
            _response(
                stream=malformed,
                headers={"Content-Length": "not-an-integer", "ETag": '"revision-1"'},
            ),
            _response(b"ok"),
        )
    )
    storage, _ = _storage(lambda request: next(responses))
    try:
        with pytest.raises(WorkspaceError) as caught:
            await storage.open_stream("malformed.bin")
        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_ERROR
    assert malformed.closed is True


async def test_close_failure_during_open_error_does_not_leak_capacity() -> None:
    malformed = _CloseFailStream([b"data"])
    responses = iter(
        (
            _response(
                stream=malformed,
                headers={"Content-Length": "invalid", "ETag": '"revision-1"'},
            ),
            _response(b"ok"),
        )
    )
    storage, _ = _storage(lambda request: next(responses))
    try:
        with pytest.raises(WorkspaceError):
            await storage.open_stream("malformed.bin")
        next_stream = await asyncio.wait_for(storage.open_stream("ok.bin"), timeout=1)
        await next_stream.aclose()
    finally:
        await storage.close()

    assert malformed.closed is True


async def test_redirect_is_rejected_without_following_signed_url() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(307, headers={"Location": "https://attacker.test/steal"})

    storage, _ = _storage(handler)
    try:
        with pytest.raises(WorkspaceError) as caught:
            await storage.open_stream("file.bin")
    finally:
        await storage.close()

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_ERROR
    assert len(requests) == 1


def test_fixed_region_presigning_performs_no_network_io() -> None:
    client = Minio(
        "rustfs.test",
        access_key="access",
        secret_key="secret",
        secure=True,
        region="us-east-1",
    )

    def fail_network(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("presigning must not perform network I/O")

    client._url_open = fail_network  # type: ignore[method-assign]

    url = client.presigned_get_object(
        "openoctopus",
        "file.bin",
        expires=timedelta(minutes=5),
    )

    assert "X-Amz-Signature=" in url
