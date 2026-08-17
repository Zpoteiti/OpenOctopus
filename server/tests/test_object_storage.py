from __future__ import annotations

import asyncio
import queue
from io import BytesIO
from threading import Event, Lock
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest
import urllib3
from minio.error import ServerError
from storage_http import object_storage_for_fake

from openctopus_server.config import Settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.storage import (
    STARTUP_PROBE_KEY,
    TRANSFER_TEMP_PREFIX,
    ObjectStorage,
    ObjectUpload,
    _PresignedGetTransport,
    _QueueReader,
    build_object_storage,
)


class _Response:
    def __init__(self, data: bytes, *, etag: str | None = "response-etag"):
        self._data = data
        self.headers = {} if etag is None else {"ETag": f'"{etag}"'}
        self.closed = False
        self.released = False

    def read(self, size: int = -1) -> bytes:
        return self._data if size < 0 else self._data[:size]

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://u:p@localhost/db",
        "database_pool_size": 5,
        "database_max_overflow": 10,
        "database_pool_timeout": 30,
        "database_pool_pre_ping": True,
        "host": "127.0.0.1",
        "port": 8080,
        "jwt_secret": "secret",
        "cookie_secure": False,
        "admin_token": "admin",
        "object_storage_endpoint": "https://rustfs.internal:9000",
        "object_storage_bucket": "openoctopus",
        "object_storage_region": "us-east-1",
        "object_storage_access_key": "key",
        "object_storage_secret_key": "secret",
        "object_storage_max_connections": 17,
        "rest_upload_max_concurrency": 4,
        "rest_download_max_concurrency": 8,
        "rest_transfer_max_concurrency_per_user": 2,
        "rest_transfer_queue_timeout_seconds": 5,
        "rest_transfer_idle_timeout_seconds": 30,
        "content_conversion_memory_mb": 1024,
        "content_conversion_timeout_seconds": 20,
        "content_conversion_max_concurrency": 2,
        "content_conversion_queue_timeout_seconds": 5,
        "web_fetch_max_concurrency": 16,
        "web_fetch_max_concurrency_per_user": 2,
        "web_fetch_queue_timeout_seconds": 5,
        "chat_context_max_concurrency": 32,
        "chat_context_max_concurrency_per_user": 2,
        "chat_context_queue_timeout_seconds": 30,
        "device_pending_calls_max": 4096,
        "device_pending_calls_max_per_user": 64,
        "device_pending_bytes_max": 256 * 1024 * 1024,
        "device_pending_bytes_max_per_user": 32 * 1024 * 1024,
        "device_transfer_max_concurrency": 32,
        "device_transfer_max_concurrency_per_user": 2,
        "device_transfer_queue_timeout_seconds": 5,
        "device_transfer_idle_timeout_seconds": 30,
        "workspace_deletion_purge_timeout_seconds": 300,
        "workspace_deletion_shutdown_grace_seconds": 5,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[arg-type]


def test_build_object_storage_uses_independent_bounded_health_pool() -> None:
    pool = Mock(name="workspace_pool")
    health_pool = Mock(name="health_pool")
    client = Mock(name="workspace_client")
    health_client = Mock(name="health_client")
    async_client = Mock()
    async_transport = Mock()

    with (
        patch(
            "openctopus_server.workspace.storage.urllib3.PoolManager",
            side_effect=[pool, health_pool],
        ) as make_pool,
        patch(
            "openctopus_server.workspace.storage.Minio",
            side_effect=[client, health_client],
        ) as make_client,
        patch(
            "openctopus_server.workspace.storage.httpx.AsyncClient",
            return_value=async_client,
        ) as make_async_client,
        patch(
            "openctopus_server.workspace.storage.httpx.AsyncHTTPTransport",
            return_value=async_transport,
        ) as make_async_transport,
    ):
        storage = build_object_storage(_settings())

    assert make_pool.call_count == 2
    pool_kwargs = make_pool.call_args_list[0].kwargs
    assert pool_kwargs["num_pools"] == 1
    assert pool_kwargs["maxsize"] == 17
    assert pool_kwargs["block"] is True
    assert pool_kwargs["timeout"].connect_timeout == 5
    assert pool_kwargs["timeout"].read_timeout == 30
    assert pool_kwargs["retries"].total == 2
    health_pool_kwargs = make_pool.call_args_list[1].kwargs
    assert health_pool_kwargs["num_pools"] == 1
    assert health_pool_kwargs["maxsize"] == 1
    assert health_pool_kwargs["block"] is True
    assert health_pool_kwargs["timeout"].connect_timeout == 5
    assert health_pool_kwargs["timeout"].read_timeout == 30

    assert make_client.call_args_list[0] == (
        (),
        {
            "endpoint": "rustfs.internal:9000",
            "access_key": "key",
            "secret_key": "secret",
            "secure": True,
            "region": "us-east-1",
            "http_client": pool,
        },
    )
    assert make_client.call_args_list[1] == (
        (),
        {
            "endpoint": "rustfs.internal:9000",
            "access_key": "key",
            "secret_key": "secret",
            "secure": True,
            "region": "us-east-1",
            "http_client": health_pool,
        },
    )
    assert storage.client is client
    assert storage.max_connections == 17
    async_kwargs = make_async_client.call_args.kwargs
    assert async_kwargs["trust_env"] is False
    assert async_kwargs["follow_redirects"] is False
    assert isinstance(async_kwargs["transport"], _PresignedGetTransport)
    assert make_async_transport.call_args.kwargs["limits"].max_connections == 17


@pytest.mark.parametrize(
    "endpoint",
    ["rustfs.internal:9000", "ftp://rustfs.internal", "https://rustfs.internal/path"],
)
def test_build_object_storage_rejects_invalid_endpoint(endpoint: str) -> None:
    with pytest.raises(ValueError, match="object storage endpoint"):
        build_object_storage(_settings(object_storage_endpoint=endpoint))


async def test_startup_probe_verifies_exact_bytes_and_cleans_up() -> None:
    client = Mock()
    response = _Response(b"probe-data")
    client.bucket_exists.return_value = True
    client.put_object.return_value = SimpleNamespace(etag="probe-etag")
    client.stat_object.return_value = SimpleNamespace(size=10, etag="probe-etag")
    client.get_object.return_value = response
    storage = object_storage_for_fake(client, "openoctopus", max_connections=2)

    with patch(
        "openctopus_server.workspace.storage.secrets.token_bytes", return_value=b"probe-data"
    ):
        await storage.probe_startup()

    client.put_object.assert_called_once()
    put_args = client.put_object.call_args.args
    assert put_args[:2] == ("openoctopus", STARTUP_PROBE_KEY)
    assert isinstance(put_args[2], BytesIO)
    assert put_args[2].getvalue() == b"probe-data"
    assert put_args[3] == 10
    client.stat_object.assert_called_once_with("openoctopus", STARTUP_PROBE_KEY)
    client.get_object.assert_called_once_with(
        "openoctopus",
        STARTUP_PROBE_KEY,
        offset=0,
        length=0,
    )
    client.remove_object.assert_called_once_with("openoctopus", STARTUP_PROBE_KEY)
    assert response.closed
    assert response.released


async def test_startup_probe_rejects_missing_bucket_without_creating_it() -> None:
    client = Mock()
    client.bucket_exists.return_value = False
    storage = object_storage_for_fake(client, "openoctopus", max_connections=2)

    with pytest.raises(WorkspaceError) as exc_info:
        await storage.probe_startup()

    assert exc_info.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    client.make_bucket.assert_not_called()
    client.put_object.assert_not_called()


async def test_startup_probe_attempts_cleanup_after_read_failure() -> None:
    client = Mock()
    client.bucket_exists.return_value = True
    client.put_object.return_value = SimpleNamespace(etag="probe-etag")
    client.stat_object.return_value = SimpleNamespace(size=32, etag="probe-etag")
    client.get_object.side_effect = OSError("connection lost")
    storage = object_storage_for_fake(client, "openoctopus", max_connections=2)

    with pytest.raises(WorkspaceError) as exc_info:
        await storage.probe_startup()

    assert exc_info.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    client.remove_object.assert_called_once_with("openoctopus", STARTUP_PROBE_KEY)


async def test_startup_recovery_removes_only_transfer_temporaries() -> None:
    client = Mock()
    client.list_objects.return_value = [
        SimpleNamespace(
            object_name=f"{TRANSFER_TEMP_PREFIX}one",
            size=3,
            etag="one",
        ),
        SimpleNamespace(
            object_name=f"{TRANSFER_TEMP_PREFIX}two",
            size=4,
            etag="two",
        ),
    ]
    storage = object_storage_for_fake(client, "openoctopus", max_connections=2)

    removed = await storage.recover_transfer_uploads()

    assert removed == 2
    client.list_objects.assert_called_once_with(
        "openoctopus",
        prefix=TRANSFER_TEMP_PREFIX,
        recursive=True,
        start_after=None,
    )
    assert client.remove_object.call_args_list == [
        (("openoctopus", f"{TRANSFER_TEMP_PREFIX}one"),),
        (("openoctopus", f"{TRANSFER_TEMP_PREFIX}two"),),
    ]


async def test_health_check_is_non_mutating() -> None:
    client = Mock()
    client.bucket_exists.return_value = True
    storage = ObjectStorage(client, "openoctopus", max_connections=2)

    await storage.check_health()

    client.bucket_exists.assert_called_once_with("openoctopus")
    client.put_object.assert_not_called()
    client.remove_object.assert_not_called()


async def test_cancelled_health_probe_does_not_consume_workspace_capacity() -> None:
    started = Event()
    release = Event()
    health_client = Mock()

    def blocking_bucket_exists(bucket: str) -> bool:
        assert bucket == "openoctopus"
        started.set()
        release.wait()
        return True

    health_client.bucket_exists.side_effect = blocking_bucket_exists
    storage = ObjectStorage(
        Mock(),
        "openoctopus",
        max_connections=1,
        health_client=health_client,
    )
    health_task = asyncio.create_task(storage.check_health())
    try:
        assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)
        health_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await health_task

        result = await asyncio.wait_for(storage.execute(lambda: "available"), timeout=0.5)

        assert result == "available"
    finally:
        release.set()
        await storage.close()


async def test_concurrent_health_checks_share_one_probe() -> None:
    started = Event()
    release = Event()
    calls_lock = Lock()
    calls = 0
    health_client = Mock()

    def blocking_bucket_exists(bucket: str) -> bool:
        nonlocal calls
        assert bucket == "openoctopus"
        with calls_lock:
            calls += 1
        started.set()
        release.wait()
        return True

    health_client.bucket_exists.side_effect = blocking_bucket_exists
    storage = ObjectStorage(
        Mock(),
        "openoctopus",
        max_connections=1,
        health_client=health_client,
    )
    checks = [
        asyncio.create_task(storage.check_health()),
        asyncio.create_task(storage.check_health()),
    ]
    try:
        assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)
        await asyncio.sleep(0)
        assert calls == 1
    finally:
        release.set()
        await asyncio.gather(*checks)
        await storage.close()
    assert calls == 1


async def test_close_waits_for_detached_health_probe_and_clears_pool() -> None:
    started = Event()
    release = Event()
    health_client = Mock()
    main_pool = Mock()
    health_pool = Mock()

    def blocking_bucket_exists(bucket: str) -> bool:
        assert bucket == "openoctopus"
        started.set()
        release.wait()
        return True

    health_client.bucket_exists.side_effect = blocking_bucket_exists
    storage = ObjectStorage(
        Mock(),
        "openoctopus",
        max_connections=1,
        http_client=main_pool,
        health_client=health_client,
        health_http_client=health_pool,
    )
    health_task = asyncio.create_task(storage.check_health())
    assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)
    health_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await health_task

    close_task = asyncio.create_task(storage.close())
    await asyncio.sleep(0)
    assert not close_task.done()
    close_task.cancel()
    await asyncio.sleep(0)
    close_task.cancel()
    await asyncio.sleep(0)
    assert not close_task.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(close_task, timeout=1)
    await storage.close()
    main_pool.clear.assert_called_once_with()
    health_pool.clear.assert_called_once_with()


async def test_write_disables_minio_parallel_uploads() -> None:
    client = Mock()
    client.put_object.return_value = SimpleNamespace(etag="revision-1")
    storage = ObjectStorage(client, "openoctopus", max_connections=1)

    await storage.write("file.bin", b"data")

    client.put_object.assert_called_once()
    assert client.put_object.call_args.kwargs["num_parallel_uploads"] == 1


async def test_stream_upload_writes_bounded_chunks_and_finishes() -> None:
    client = Mock()
    uploaded = bytearray()

    def put_object(
        bucket: str,
        object_name: str,
        stream: Any,
        length: int,
        **kwargs: Any,
    ) -> SimpleNamespace:
        assert bucket == "openoctopus"
        assert object_name == "_temporary/upload"
        assert length == -1
        assert kwargs["num_parallel_uploads"] == 1
        while chunk := stream.read(5 * 1024 * 1024):
            uploaded.extend(chunk)
        return SimpleNamespace(etag="stream-etag")

    client.put_object.side_effect = put_object
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    upload = storage.begin_upload("_temporary/upload")

    await upload.write(b"first")
    await upload.write(b"second")
    metadata = await upload.finish()

    assert bytes(uploaded) == b"firstsecond"
    assert metadata.size == 11
    assert metadata.etag == "stream-etag"
    await storage.close()


async def test_stream_upload_does_not_block_when_storage_worker_has_failed() -> None:
    client = Mock()
    client.put_object.side_effect = OSError("synthetic upload failure")
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    upload = storage.begin_upload("_temporary/upload")
    for _ in range(100):
        if upload._worker.done():
            break
        await asyncio.sleep(0.001)
    assert upload._worker.done()

    with pytest.raises(WorkspaceError):
        await asyncio.wait_for(upload.write(b"data"), timeout=0.2)

    await upload.abort()
    await storage.close()


async def test_stream_upload_finish_cleans_up_after_worker_failure() -> None:
    client = Mock()
    client.put_object.side_effect = OSError("synthetic upload failure")
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    upload = storage.begin_upload("_temporary/upload", length=4)
    for _ in range(100):
        if upload._worker.done():
            break
        await asyncio.sleep(0.001)

    with pytest.raises(WorkspaceError):
        await upload.finish()

    client.remove_object.assert_called_once_with("openoctopus", "_temporary/upload")
    await storage.close()


async def test_stream_upload_checks_declared_length_when_worker_finishes_early() -> None:
    client = Mock()

    def put_object(
        _bucket: str,
        _object_name: str,
        stream: Any,
        _length: int,
        **_kwargs: Any,
    ) -> SimpleNamespace:
        assert stream.read(4) == b"abc"
        return SimpleNamespace(etag="stream-etag")

    client.put_object.side_effect = put_object
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    upload = storage.begin_upload("_temporary/upload", length=4)

    await upload.write(b"abc")
    for _ in range(100):
        if upload._worker.done():
            break
        await asyncio.sleep(0.001)
    with pytest.raises(WorkspaceError) as caught:
        await upload.finish()

    assert caught.value.code is ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE
    client.remove_object.assert_called_once_with("openoctopus", "_temporary/upload")
    await storage.close()


def test_queue_reader_never_returns_more_than_requested() -> None:
    values: queue.Queue[bytes | object] = queue.Queue()
    values.put(b"abcdef")
    reader = _QueueReader(values)

    assert reader.read(2) == b"ab"
    assert reader.read(2) == b"cd"
    assert reader.read(8) == b"ef"


def test_queue_reader_default_read_consumes_until_end() -> None:
    values: queue.Queue[bytes | object] = queue.Queue()
    values.put(b"ab")
    values.put(b"cd")
    values.put(ObjectUpload._END)
    reader = _QueueReader(values)

    assert reader.read() == b"abcd"


async def test_stream_upload_rejects_declared_length_overrun_before_queueing() -> None:
    client = Mock()
    client.put_object.return_value = SimpleNamespace(etag="stream-etag")
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    upload = storage.begin_upload("_temporary/upload", length=3)

    await upload.write(b"abc")
    with pytest.raises(WorkspaceError) as caught:
        await upload.write(b"d")

    assert caught.value.code is ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE
    assert upload.written == 3
    await upload.abort()
    await storage.close()


async def test_cancelled_upload_abort_cleans_up_and_propagates_cancellation() -> None:
    started = Event()
    release = Event()
    client = Mock()

    def put_object(_bucket: str, _name: str, _stream: Any, _length: int, **_kwargs: Any):
        started.set()
        release.wait()
        return SimpleNamespace(etag="stream-etag")

    client.put_object.side_effect = put_object
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    upload = storage.begin_upload("_temporary/upload")
    assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)

    abort_task = asyncio.create_task(upload.abort())
    await asyncio.sleep(0.01)
    abort_task.cancel()
    await asyncio.sleep(0.01)
    assert not abort_task.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await abort_task
    client.remove_object.assert_called_once_with("openoctopus", "_temporary/upload")
    await storage.close()


async def test_storage_close_aborts_orphaned_stream_upload() -> None:
    started = Event()
    client = Mock()

    def put_object(_bucket: str, _name: str, stream: Any, _length: int, **_kwargs: Any):
        started.set()
        while stream.read(5):
            pass
        return SimpleNamespace(etag="stream-etag")

    client.put_object.side_effect = put_object
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    storage.begin_upload("_temporary/orphan")
    assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)

    await asyncio.wait_for(storage.close(), timeout=1)
    client.remove_object.assert_called_once_with("openoctopus", "_temporary/orphan")


async def test_storage_close_rejects_new_stream_uploads() -> None:
    storage = ObjectStorage(Mock(), "openoctopus", max_connections=1)
    await storage.close()

    with pytest.raises(WorkspaceError) as caught:
        storage.begin_upload("_temporary/late")

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE


@pytest.mark.parametrize(
    "failure",
    [
        urllib3.exceptions.MaxRetryError(None, "internal/key/secret.txt"),
        urllib3.exceptions.ReadTimeoutError(None, "request", "internal/key/secret.txt"),
        urllib3.exceptions.ProtocolError("internal/key/secret.txt"),
    ],
)
async def test_transport_failures_are_storage_unavailable(failure: Exception) -> None:
    storage = ObjectStorage(Mock(), "openoctopus", max_connections=1)

    with pytest.raises(WorkspaceError) as caught:
        await storage.execute(Mock(side_effect=failure))

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    assert "internal/key/secret.txt" not in str(caught.value)


@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
async def test_retry_exhausted_server_errors_are_storage_unavailable(
    status_code: int,
) -> None:
    storage = ObjectStorage(Mock(), "openoctopus", max_connections=1)

    with pytest.raises(WorkspaceError) as caught:
        await storage.execute(Mock(side_effect=ServerError("internal/key/secret.txt", status_code)))

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE


@pytest.mark.parametrize("operation", ["stat", "list_page", "read", "write"])
async def test_malformed_storage_metadata_is_normalized(operation: str) -> None:
    client = Mock()
    client.stat_object.return_value = SimpleNamespace()
    client.list_objects.return_value = [SimpleNamespace(object_name="key")]
    client.get_object.return_value = _Response(b"data", etag=None)
    client.put_object.return_value = SimpleNamespace()
    storage = (
        object_storage_for_fake(client, "openoctopus", max_connections=1)
        if operation == "read"
        else ObjectStorage(client, "openoctopus", max_connections=1)
    )

    with pytest.raises(WorkspaceError) as caught:
        if operation == "stat":
            await storage.stat("key")
        elif operation == "list_page":
            await storage.list_page("prefix")
        elif operation == "read":
            await storage.read("key", max_bytes=4)
        else:
            await storage.write("key", b"data")

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_ERROR
