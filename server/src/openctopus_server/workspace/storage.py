from __future__ import annotations

import asyncio
import logging
import queue
import secrets
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache, partial
from io import BytesIO
from threading import Event
from typing import Any, TypeVar
from urllib.parse import urlsplit

import httpx
import urllib3
from minio import Minio
from minio.commonconfig import CopySource

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.config import Settings, get_settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError

STARTUP_PROBE_KEY = "_openoctopus/startup-probe"
TRANSFER_TEMP_PREFIX = "_openoctopus-transfers/"
_PROBE_BYTES = 32
MAX_LIST_PAGE_SIZE = 1000
STREAM_CHUNK_SIZE = 64 * 1024
UPLOAD_QUEUE_CHUNKS = 4
_PRESIGNED_GET_LIFETIME = timedelta(minutes=5)
_ASYNC_RETRY_STATUSES = frozenset({500, 502, 503, 504})
_ASYNC_REQUEST_ATTEMPTS = 3
_SIGNED_URL_EXTENSION = "openoctopus.presigned_url"
_T = TypeVar("_T")
logger = logging.getLogger(__name__)


class _PresignedGetTransport(httpx.AsyncBaseTransport):
    """Send a signed URL while exposing only its query-free form to HTTPX logs."""

    def __init__(self, *, max_connections: int) -> None:
        self._transport = httpx.AsyncHTTPTransport(
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            )
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        signed_url = request.extensions.pop(_SIGNED_URL_EXTENSION, None)
        if not isinstance(signed_url, str):
            raise httpx.RequestError("Object storage request was not signed", request=request)
        extensions = dict(request.extensions)
        signed_request = httpx.Request(
            request.method,
            signed_url,
            headers=request.headers,
            extensions=extensions,
        )
        try:
            response = await self._transport.handle_async_request(signed_request)
        except httpx.RequestError:
            response = None
        if response is None:
            del signed_url, signed_request
            raise httpx.RequestError("Object storage request failed", request=request) from None
        response.request = request
        return response

    async def aclose(self) -> None:
        await self._transport.aclose()


@dataclass(frozen=True)
class StoredObject:
    data: bytes
    etag: str
    truncated: bool


@dataclass(frozen=True)
class ObjectMetadata:
    object_name: str
    size: int
    etag: str
    modified: datetime | None = None


@dataclass(frozen=True)
class ObjectPage:
    items: tuple[ObjectMetadata, ...]
    next_start_after: str | None


@dataclass(frozen=True)
class DirectoryObject:
    object_name: str
    size: int | None
    is_directory: bool


@dataclass(frozen=True)
class DirectoryObjectPage:
    items: tuple[DirectoryObject, ...]
    next_start_after: str | None


class ObjectStream:
    """One bounded object response that owns a storage slot until closed."""

    def __init__(
        self,
        storage: ObjectStorage,
        response: Any,
        *,
        size: int,
        etag: str,
    ) -> None:
        self.size = size
        self.etag = etag
        self._storage = storage
        self._response = response
        self._chunks = response.aiter_raw(chunk_size=STREAM_CHUNK_SIZE)
        self._closed = False
        self._lock = asyncio.Lock()

    async def read(self) -> bytes:
        failure: WorkspaceError | None = None
        try:
            async with self._lock:
                if self._closed:
                    return b""
                try:
                    chunk = await anext(self._chunks)
                except StopAsyncIteration:
                    chunk = b""
                if not isinstance(chunk, bytes):
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_STORAGE_ERROR,
                        "Object storage request failed",
                    )
                if not chunk:
                    await self._close_locked()
                return chunk
        except asyncio.CancelledError:
            close_task = asyncio.create_task(self.aclose())
            await _wait_for_worker(close_task)
            raise
        except Exception as exc:
            failure = normalize_storage_error(exc)
            failure.__cause__ = None
            failure.__context__ = None
            failure.__traceback__ = None
            try:
                await self.aclose()
            except WorkspaceError:
                pass
        if failure is not None:
            raise failure from None

        raise AssertionError("unreachable object stream read state")

    async def aclose(self) -> None:
        async with self._lock:
            await self._close_locked()

    async def _close_locked(self) -> None:
        if self._closed:
            return
        self._closed = True
        failure: WorkspaceError | None = None
        try:
            await _close_async_response(self._response)
        except Exception as exc:
            failure = normalize_storage_error(exc)
            failure.__cause__ = None
            failure.__context__ = None
            failure.__traceback__ = None
        finally:
            self._storage._semaphore.release()
        if failure is not None:
            raise failure from None


class ObjectUpload:
    """A bounded, RustFS-backed upload sink.

    MinIO's synchronous multipart writer runs in the storage executor and
    consumes a four-chunk queue.  The server never stages upload bytes on its
    local filesystem or retains the complete object in memory.
    """

    _END = object()

    def __init__(
        self,
        storage: ObjectStorage,
        object_name: str,
        *,
        length: int | None,
    ) -> None:
        self._storage = storage
        self.object_name = object_name
        self.length = length
        self._queue: queue.Queue[bytes | object] = queue.Queue(maxsize=UPLOAD_QUEUE_CHUNKS)
        self._written = 0
        self._closed = False
        self._end_enqueued = False
        self._cancelled = Event()
        self._worker = asyncio.create_task(self._upload())

    @property
    def written(self) -> int:
        return self._written

    async def write(self, chunk: bytes) -> None:
        if self._closed:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_ERROR,
                "Object upload is already closed",
            )
        if not isinstance(chunk, bytes) or len(chunk) > STREAM_CHUNK_SIZE:
            raise ValueError("object upload chunks must be at most 64 KiB")
        if not chunk:
            return
        if self.length is not None and self._written + len(chunk) > self.length:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
                "Object upload length exceeded its declaration",
            )
        try:
            await self._enqueue(chunk)
        except asyncio.CancelledError:
            self._closed = True
            self._cancelled.set()
            raise
        self._written += len(chunk)

    async def finish(self) -> ObjectMetadata:
        if self._closed:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_ERROR,
                "Object upload is already closed",
            )
        self._closed = True
        try:
            if not self._worker.done():
                await self._enqueue(self._END)
                self._end_enqueued = True
            metadata = await _await_upload_worker(self._worker)
        except asyncio.CancelledError:
            self._cancelled.set()
            raise
        except BaseException:
            self._cancelled.set()
            if self.length is None or self._written == self.length:
                await self._delete_safely()
                raise
            await self._delete_for_length_mismatch()
            raise self._length_mismatch_error() from None
        if self.length is not None and self._written != self.length:
            await self._delete_for_length_mismatch()
            raise self._length_mismatch_error()
        return metadata

    async def abort(self) -> None:
        self._closed = True
        self._cancelled.set()
        worker = self._worker
        cancelled = False
        try:
            await _await_upload_worker(worker)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            pass
        await self._delete_safely()
        if cancelled:
            raise asyncio.CancelledError

    async def _delete_safely(self) -> None:
        try:
            await self._storage.delete(self.object_name)
        except Exception:
            pass

    async def _delete_for_length_mismatch(self) -> None:
        try:
            await self._storage.delete(self.object_name)
        except WorkspaceError as exc:
            if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                raise

    def _length_mismatch_error(self) -> WorkspaceError:
        return WorkspaceError(
            ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
            "Object upload length did not match its declaration",
        )

    async def _enqueue(self, value: bytes | object) -> None:
        while True:
            if self._worker.done():
                await _await_upload_worker(self._worker)
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_STORAGE_ERROR,
                    "Object upload completed before accepting all bytes",
                )
            try:
                self._queue.put_nowait(value)
                return
            except queue.Full:
                done, _ = await asyncio.wait((self._worker,), timeout=0.01)
                if done:
                    await _await_upload_worker(self._worker)
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_STORAGE_ERROR,
                        "Object upload completed before accepting all bytes",
                    )

    async def _upload(self) -> ObjectMetadata:
        def put() -> ObjectMetadata:
            result = self._storage.client.put_object(
                self._storage.bucket,
                self.object_name,
                _QueueReader(self._queue, self._cancelled),
                self.length if self.length is not None else -1,
                part_size=5 * 1024 * 1024,
                num_parallel_uploads=1,
            )
            etag = getattr(result, "etag", None)
            if not isinstance(etag, str) or not etag:
                raise ValueError("object upload response is missing an ETag")
            return ObjectMetadata(
                object_name=self.object_name,
                size=self._written,
                etag=etag,
                modified=None,
            )

        return await self._storage.execute(put)


class _QueueReader:
    def __init__(
        self,
        values: queue.Queue[bytes | object],
        cancelled: Event | None = None,
    ) -> None:
        self._values = values
        self._cancelled = cancelled if cancelled is not None else Event()
        self._ended = False
        self._pending = b""

    def read(self, size: int = -1) -> bytes:
        if not isinstance(size, int):
            raise TypeError("object upload read size must be an integer")
        if self._ended or size == 0:
            return b""
        if self._cancelled.is_set():
            self._ended = True
            return b""
        if size < 0:
            chunks = bytearray()
            if self._pending:
                chunks.extend(self._pending)
                self._pending = b""
            while True:
                value = self._next_value()
                if value is ObjectUpload._END:
                    self._ended = True
                    return bytes(chunks)
                if not isinstance(value, bytes):
                    raise ValueError("object upload queue contained an invalid chunk")
                chunks.extend(value)

        bounded_value: bytes | object = self._pending
        self._pending = b""
        if not bounded_value:
            bounded_value = self._next_value()
        if bounded_value is ObjectUpload._END:
            self._ended = True
            return b""
        if not isinstance(bounded_value, bytes):
            raise ValueError("object upload queue contained an invalid chunk")
        if len(bounded_value) > size:
            self._pending = bounded_value[size:]
            return bounded_value[:size]
        return bounded_value

    def _next_value(self) -> bytes | object:
        while True:
            if self._cancelled.is_set():
                return ObjectUpload._END
            try:
                return self._values.get(timeout=0.1)
            except queue.Empty:
                continue


class ObjectStorage:
    """Bounded async access to the process-wide synchronous MinIO client."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        max_connections: int,
        *,
        http_client: urllib3.PoolManager | None = None,
        async_client: httpx.AsyncClient | None = None,
        health_client: Any | None = None,
        health_http_client: urllib3.PoolManager | None = None,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.max_connections = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)
        self._http_client = http_client
        self._async_client = async_client
        self._health_client = health_client if health_client is not None else client
        self._health_http_client = health_http_client
        self._executor = ThreadPoolExecutor(
            max_workers=max_connections,
            thread_name_prefix="openoctopus-rustfs",
        )
        self._health_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="openoctopus-rustfs-health",
        )
        self._cancelled_workers: set[asyncio.Future[Any]] = set()
        self._health_lock = asyncio.Lock()
        self._health_worker: asyncio.Future[Any] | None = None
        self._close_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._closing = False
        self._uploads: set[ObjectUpload] = set()

    async def execute(self, operation: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        return await self._execute(
            operation,
            *args,
            detach_on_cancel=False,
            **kwargs,
        )

    async def execute_detached_on_cancel(
        self,
        operation: Callable[..., _T],
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        return await self._execute(
            operation,
            *args,
            detach_on_cancel=True,
            **kwargs,
        )

    async def _execute(
        self,
        operation: Callable[..., _T],
        *args: Any,
        detach_on_cancel: bool,
        **kwargs: Any,
    ) -> _T:
        await self._semaphore.acquire()
        release_now = True
        worker = asyncio.get_running_loop().run_in_executor(
            self._executor,
            partial(operation, *args, **kwargs),
        )
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            if detach_on_cancel:
                release_now = False
                self._cancelled_workers.add(worker)
                worker.add_done_callback(self._release_cancelled_worker)
            else:
                await _wait_for_worker(worker)
            raise
        except Exception as exc:
            normalized = normalize_storage_error(exc)
            operation_name = getattr(operation, "__name__", type(operation).__name__)
            logger.warning(
                "Object storage operation %s failed: %s",
                operation_name,
                normalized.code.value,
            )
            raise normalized from exc
        finally:
            if release_now:
                self._semaphore.release()

    def _release_cancelled_worker(self, worker: asyncio.Future[Any]) -> None:
        try:
            worker.exception()
        except BaseException:
            pass
        self._cancelled_workers.discard(worker)
        self._semaphore.release()

    async def bucket_exists(self) -> bool:
        return await self.execute(self.client.bucket_exists, self.bucket)

    async def stat(self, object_name: str) -> ObjectMetadata:
        def stat_and_validate() -> ObjectMetadata:
            result = self.client.stat_object(self.bucket, object_name)
            return _metadata(result, expected_name=object_name)

        return await self.execute(stat_and_validate)

    async def open_stream(self, object_name: str) -> ObjectStream:
        return await self._open_async_stream(object_name)

    def begin_upload(self, object_name: str, *, length: int | None = None) -> ObjectUpload:
        """Create a bounded stream into an internal RustFS object."""
        if self._closing:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                "Object storage is unavailable",
            )
        upload = ObjectUpload(self, object_name, length=length)
        self._uploads.add(upload)
        upload._worker.add_done_callback(lambda _worker: self._uploads.discard(upload))
        return upload

    async def _open_async_stream(
        self,
        object_name: str,
        *,
        offset: int = 0,
        length: int = 0,
    ) -> ObjectStream:
        await self._semaphore.acquire()
        response: httpx.Response | None = None
        transferred = False
        failure: WorkspaceError | None = None
        try:
            response = await self._send_async_get(
                object_name,
                offset=offset,
                length=length,
            )
            size, etag = _stream_metadata(response)
            stream = ObjectStream(self, response, size=size, etag=etag)
            transferred = True
            return stream
        except asyncio.CancelledError:
            if response is not None:
                await _close_async_response_safely(response)
            raise
        except Exception as exc:
            if response is not None:
                await _close_async_response_safely(response)
            failure = normalize_storage_error(exc)
            failure.__cause__ = None
            failure.__context__ = None
            failure.__traceback__ = None
            logger.warning("Object storage GET failed: %s", failure.code.value)
        finally:
            if not transferred:
                self._semaphore.release()
        if failure is not None:
            raise failure from None
        raise AssertionError("unreachable object storage open state")

    async def _send_async_get(
        self,
        object_name: str,
        *,
        offset: int,
        length: int,
    ) -> httpx.Response:
        if self._async_client is None:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_ERROR,
                "Object storage async client is not configured",
            )
        signed_url = self.client.presigned_get_object(
            self.bucket,
            object_name,
            expires=_PRESIGNED_GET_LIFETIME,
        )
        redacted_url = httpx.URL(signed_url).copy_with(query=None)
        headers = {"Accept-Encoding": "identity"}
        ranged = offset > 0 or length > 0
        if ranged:
            last_byte = offset + length - 1
            headers["Range"] = f"bytes={offset}-{last_byte}"
        for attempt in range(_ASYNC_REQUEST_ATTEMPTS):
            request = self._async_client.build_request(
                "GET",
                redacted_url,
                headers=headers,
                extensions={_SIGNED_URL_EXTENSION: signed_url},
            )
            try:
                response = await self._async_client.send(
                    request,
                    stream=True,
                    follow_redirects=False,
                )
            except httpx.RequestError:
                if attempt + 1 == _ASYNC_REQUEST_ATTEMPTS:
                    raise
                await asyncio.sleep(0.2 * (2**attempt))
                continue
            finally:
                request.extensions.pop(_SIGNED_URL_EXTENSION, None)
            if response.status_code in _ASYNC_RETRY_STATUSES:
                await _close_async_response(response)
                if attempt + 1 == _ASYNC_REQUEST_ATTEMPTS:
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                        "Object storage is unavailable",
                    )
                await asyncio.sleep(0.2 * (2**attempt))
                continue
            expected_status = 206 if ranged else 200
            if response.status_code == expected_status:
                return response
            await _close_async_response(response)
            if response.status_code == 404:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_NOT_FOUND,
                    "Workspace file was not found",
                )
            if response.status_code in {401, 403, 429}:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                    "Object storage is unavailable",
                )
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_ERROR,
                "Object storage request failed",
            )
        raise AssertionError("unreachable object storage retry state")

    async def list_page(
        self,
        prefix: str,
        *,
        start_after: str | None = None,
        limit: int = MAX_LIST_PAGE_SIZE,
    ) -> ObjectPage:
        if not 1 <= limit <= MAX_LIST_PAGE_SIZE:
            raise ValueError(f"list limit must be between 1 and {MAX_LIST_PAGE_SIZE}")

        def collect_page() -> ObjectPage:
            items: list[ObjectMetadata] = []
            has_more = False
            for item in self.client.list_objects(
                self.bucket,
                prefix=prefix,
                recursive=True,
                start_after=start_after,
            ):
                if len(items) == limit:
                    has_more = True
                    break
                items.append(_metadata(item))
            return ObjectPage(
                items=tuple(items),
                next_start_after=items[-1].object_name if has_more else None,
            )

        return await self.execute(collect_page)

    async def list_directory_page(
        self,
        prefix: str,
        *,
        start_after: str | None = None,
        limit: int = MAX_LIST_PAGE_SIZE,
    ) -> DirectoryObjectPage:
        if not 1 <= limit <= MAX_LIST_PAGE_SIZE:
            raise ValueError(f"list limit must be between 1 and {MAX_LIST_PAGE_SIZE}")

        def collect_page() -> DirectoryObjectPage:
            items: list[DirectoryObject] = []
            has_more = False
            for item in self.client.list_objects(
                self.bucket,
                prefix=prefix,
                recursive=False,
                start_after=start_after,
            ):
                if len(items) == limit:
                    has_more = True
                    break
                object_name = getattr(item, "object_name", None)
                if not isinstance(object_name, str) or not object_name:
                    raise ValueError("object listing entry is malformed")
                is_directory = bool(getattr(item, "is_dir", False)) or object_name.endswith("/")
                if is_directory:
                    items.append(
                        DirectoryObject(
                            object_name=object_name,
                            size=None,
                            is_directory=True,
                        )
                    )
                else:
                    metadata = _metadata(item)
                    items.append(
                        DirectoryObject(
                            object_name=metadata.object_name,
                            size=metadata.size,
                            is_directory=False,
                        )
                    )
            return DirectoryObjectPage(
                items=tuple(items),
                next_start_after=items[-1].object_name if has_more else None,
            )

        return await self.execute(collect_page)

    async def read(
        self,
        object_name: str,
        *,
        max_bytes: int,
        offset: int = 0,
        length: int = 0,
    ) -> StoredObject:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        requested_length = min(length, max_bytes) if length else max_bytes
        if not length or length > max_bytes:
            requested_length += 1
        ranged = offset > 0 or length > 0
        stream = await self._open_async_stream(
            object_name,
            offset=offset,
            length=requested_length if ranged else 0,
        )
        collected = bytearray()
        try:
            while len(collected) < requested_length:
                chunk = await stream.read()
                if not chunk:
                    break
                remaining = requested_length - len(collected)
                collected.extend(chunk[:remaining])
        finally:
            await stream.aclose()
        data = bytes(collected)
        return StoredObject(
            data=data[:max_bytes],
            etag=stream.etag,
            truncated=len(data) > max_bytes,
        )

    async def write(self, object_name: str, data: bytes) -> ObjectMetadata:
        def put_and_validate() -> ObjectMetadata:
            result = self.client.put_object(
                self.bucket,
                object_name,
                BytesIO(data),
                len(data),
                num_parallel_uploads=1,
            )
            etag = getattr(result, "etag", None)
            if not isinstance(etag, str) or not etag:
                raise ValueError("object write response is missing an ETag")
            return ObjectMetadata(
                object_name=object_name,
                size=len(data),
                etag=etag,
                modified=None,
            )

        return await self.execute(put_and_validate)

    async def promote_if_absent(
        self,
        source_name: str,
        destination_name: str,
        *,
        size: int,
    ) -> ObjectMetadata:
        """Copy an uploaded temporary object to a new destination.

        Workspace mutation locks serialize this check with supported OO writes;
        RustFS' conditional-copy primitive, when available, is an additional
        defense against an external writer.
        """
        try:
            await self.stat(destination_name)
        except WorkspaceError as exc:
            if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                raise
        else:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file already exists",
            )

        def copy() -> ObjectMetadata:
            result = self.client.copy_object(
                self.bucket,
                destination_name,
                CopySource(self.bucket, source_name),
            )
            etag = getattr(result, "etag", None)
            if not isinstance(etag, str) or not etag:
                raise ValueError("object copy response is missing an ETag")
            return ObjectMetadata(
                object_name=destination_name,
                size=size,
                etag=etag,
                modified=None,
            )

        return await self.execute(copy)

    async def delete(self, object_name: str) -> None:
        await self.execute(self.client.remove_object, self.bucket, object_name)

    async def probe_startup(self) -> None:
        may_exist = False
        deleted = False
        try:
            if not await self.bucket_exists():
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                    "Object storage bucket is unavailable",
                )
            probe = secrets.token_bytes(_PROBE_BYTES)
            may_exist = True
            await self.write(STARTUP_PROBE_KEY, probe)
            metadata = await self.stat(STARTUP_PROBE_KEY)
            if metadata.size != len(probe):
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_STORAGE_ERROR,
                    "Object storage probe metadata did not match",
                )
            stored = await self.read(STARTUP_PROBE_KEY, max_bytes=len(probe))
            if stored.truncated or stored.data != probe:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_STORAGE_ERROR,
                    "Object storage probe contents did not match",
                )
            await self.delete(STARTUP_PROBE_KEY)
            deleted = True
        except WorkspaceError:
            raise
        except Exception as exc:
            raise normalize_storage_error(exc) from exc
        finally:
            if may_exist and not deleted:
                try:
                    await self.delete(STARTUP_PROBE_KEY)
                except WorkspaceError:
                    pass

    async def recover_transfer_uploads(self) -> int:
        """Remove transfer temporaries left by a previous server process."""

        removed = 0
        start_after: str | None = None
        while True:
            page = await self.list_page(
                TRANSFER_TEMP_PREFIX,
                start_after=start_after,
            )
            for item in page.items:
                await self.delete(item.object_name)
                removed += 1
            if page.next_start_after is None:
                return removed
            start_after = page.next_start_after

    async def check_health(self) -> None:
        async with self._health_lock:
            if self._closing:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                    "Object storage is unavailable",
                )
            worker = self._health_worker
            if worker is None:
                worker = asyncio.get_running_loop().run_in_executor(
                    self._health_executor,
                    self._health_client.bucket_exists,
                    self.bucket,
                )
                self._health_worker = worker
                worker.add_done_callback(self._health_worker_done)
        try:
            exists = await asyncio.shield(worker)
        except Exception as exc:
            normalized = normalize_storage_error(exc)
            logger.warning("Object storage health check failed: %s", normalized.code.value)
            raise normalized from exc
        if not exists:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                "Object storage bucket is unavailable",
            )

    def _health_worker_done(self, worker: asyncio.Future[Any]) -> None:
        try:
            worker.exception()
        except BaseException:
            pass
        if self._health_worker is worker:
            self._health_worker = None

    async def close(self) -> None:
        async with self._close_lock:
            if self._close_task is None:
                self._close_task = asyncio.create_task(self._close_impl())
            close_task = self._close_task
        await await_future_cancellation_safe(close_task)

    async def _close_impl(self) -> None:
        async with self._health_lock:
            self._closing = True
            health_worker = self._health_worker
        if health_worker is not None:
            await _wait_for_worker(health_worker)
        if self._uploads:
            await asyncio.gather(
                *(upload.abort() for upload in tuple(self._uploads)),
                return_exceptions=True,
            )
        if self._cancelled_workers:
            await asyncio.gather(*self._cancelled_workers, return_exceptions=True)
        if self._async_client is not None:
            await self._async_client.aclose()
        await asyncio.to_thread(self._health_executor.shutdown)
        await asyncio.to_thread(self._executor.shutdown)
        if self._health_http_client is not None:
            await asyncio.to_thread(self._health_http_client.clear)
        if self._http_client is not None:
            await asyncio.to_thread(self._http_client.clear)


async def _wait_for_worker(worker: asyncio.Future[Any]) -> None:
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            continue
        except Exception:
            break
    if worker.done():
        try:
            worker.exception()
        except BaseException:
            pass


async def _await_upload_worker(worker: asyncio.Future[ObjectMetadata]) -> ObjectMetadata:
    return await await_future_cancellation_safe(worker)


async def _close_async_response_safely(response: httpx.Response) -> None:
    try:
        await _close_async_response(response)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


async def _close_async_response(response: Any) -> None:
    close_task = asyncio.create_task(response.aclose())
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError:
        await _wait_for_worker(close_task)
        raise


def build_object_storage(settings: Settings) -> ObjectStorage:
    endpoint, secure = _parse_endpoint(settings.object_storage_endpoint)
    retries = urllib3.Retry(
        total=2,
        connect=2,
        read=0,
        status=2,
        backoff_factor=0.2,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"DELETE", "GET", "HEAD", "PUT"}),
    )
    http_client = urllib3.PoolManager(
        num_pools=1,
        maxsize=settings.object_storage_max_connections,
        block=True,
        timeout=urllib3.Timeout(connect=5, read=30),
        retries=retries,
    )
    client = Minio(
        endpoint=endpoint,
        access_key=settings.object_storage_access_key,
        secret_key=settings.object_storage_secret_key,
        secure=secure,
        region=settings.object_storage_region,
        http_client=http_client,
    )
    health_http_client = urllib3.PoolManager(
        num_pools=1,
        maxsize=1,
        block=True,
        timeout=urllib3.Timeout(connect=5, read=30),
        retries=retries,
    )
    health_client = Minio(
        endpoint=endpoint,
        access_key=settings.object_storage_access_key,
        secret_key=settings.object_storage_secret_key,
        secure=secure,
        region=settings.object_storage_region,
        http_client=health_http_client,
    )
    async_client = httpx.AsyncClient(
        timeout=httpx.Timeout(30, connect=5),
        transport=_PresignedGetTransport(
            max_connections=settings.object_storage_max_connections,
        ),
        follow_redirects=False,
        trust_env=False,
    )
    return ObjectStorage(
        client,
        settings.object_storage_bucket,
        settings.object_storage_max_connections,
        http_client=http_client,
        async_client=async_client,
        health_client=health_client,
        health_http_client=health_http_client,
    )


@lru_cache
def get_object_storage() -> ObjectStorage:
    return build_object_storage(get_settings())


def normalize_storage_error(exc: Exception) -> WorkspaceError:
    if isinstance(exc, WorkspaceError):
        return exc
    code = getattr(exc, "code", None)
    if code in {"NoSuchKey", "NoSuchObject", "NoSuchObjectName"}:
        return WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Workspace file was not found")
    status_code = getattr(exc, "status_code", None)
    if (
        code
        in {
            "AccessDenied",
            "InvalidAccessKeyId",
            "InvalidToken",
            "NoSuchBucket",
            "SignatureDoesNotMatch",
            "InternalError",
            "ServiceUnavailable",
            "SlowDown",
        }
        or status_code in {500, 502, 503, 504}
        or isinstance(
            exc,
            (
                ConnectionError,
                TimeoutError,
                OSError,
                httpx.RequestError,
                urllib3.exceptions.HTTPError,
            ),
        )
    ):
        return WorkspaceError(
            ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
            "Object storage is unavailable",
        )
    return WorkspaceError(ErrorCode.WORKSPACE_STORAGE_ERROR, "Object storage request failed")


def _stream_metadata(response: Any) -> tuple[int, str]:
    headers = getattr(response, "headers", None)
    if headers is None:
        raise ValueError("object response is missing headers")
    raw_size = headers.get("Content-Length")
    etag = headers.get("ETag")
    if not isinstance(raw_size, str):
        raise ValueError("object response is missing a Content-Length")
    try:
        size = int(raw_size)
    except ValueError as exc:
        raise ValueError("object response has an invalid Content-Length") from exc
    if size < 0:
        raise ValueError("object response has an invalid Content-Length")
    if not isinstance(etag, str) or not etag.strip('"'):
        raise ValueError("object response is missing an ETag")
    return size, etag.strip('"')


def _metadata(item: Any, *, expected_name: str | None = None) -> ObjectMetadata:
    object_name = getattr(item, "object_name", expected_name)
    size = getattr(item, "size", None)
    etag = getattr(item, "etag", None)
    modified = getattr(item, "last_modified", None)
    if (
        not isinstance(object_name, str)
        or not object_name
        or (expected_name is not None and object_name != expected_name)
        or not isinstance(size, int)
        or size < 0
        or not isinstance(etag, str)
        or not etag
    ):
        raise ValueError("object metadata is malformed")
    return ObjectMetadata(
        object_name=object_name,
        size=size,
        etag=etag,
        modified=modified if isinstance(modified, datetime) else None,
    )


def _parse_endpoint(endpoint: str) -> tuple[str, bool]:
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("object storage endpoint must be an http(s) origin URL")
    return parsed.netloc, parsed.scheme == "https"
