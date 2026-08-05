from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import lru_cache, partial
from io import BytesIO
from typing import Any, TypeVar
from urllib.parse import urlsplit

import urllib3
from minio import Minio

from openctopus_server.config import Settings, get_settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError

STARTUP_PROBE_KEY = "_openoctopus/startup-probe"
_PROBE_BYTES = 32
MAX_LIST_PAGE_SIZE = 1000
_T = TypeVar("_T")
logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class ObjectPage:
    items: tuple[ObjectMetadata, ...]
    next_start_after: str | None


class ObjectStorage:
    """Bounded async access to the process-wide synchronous MinIO client."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        max_connections: int,
        *,
        http_client: urllib3.PoolManager | None = None,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.max_connections = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)
        self._http_client = http_client
        self._executor = ThreadPoolExecutor(
            max_workers=max_connections,
            thread_name_prefix="openoctopus-rustfs",
        )
        self._cancelled_workers: set[asyncio.Future[Any]] = set()

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

        def read_and_close() -> StoredObject:
            requested_length = min(length, max_bytes) if length else max_bytes
            if not length or length > max_bytes:
                requested_length += 1
            response = self.client.get_object(
                self.bucket,
                object_name,
                offset=offset,
                length=requested_length,
            )
            try:
                data = response.read(requested_length)
                headers = getattr(response, "headers", {})
                etag = headers.get("ETag") if headers is not None else None
                if isinstance(etag, str):
                    etag = etag.strip('"')
                if not etag:
                    raise ValueError("object response is missing an ETag")
                return StoredObject(
                    data=data[:max_bytes],
                    etag=etag,
                    truncated=len(data) > max_bytes,
                )
            finally:
                response.close()
                response.release_conn()

        return await self.execute(read_and_close)

    async def write(self, object_name: str, data: bytes) -> ObjectMetadata:
        def put_and_validate() -> ObjectMetadata:
            result = self.client.put_object(
                self.bucket,
                object_name,
                BytesIO(data),
                len(data),
            )
            etag = getattr(result, "etag", None)
            if not isinstance(etag, str) or not etag:
                raise ValueError("object write response is missing an ETag")
            return ObjectMetadata(
                object_name=object_name,
                size=len(data),
                etag=etag,
            )

        return await self.execute(put_and_validate)

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

    async def check_health(self) -> None:
        exists = await self.execute_detached_on_cancel(
            self.client.bucket_exists,
            self.bucket,
        )
        if not exists:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                "Object storage bucket is unavailable",
            )

    async def close(self) -> None:
        if self._cancelled_workers:
            await asyncio.gather(*self._cancelled_workers, return_exceptions=True)
        await asyncio.to_thread(self._executor.shutdown)
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
    return ObjectStorage(
        client,
        settings.object_storage_bucket,
        settings.object_storage_max_connections,
        http_client=http_client,
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
            (ConnectionError, TimeoutError, OSError, urllib3.exceptions.HTTPError),
        )
    ):
        return WorkspaceError(
            ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
            "Object storage is unavailable",
        )
    return WorkspaceError(ErrorCode.WORKSPACE_STORAGE_ERROR, "Object storage request failed")


def _metadata(item: Any, *, expected_name: str | None = None) -> ObjectMetadata:
    object_name = getattr(item, "object_name", expected_name)
    size = getattr(item, "size", None)
    etag = getattr(item, "etag", None)
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
    return ObjectMetadata(object_name=object_name, size=size, etag=etag)


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
