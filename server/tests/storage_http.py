"""Test-only adapter from synchronous MinIO fakes to presigned HTTP GETs."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from urllib.parse import quote, unquote

import httpx

from openctopus_server.workspace.storage import ObjectStorage


class _BytesStream(httpx.AsyncByteStream):
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._data


class _PresigningClient:
    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def presigned_get_object(
        self,
        bucket: str,
        object_name: str,
        *,
        expires: timedelta,
    ) -> str:
        assert bucket == self._bucket
        assert expires > timedelta(0)
        encoded = quote(object_name, safe="/")
        return f"https://rustfs.test/{bucket}/{encoded}?X-Amz-Signature=test-only"


def object_storage_for_fake(
    client: Any,
    bucket: str,
    *,
    max_connections: int,
) -> ObjectStorage:
    async def handler(request: httpx.Request) -> httpx.Response:
        object_name = unquote(request.url.path.removeprefix(f"/{bucket}/"))
        range_header = request.headers.get("Range")
        offset = 0
        length = 0
        status = 200
        if range_header is not None:
            bounds = range_header.removeprefix("bytes=").split("-", 1)
            offset = int(bounds[0])
            length = int(bounds[1]) - offset + 1
            status = 206
        try:
            response = await asyncio.to_thread(
                client.get_object,
                bucket,
                object_name,
                offset=offset,
                length=length,
            )
            if length:
                data = await asyncio.to_thread(response.read, length)
            else:
                data = await asyncio.to_thread(response.read)
        except Exception as exc:
            code = getattr(exc, "code", None)
            if code in {"NoSuchKey", "NoSuchObject", "NoSuchObjectName"}:
                return httpx.Response(404)
            if isinstance(exc, OSError):
                raise httpx.ConnectError("test storage unavailable", request=request) from exc
            return httpx.Response(500)
        finally:
            if "response" in locals():
                await asyncio.to_thread(response.close)
                release = getattr(response, "release_conn", None)
                if release is not None:
                    await asyncio.to_thread(release)
        response_headers = getattr(response, "headers", {})
        headers = {"Content-Length": str(len(data))}
        etag = response_headers.get("ETag")
        if isinstance(etag, str):
            headers["ETag"] = etag
        return httpx.Response(status, stream=_BytesStream(data), headers=headers)

    wrapped = _PresigningClient(client, bucket)
    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ObjectStorage(
        wrapped,
        bucket,
        max_connections=max_connections,
        async_client=async_client,
    )
