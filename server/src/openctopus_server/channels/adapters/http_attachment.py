from __future__ import annotations

from collections.abc import AsyncIterator, Mapping

import httpx

AUTHENTICATED_ATTACHMENT_CHUNK_BYTES = 64 * 1024
MAX_AUTHENTICATED_ATTACHMENT_BYTES = 64 * 1024 * 1024


class HttpAuthenticatedAttachmentStream:
    def __init__(self, response: httpx.Response, *, size: int) -> None:
        self.size = size
        self._response = response
        self._chunks: AsyncIterator[bytes] = response.aiter_bytes(
            chunk_size=AUTHENTICATED_ATTACHMENT_CHUNK_BYTES
        )
        self._buffer = bytearray()
        self._observed = 0
        self._eof = False
        self._closed = False

    async def read(self, max_bytes: int) -> bytes:
        if max_bytes <= 0:
            raise ValueError("Attachment read size must be positive")
        if self._closed:
            return b""
        target = min(max_bytes, AUTHENTICATED_ATTACHMENT_CHUNK_BYTES)
        while len(self._buffer) < target and not self._eof:
            try:
                chunk = await anext(self._chunks)
            except StopAsyncIteration:
                self._eof = True
                if self._observed != self.size:
                    raise ValueError("Authenticated attachment size changed") from None
                break
            if not isinstance(chunk, bytes) or len(chunk) > AUTHENTICATED_ATTACHMENT_CHUNK_BYTES:
                raise ValueError("Authenticated attachment chunk is invalid")
            self._observed += len(chunk)
            if self._observed > self.size:
                raise ValueError("Authenticated attachment exceeds its metadata size")
            self._buffer.extend(chunk)
        result = bytes(self._buffer[:target])
        del self._buffer[:target]
        return result

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._response.aclose()


async def open_http_attachment(
    client: httpx.AsyncClient,
    url: str,
    *,
    size: int,
    headers: Mapping[str, str] | None = None,
) -> HttpAuthenticatedAttachmentStream:
    if not 0 <= size <= MAX_AUTHENTICATED_ATTACHMENT_BYTES:
        raise ValueError("Authenticated attachment size is invalid")
    request = client.build_request(
        "GET",
        url,
        headers={"Accept-Encoding": "identity", **dict(headers or {})},
    )
    response = await client.send(request, stream=True, follow_redirects=False)
    try:
        if not 200 <= response.status_code < 300:
            raise ValueError("Authenticated attachment download was rejected")
        if response.headers.get("Content-Encoding", "identity").casefold() not in {
            "",
            "identity",
        }:
            raise ValueError("Authenticated attachment response is encoded")
        content_length = response.headers.get("Content-Length")
        if content_length is not None and int(content_length) != size:
            raise ValueError("Authenticated attachment size changed")
    except BaseException:
        await response.aclose()
        raise
    return HttpAuthenticatedAttachmentStream(response, size=size)
