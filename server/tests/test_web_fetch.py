import asyncio
import gzip
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from uuid import uuid4

import httpx
import pytest

import openctopus_server.tools.web_fetch as web_fetch_module
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolContext, ToolResult
from openctopus_server.tools.truncate import TRUNCATION_MARKER
from openctopus_server.tools.web_fetch import DEFAULT_MAX_CHARS, WebFetchTool

Resolver = Callable[[str, int], Awaitable[list[str]]]
PUBLIC_IP = "93.184.216.34"


class _TrackingStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.iterated = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.iterated = True
        yield self.content


class _ChunkTrackingStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.yielded = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk


def _streaming_text_response(
    text: str,
    *,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(
        200,
        headers=headers,
        stream=_TrackingStream(text.encode()),
    )


def _ctx() -> ToolContext:
    return ToolContext(user_id=uuid4(), session_id=uuid4())


def _public_resolver(calls: list[tuple[str, int]] | None = None) -> Resolver:
    async def resolve(hostname: str, port: int) -> list[str]:
        if calls is not None:
            calls.append((hostname, port))
        return [PUBLIC_IP]

    return resolve


def _result_text(result: ToolResult) -> str:
    assert isinstance(result.content, str)
    return result.content


async def test_fetch_pins_checked_ip_and_preserves_host_and_sni() -> None:
    captured: dict[str, Any] = {}
    resolver_calls: list[tuple[str, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["host"] = request.headers["host"]
        captured["sni"] = request.extensions["sni_hostname"]
        captured["accept_encoding"] = request.headers["accept-encoding"]
        return _streaming_text_response("hello", headers={"content-type": "text/plain"})

    tool = WebFetchTool(
        resolver=_public_resolver(resolver_calls),
        transport=httpx.MockTransport(handler),
    )

    result = await tool.execute({"url": "https://example.com/path?q=1"}, _ctx())

    assert result == ToolResult(content="hello")
    assert resolver_calls == [("example.com", 443), ("example.com", 443)]
    assert captured == {
        "url": f"https://{PUBLIC_IP}/path?q=1",
        "host": "example.com",
        "sni": "example.com",
        "accept_encoding": "identity",
    }


def test_fetch_schema_advertises_hard_max_chars() -> None:
    max_chars = WebFetchTool().schema()["input_schema"]["properties"]["maxChars"]

    assert max_chars == {
        "type": "integer",
        "minimum": 100,
        "maximum": DEFAULT_MAX_CHARS,
    }


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.1.2.3",
        "172.16.1.2",
        "192.168.1.2",
        "100.64.1.2",
        "169.254.1.2",
        "224.0.0.1",
        "240.0.0.1",
        "0.0.0.0",
        "::1",
        "fc00::1",
        "fe80::1",
        "ff02::1",
    ],
)
async def test_fetch_blocks_non_public_dns_targets(address: str) -> None:
    async def resolve(hostname: str, port: int) -> list[str]:
        return [address]

    tool = WebFetchTool(
        resolver=resolve,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="unsafe")),
    )

    result = await tool.execute({"url": "http://unsafe.example"}, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED


async def test_fetch_rejects_mixed_public_and_private_dns_answers() -> None:
    async def resolve(hostname: str, port: int) -> list[str]:
        return [PUBLIC_IP, "127.0.0.1"]

    tool = WebFetchTool(resolver=resolve, transport=httpx.MockTransport(lambda request: None))

    result = await tool.execute({"url": "http://mixed.example"}, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED


async def test_fetch_rechecks_dns_immediately_before_connect() -> None:
    calls = 0

    async def rebinding_resolver(hostname: str, port: int) -> list[str]:
        nonlocal calls
        calls += 1
        return [PUBLIC_IP] if calls == 1 else ["127.0.0.1"]

    tool = WebFetchTool(
        resolver=rebinding_resolver,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="unsafe")),
    )

    result = await tool.execute({"url": "http://rebind.example"}, _ctx())

    assert calls == 2
    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED


async def test_fetch_checks_every_redirect_target_before_requesting_it() -> None:
    requests: list[str] = []

    async def resolve(hostname: str, port: int) -> list[str]:
        return ["127.0.0.1"] if hostname == "private.example" else [PUBLIC_IP]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://private.example/secret"})

    tool = WebFetchTool(resolver=resolve, transport=httpx.MockTransport(handler))

    result = await tool.execute({"url": "https://public.example/start"}, _ctx())

    assert requests == [f"https://{PUBLIC_IP}/start"]
    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("markdown", "# Hello\n\nSee [docs](https://example.com/docs)."),
        ("text", "Hello\n\nSee docs."),
    ],
)
async def test_fetch_extracts_readable_html(mode: str, expected: str) -> None:
    html = (
        "<html><body><h1>Hello</h1><p>See <a href='/docs'>docs</a>.</p>"
        "<script>ignore me</script></body></html>"
    )
    tool = WebFetchTool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(
            lambda request: _streaming_text_response(
                html,
                headers={"content-type": "text/html; charset=utf-8"},
            )
        ),
    )

    result = await tool.execute(
        {"url": "https://example.com", "extractMode": mode},
        _ctx(),
    )

    assert _result_text(result) == expected


async def test_fetch_applies_requested_and_maximum_output_caps() -> None:
    tool = WebFetchTool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(lambda request: _streaming_text_response("x" * 60_000)),
    )

    requested = await tool.execute(
        {"url": "https://example.com", "maxChars": 100},
        _ctx(),
    )
    maximum = await tool.execute(
        {"url": "https://example.com", "maxChars": DEFAULT_MAX_CHARS},
        _ctx(),
    )

    assert _result_text(requested) == f"{'x' * 100}{TRUNCATION_MARKER}"
    assert _result_text(maximum) == f"{'x' * DEFAULT_MAX_CHARS}{TRUNCATION_MARKER}"


async def test_fetch_rejects_compressed_body_without_reading_or_decompressing() -> None:
    compressed_bomb = gzip.compress(b"x" * (web_fetch_module.MAX_RESPONSE_BYTES + 1))
    stream = _TrackingStream(compressed_bomb)
    tool = WebFetchTool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={
                    "content-type": "text/plain",
                    "content-encoding": "gzip",
                },
                stream=stream,
            )
        ),
    )

    result = await tool.execute({"url": "https://example.com"}, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_HTTP_ERROR
    assert stream.iterated is False


async def test_fetch_stops_stream_at_exact_raw_body_cap() -> None:
    stream = _ChunkTrackingStream([b"x" * web_fetch_module.MAX_RESPONSE_BYTES, b"must-not-be-read"])
    tool = WebFetchTool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"content-type": "text/plain"},
                stream=stream,
            )
        ),
    )

    result = await tool.execute({"url": "https://example.com"}, _ctx())

    assert result.is_error is False
    assert stream.yielded == 1


async def test_fetch_maps_dns_timeout_and_http_errors() -> None:
    async def dns_failure(hostname: str, port: int) -> list[str]:
        raise socket.gaierror("no address")

    dns_tool = WebFetchTool(
        resolver=dns_failure,
        transport=httpx.MockTransport(lambda request: httpx.Response(200)),
    )

    def timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    timeout_tool = WebFetchTool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(timeout_handler),
    )
    http_tool = WebFetchTool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(lambda request: httpx.Response(503)),
    )

    dns_result = await dns_tool.execute({"url": "https://missing.example"}, _ctx())
    timeout_result = await timeout_tool.execute({"url": "https://slow.example"}, _ctx())
    http_result = await http_tool.execute({"url": "https://down.example"}, _ctx())

    assert dns_result.code == ErrorCode.NETWORK_DNS_FAILED
    assert timeout_result.code == ErrorCode.NETWORK_TIMEOUT
    assert http_result.code == ErrorCode.NETWORK_HTTP_ERROR


async def test_fetch_enforces_total_timeout_during_resolution(monkeypatch) -> None:
    async def never_resolves(hostname: str, port: int) -> list[str]:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(web_fetch_module, "TOTAL_TIMEOUT_SECONDS", 0.01)
    tool = WebFetchTool(
        resolver=never_resolves,
        transport=httpx.MockTransport(lambda request: httpx.Response(200)),
    )

    result = await tool.execute({"url": "https://slow-dns.example"}, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_TIMEOUT


@pytest.mark.parametrize(
    "args",
    [
        {"url": "file:///etc/passwd"},
        {"url": "https://user:password@example.com"},
        {"url": "https://example.com", "maxChars": 99},
        {"url": "https://example.com", "maxChars": DEFAULT_MAX_CHARS + 1},
        {"url": "https://example.com", "extractMode": "raw"},
        {"url": "https://example.com", "unknown": True},
    ],
)
async def test_fetch_rejects_invalid_args(args: dict[str, Any]) -> None:
    tool = WebFetchTool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="unused")),
    )

    result = await tool.execute(args, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.TOOL_INVALID_ARGS
