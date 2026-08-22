import asyncio
import gzip
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import openctopus_server.tools.web_fetch as web_fetch_module
from openctopus_server.admission import KeyedAdmission
from openctopus_server.db.models import SystemConfig
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.network_policy import compile_ssrf_policy
from openctopus_server.services.system_config import load_web_fetch_policy
from openctopus_server.tools.base import ToolContext, ToolResult
from openctopus_server.tools.truncate import TRUNCATION_MARKER
from openctopus_server.tools.web_fetch import (
    DEFAULT_MAX_CHARS,
    HtmlContentConverter,
    PolicyLoader,
    WebFetchTool,
)

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


class _StubContentConverter:
    def __init__(self, result: str = "unused") -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    async def parse_html(
        self,
        data: bytes,
        *,
        user_id: Any,
        charset: str,
        base_url: str,
        mode: str,
        max_chars: int,
    ) -> str:
        self.calls.append(
            {
                "data": data,
                "user_id": user_id,
                "charset": charset,
                "base_url": base_url,
                "mode": mode,
                "max_chars": max_chars,
            }
        )
        return self.result


def _web_fetch_tool(
    *,
    resolver: Resolver | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    content_converter: HtmlContentConverter | None = None,
    web_admission: KeyedAdmission | None = None,
    denylist: list[str] | None = None,
    policy_loader: PolicyLoader | None = None,
) -> WebFetchTool:
    async def load_policy():
        return compile_ssrf_policy(denylist) if denylist is not None else compile_ssrf_policy(None)

    return WebFetchTool(
        web_admission=web_admission
        or KeyedAdmission(global_limit=2, per_key_limit=1, timeout_seconds=1),
        content_converter=content_converter or _StubContentConverter(),
        resolver=resolver,
        transport=transport,
        policy_loader=policy_loader or load_policy,
    )


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

    tool = _web_fetch_tool(
        resolver=_public_resolver(resolver_calls),
        transport=httpx.MockTransport(handler),
    )

    result = await tool.execute({"url": "https://example.com/path?q=1"}, _ctx())

    assert result == ToolResult(content="hello")
    assert resolver_calls == [("example.com", 443)]
    assert captured == {
        "url": f"https://{PUBLIC_IP}/path?q=1",
        "host": "example.com",
        "sni": "example.com",
        "accept_encoding": "identity",
    }


def test_fetch_schema_advertises_hard_max_chars() -> None:
    max_chars = _web_fetch_tool().schema()["input_schema"]["properties"]["maxChars"]

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
        "::ffff:127.0.0.1",
        "fc00::1",
        "fe80::1",
        "ff02::1",
    ],
)
async def test_fetch_blocks_non_public_dns_targets(address: str) -> None:
    async def resolve(hostname: str, port: int) -> list[str]:
        return [address]

    tool = _web_fetch_tool(
        resolver=resolve,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="unsafe")),
    )

    result = await tool.execute({"url": "http://unsafe.example"}, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED


async def test_fetch_rejects_mixed_public_and_private_dns_answers() -> None:
    async def resolve(hostname: str, port: int) -> list[str]:
        return [PUBLIC_IP, "127.0.0.1"]

    tool = _web_fetch_tool(resolver=resolve, transport=httpx.MockTransport(lambda request: None))

    result = await tool.execute({"url": "http://mixed.example"}, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED


async def test_fetch_uses_one_dns_snapshot_per_hop_and_connects_to_that_ip() -> None:
    calls = 0
    captured_url = ""

    async def resolver(hostname: str, port: int) -> list[str]:
        nonlocal calls
        calls += 1
        assert (hostname, port) == ("snapshot.example", 80)
        return [PUBLIC_IP]

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_url
        captured_url = str(request.url)
        return _streaming_text_response("safe", headers={"content-type": "text/plain"})

    tool = _web_fetch_tool(resolver=resolver, transport=httpx.MockTransport(handler))

    result = await tool.execute({"url": "http://snapshot.example/path"}, _ctx())

    assert result == ToolResult(content="safe")
    assert calls == 1
    assert captured_url == f"http://{PUBLIC_IP}/path"


async def test_fetch_explicit_empty_denylist_allows_private_target() -> None:
    tool = _web_fetch_tool(
        denylist=[],
        resolver=lambda hostname, port: asyncio.sleep(0, result=["10.1.2.3"]),
        transport=httpx.MockTransport(
            lambda request: _streaming_text_response(
                "internal",
                headers={"content-type": "text/plain"},
            )
        ),
    )

    result = await tool.execute({"url": "http://internal.example"}, _ctx())

    assert result == ToolResult(content="internal")


async def test_fetch_applies_hostname_port_rule_before_dns() -> None:
    resolver_calls: list[tuple[str, int]] = []
    tool = _web_fetch_tool(
        denylist=["blocked.example:8443"],
        resolver=_public_resolver(resolver_calls),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="unused")),
    )

    result = await tool.execute({"url": "https://blocked.example:8443/path"}, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED
    assert resolver_calls == []


async def test_fetch_admission_is_acquired_before_dns_and_is_released() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    resolver_calls: list[str] = []

    async def blocking_resolver(hostname: str, port: int) -> list[str]:
        del port
        resolver_calls.append(hostname)
        entered.set()
        await release.wait()
        return [PUBLIC_IP]

    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=0.01)
    tool = _web_fetch_tool(
        web_admission=admission,
        resolver=blocking_resolver,
        transport=httpx.MockTransport(
            lambda request: _streaming_text_response("ok", headers={"content-type": "text/plain"})
        ),
    )
    first = asyncio.create_task(tool.execute({"url": "https://first.example"}, _ctx()))
    await entered.wait()

    second = await tool.execute({"url": "https://second.example"}, _ctx())

    assert second.code is ErrorCode.NETWORK_TIMEOUT
    assert resolver_calls == ["first.example"]
    release.set()
    assert await first == ToolResult(content="ok")
    assert admission.entry_count == 0


async def test_fetch_admission_is_held_through_html_conversion() -> None:
    entered_conversion = asyncio.Event()
    release_conversion = asyncio.Event()
    resolver_calls: list[str] = []

    class BlockingConverter(_StubContentConverter):
        async def parse_html(self, *args: Any, **kwargs: Any) -> str:
            entered_conversion.set()
            await release_conversion.wait()
            return "converted"

    async def counting_resolver(hostname: str, port: int) -> list[str]:
        del port
        resolver_calls.append(hostname)
        return [PUBLIC_IP]

    admission = KeyedAdmission(global_limit=1, per_key_limit=1, timeout_seconds=0.01)
    tool = _web_fetch_tool(
        web_admission=admission,
        content_converter=BlockingConverter(),
        resolver=counting_resolver,
        transport=httpx.MockTransport(
            lambda request: _streaming_text_response(
                "<p>body</p>", headers={"content-type": "text/html; charset=utf-8"}
            )
        ),
    )
    first = asyncio.create_task(tool.execute({"url": "https://first.example"}, _ctx()))
    await entered_conversion.wait()

    second = await tool.execute({"url": "https://second.example"}, _ctx())

    assert second.code is ErrorCode.NETWORK_TIMEOUT
    assert resolver_calls == ["first.example"]
    release_conversion.set()
    assert await first == ToolResult(content="converted")


async def test_fetch_checks_every_redirect_target_before_requesting_it() -> None:
    requests: list[str] = []
    policy_loads = 0

    async def load_policy():
        nonlocal policy_loads
        policy_loads += 1
        return compile_ssrf_policy(None)

    async def resolve(hostname: str, port: int) -> list[str]:
        return ["127.0.0.1"] if hostname == "private.example" else [PUBLIC_IP]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://private.example/secret"})

    tool = _web_fetch_tool(
        resolver=resolve,
        transport=httpx.MockTransport(handler),
        policy_loader=load_policy,
    )

    result = await tool.execute({"url": "https://public.example/start"}, _ctx())

    assert requests == [f"https://{PUBLIC_IP}/start"]
    assert policy_loads == 1
    assert result.is_error is True
    assert result.code == ErrorCode.NETWORK_SSRF_BLOCKED


async def test_fetch_hot_reads_the_postgres_policy(pg_engine: Any) -> None:
    requests: list[str] = []

    async def resolve(_hostname: str, _port: int) -> list[str]:
        return ["10.1.2.3"]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return _streaming_text_response("ok", headers={"content-type": "text/plain"})

    tool = WebFetchTool(
        web_admission=KeyedAdmission(global_limit=2, per_key_limit=1, timeout_seconds=1),
        content_converter=_StubContentConverter(),
        resolver=resolve,
        transport=httpx.MockTransport(handler),
        policy_loader=lambda: load_web_fetch_policy(pg_engine),
    )

    blocked = await tool.execute({"url": "http://internal.example/status"}, _ctx())
    async with AsyncSession(pg_engine) as db:
        db.add(SystemConfig(key="web_fetch_denylist", value=[]))
        await db.commit()
    allowed = await tool.execute({"url": "http://internal.example/status"}, _ctx())

    assert blocked.code is ErrorCode.NETWORK_SSRF_BLOCKED
    assert allowed == ToolResult(content="ok")
    assert requests == ["http://10.1.2.3/status"]


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
    converter = _StubContentConverter(expected)
    tool = _web_fetch_tool(
        content_converter=converter,
        resolver=_public_resolver(),
        transport=httpx.MockTransport(
            lambda request: _streaming_text_response(
                html,
                headers={"content-type": "text/html; charset=utf-8"},
            )
        ),
    )

    ctx = _ctx()
    result = await tool.execute(
        {"url": "https://example.com", "extractMode": mode},
        ctx,
    )

    assert _result_text(result) == expected
    assert converter.calls == [
        {
            "data": html.encode(),
            "user_id": ctx.user_id,
            "charset": "utf-8",
            "base_url": "https://example.com",
            "mode": mode,
            "max_chars": DEFAULT_MAX_CHARS,
        }
    ]


async def test_fetch_applies_requested_and_maximum_output_caps() -> None:
    tool = _web_fetch_tool(
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
    tool = _web_fetch_tool(
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
    tool = _web_fetch_tool(
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

    dns_tool = _web_fetch_tool(
        resolver=dns_failure,
        transport=httpx.MockTransport(lambda request: httpx.Response(200)),
    )

    def timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    timeout_tool = _web_fetch_tool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(timeout_handler),
    )
    http_tool = _web_fetch_tool(
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
    tool = _web_fetch_tool(
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
    tool = _web_fetch_tool(
        resolver=_public_resolver(),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="unused")),
    )

    result = await tool.execute(args, _ctx())

    assert result.is_error is True
    assert result.code == ErrorCode.TOOL_INVALID_ARGS
