import asyncio
import ipaddress
import re
import socket
from collections.abc import Awaitable, Callable
from copy import deepcopy
from html.parser import HTMLParser
from typing import Any, Literal
from urllib.parse import SplitResult, urljoin, urlsplit, urlunsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import NetworkError
from openctopus_server.tools.base import Tool, ToolContext, ToolResult
from openctopus_server.tools.truncate import truncate_head

Resolver = Callable[[str, int], Awaitable[list[str]]]

DEFAULT_MAX_CHARS = 50_000
MAX_RESPONSE_BYTES = 5_000_000
MAX_REDIRECTS = 10
CONNECT_TIMEOUT_SECONDS = 10.0
TOTAL_TIMEOUT_SECONDS = 30.0

WEB_FETCH_SCHEMA: dict[str, Any] = {
    "name": "web_fetch",
    "description": (
        "Fetch a URL and extract readable content (HTML → markdown/text). "
        "Output is capped at maxChars (default 50 000). Works for most web pages "
        "and docs; may fail on login-walled or JS-heavy sites."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "URL to fetch"},
            "extractMode": {
                "type": "string",
                "enum": ["markdown", "text"],
                "default": "markdown",
            },
            "maxChars": {
                "type": "integer",
                "minimum": 100,
                "maximum": DEFAULT_MAX_CHARS,
            },
        },
        "required": ["url"],
    },
}


class _WebFetchArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    extract_mode: Literal["markdown", "text"] = Field("markdown", alias="extractMode")
    max_chars: int = Field(
        DEFAULT_MAX_CHARS,
        alias="maxChars",
        ge=100,
        le=DEFAULT_MAX_CHARS,
    )


class WebFetchTool(Tool):
    def __init__(
        self,
        *,
        resolver: Resolver | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._resolver = resolver or _resolve_dns
        self._transport = transport

    def name(self) -> str:
        return "web_fetch"

    def schema(self) -> dict[str, Any]:
        return deepcopy(WEB_FETCH_SCHEMA)

    def max_output_chars(self) -> int:
        return DEFAULT_MAX_CHARS

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del ctx
        try:
            parsed_args = _WebFetchArgs.model_validate(args)
            _parse_target(parsed_args.url)
        except (ValidationError, ValueError) as exc:
            return _error_result(ErrorCode.TOOL_INVALID_ARGS, f"Invalid web_fetch arguments: {exc}")

        max_chars = parsed_args.max_chars
        timeout = httpx.Timeout(TOTAL_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS)
        try:
            async with asyncio.timeout(TOTAL_TIMEOUT_SECONDS):
                async with httpx.AsyncClient(
                    transport=self._transport,
                    timeout=timeout,
                    follow_redirects=False,
                    trust_env=False,
                ) as client:
                    text = await self._fetch(
                        client,
                        url=parsed_args.url,
                        extract_mode=parsed_args.extract_mode,
                    )
        except NetworkError as exc:
            return _error_result(exc.code, exc.message)
        except httpx.TimeoutException:
            return _error_result(ErrorCode.NETWORK_TIMEOUT, "web_fetch timed out")
        except TimeoutError:
            return _error_result(ErrorCode.NETWORK_TIMEOUT, "web_fetch timed out")
        except httpx.RequestError as exc:
            return _error_result(
                ErrorCode.NETWORK_HTTP_ERROR,
                f"web_fetch request failed: {exc}",
            )

        return ToolResult(content=truncate_head(text, max_chars))

    async def _fetch(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        extract_mode: Literal["markdown", "text"],
    ) -> str:
        current_url = url
        for redirect_count in range(MAX_REDIRECTS + 1):
            response = await self._request_once(client, current_url)
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                await response.aclose()
                if location is None:
                    raise NetworkError(
                        ErrorCode.NETWORK_HTTP_ERROR,
                        "web_fetch redirect omitted the Location header",
                    )
                if redirect_count == MAX_REDIRECTS:
                    raise NetworkError(
                        ErrorCode.NETWORK_HTTP_ERROR,
                        "web_fetch exceeded the redirect limit",
                    )
                current_url = urljoin(current_url, location)
                continue

            if response.status_code >= 400:
                status = response.status_code
                await response.aclose()
                raise NetworkError(
                    ErrorCode.NETWORK_HTTP_ERROR,
                    f"web_fetch received HTTP {status}",
                )

            content_type = response.headers.get("content-type", "").lower()
            encoding = response.encoding or "utf-8"
            try:
                body = await _read_limited(response)
            finally:
                await response.aclose()
            decoded = _decode(body, encoding)
            if "text/html" in content_type or "application/xhtml+xml" in content_type:
                return _extract_html(decoded, mode=extract_mode, base_url=current_url)
            return decoded

        raise AssertionError("redirect loop terminated unexpectedly")

    async def _request_once(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> httpx.Response:
        target = _parse_target(url)
        await self._resolve_and_validate(target.hostname, target.port)
        connect_addresses = await self._resolve_and_validate(target.hostname, target.port)
        connect_address = connect_addresses[0]
        pinned_url = _pinned_url(target.parsed, connect_address, target.port)
        request = client.build_request(
            "GET",
            pinned_url,
            headers={
                "host": _host_header(target.hostname, target.port, target.parsed.scheme),
                "user-agent": "OpenOctopus/0.0.1 web_fetch",
                "accept": "text/html,text/plain,application/json,*/*",
                "accept-encoding": "identity",
            },
            extensions={"sni_hostname": target.hostname},
        )
        return await client.send(request, stream=True, follow_redirects=False)

    async def _resolve_and_validate(self, hostname: str, port: int) -> list[str]:
        try:
            addresses = await self._resolver(hostname, port)
        except (OSError, UnicodeError) as exc:
            raise NetworkError(
                ErrorCode.NETWORK_DNS_FAILED,
                f"Could not resolve {hostname}",
            ) from exc
        if not addresses:
            raise NetworkError(
                ErrorCode.NETWORK_DNS_FAILED,
                f"Could not resolve {hostname}",
            )

        parsed_addresses: list[str] = []
        for address in addresses:
            try:
                parsed = ipaddress.ip_address(address)
            except ValueError as exc:
                raise NetworkError(
                    ErrorCode.NETWORK_DNS_FAILED,
                    f"Resolver returned an invalid address for {hostname}",
                ) from exc
            if not _is_public_address(parsed):
                raise NetworkError(
                    ErrorCode.NETWORK_SSRF_BLOCKED,
                    f"Blocked non-public address for {hostname}",
                )
            normalized = str(parsed)
            if normalized not in parsed_addresses:
                parsed_addresses.append(normalized)
        return parsed_addresses


class _Target:
    def __init__(self, parsed: SplitResult, hostname: str, port: int) -> None:
        self.parsed = parsed
        self.hostname = hostname
        self.port = port


def _parse_target(url: str) -> _Target:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("url must use http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("url credentials are not supported")
    if parsed.hostname is None:
        raise ValueError("url must include a hostname")
    if "%" in parsed.hostname:
        raise ValueError("IPv6 zone identifiers are not supported")
    try:
        hostname = parsed.hostname.encode("idna").decode("ascii").rstrip(".").lower()
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except (UnicodeError, ValueError) as exc:
        raise ValueError("url hostname or port is invalid") from exc
    if not hostname:
        raise ValueError("url must include a hostname")
    return _Target(parsed, hostname, port)


async def _resolve_dns(hostname: str, port: int) -> list[str]:
    try:
        parsed = ipaddress.ip_address(hostname)
    except ValueError:
        loop = asyncio.get_running_loop()
        records = await loop.getaddrinfo(
            hostname,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
        return [str(record[4][0]) for record in records]
    return [str(parsed)]


def _is_public_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return address.is_global and not (
        address.is_loopback
        or address.is_link_local
        or address.is_private
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def _pinned_url(parsed: SplitResult, address: str, port: int) -> str:
    ip = ipaddress.ip_address(address)
    host = f"[{ip}]" if ip.version == 6 else str(ip)
    default_port = 443 if parsed.scheme == "https" else 80
    netloc = host if port == default_port else f"{host}:{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path or "/", parsed.query, ""))


def _host_header(hostname: str, port: int, scheme: str) -> str:
    try:
        parsed = ipaddress.ip_address(hostname)
    except ValueError:
        host = hostname
    else:
        host = f"[{parsed}]" if parsed.version == 6 else str(parsed)
    default_port = 443 if scheme == "https" else 80
    return host if port == default_port else f"{host}:{port}"


async def _read_limited(response: httpx.Response) -> bytes:
    content_encoding = response.headers.get("content-encoding", "").strip().lower()
    if content_encoding not in {"", "identity"}:
        raise NetworkError(
            ErrorCode.NETWORK_HTTP_ERROR,
            f"web_fetch does not support Content-Encoding: {content_encoding}",
        )

    body = bytearray()
    async for chunk in response.aiter_raw():
        remaining = MAX_RESPONSE_BYTES - len(body)
        if remaining <= 0:
            break
        body.extend(chunk[:remaining])
        if len(chunk) >= remaining:
            break
    return bytes(body)


def _decode(body: bytes, encoding: str) -> str:
    try:
        return body.decode(encoding, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


_BLOCK_TAGS = {
    "article",
    "aside",
    "blockquote",
    "div",
    "footer",
    "header",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tr",
    "ul",
}
_SKIP_TAGS = {"script", "style", "noscript", "template", "svg"}


class _ReadableHTMLParser(HTMLParser):
    def __init__(self, *, mode: Literal["markdown", "text"], base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.mode = mode
        self.base_url = base_url
        self.parts: list[str] = []
        self.skip_depth = 0
        self.links: list[str | None] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            self.skip_depth += 1
            return
        if self.skip_depth:
            return
        if tag == "br":
            self.parts.append("\n")
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n\n")
        elif tag == "li":
            self.parts.append("\n- " if self.mode == "markdown" else "\n")
        elif tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            prefix = f"{'#' * int(tag[1])} " if self.mode == "markdown" else ""
            self.parts.append(f"\n\n{prefix}")
        elif tag == "a":
            href = dict(attrs).get("href")
            resolved = urljoin(self.base_url, href) if href else None
            self.links.append(resolved)
            if resolved is not None and self.mode == "markdown":
                self.parts.append("[")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            if self.skip_depth:
                self.skip_depth -= 1
            return
        if self.skip_depth:
            return
        if tag == "a":
            href = self.links.pop() if self.links else None
            if href is not None and self.mode == "markdown":
                self.parts.append(f"]({href})")
        elif (
            tag in _BLOCK_TAGS
            or tag == "li"
            or tag
            in {
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
            }
        ):
            self.parts.append("\n\n")

    def handle_data(self, data: str) -> None:
        if not self.skip_depth:
            self.parts.append(data)

    def rendered(self) -> str:
        text = "".join(self.parts).replace("\r", "\n")
        text = re.sub(r"[\t\f\v ]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def _extract_html(
    source: str,
    *,
    mode: Literal["markdown", "text"],
    base_url: str,
) -> str:
    parser = _ReadableHTMLParser(mode=mode, base_url=base_url)
    parser.feed(source)
    parser.close()
    return parser.rendered()


def _error_result(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=f"[{code.value}] {message}",
        is_error=True,
        code=code,
    )
