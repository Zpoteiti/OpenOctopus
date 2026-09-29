from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx

from openoctopus_client.document_convert import (
    ConversionError,
    convert_html_bytes_async,
)
from openoctopus_client.tools.common import ToolFailure, ToolOutput, fail

from .common import (
    _cap,
    _int_arg,
    _optional_str,
    _required_str,
)

MAX_REDIRECTS = 10
MAX_RESPONSE_BYTES = 5_000_000


async def web_fetch(args: dict[str, Any], denylist: tuple[str, ...]) -> ToolOutput:
    url = _required_str(args, "url")
    mode = _optional_str(args, "extractMode") or "markdown"
    chars = _int_arg(args, "maxChars", 50_000, minimum=100, maximum=50_000)
    if mode not in {"markdown", "text"}:
        raise ToolFailure("tool_invalid_args", "extractMode is invalid")
    try:
        content, final_url, content_type, charset = await _fetch_bounded(url, denylist)
    except ToolFailure as exc:
        return fail(exc.code, exc.message)
    except httpx.TimeoutException:
        return fail("network_timeout", "web_fetch timed out")
    except httpx.HTTPError:
        return fail("network_http_error", "web_fetch request failed")
    if "text/html" in content_type or "application/xhtml+xml" in content_type:
        return await _convert_html(content, final_url, mode, chars)
    return ToolOutput(_cap(_decode(content, charset), chars))


async def _convert_html(data: bytes, url: str, mode: str, chars: int) -> ToolOutput:
    try:
        markdown = await convert_html_bytes_async(data, base_url=url)
    except ConversionError as exc:
        return fail(exc.code, exc.message)
    if mode == "text":
        markdown = re.sub(r"\[([^]]+)]\([^)]+\)", r"\1", markdown)
    return ToolOutput(_cap(markdown, chars))


async def _fetch_bounded(url: str, denylist: tuple[str, ...]) -> tuple[bytes, str, str, str]:
    current = url
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=False, trust_env=False
    ) as client:
        for index in range(MAX_REDIRECTS + 1):
            target = httpx.URL(current)
            if (
                target.scheme not in {"http", "https"}
                or target.host is None
                or target.username
                or target.password
            ):
                raise ToolFailure(
                    "tool_invalid_args", "url must be an http(s) URL without credentials"
                )
            host = target.host.rstrip(".").lower()
            port = target.port or (443 if target.scheme == "https" else 80)
            addresses = await _validated_addresses(host, port, denylist)
            address = addresses[0]
            pinned = target.copy_with(host=address)
            headers = {
                "host": target.netloc.decode(),
                "accept-encoding": "identity",
                "user-agent": "OpenOctopus/0.0.1 web_fetch",
            }
            response = await client.send(
                client.build_request(
                    "GET",
                    pinned,
                    headers=headers,
                    extensions={"sni_hostname": host},
                ),
                stream=True,
            )
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                await response.aclose()
                if location is None or index == MAX_REDIRECTS:
                    raise ToolFailure("network_http_error", "web_fetch redirect is invalid")
                current = str(target.join(location))
                continue
            if response.status_code >= 400:
                status = response.status_code
                await response.aclose()
                raise ToolFailure("network_http_error", f"web_fetch received HTTP {status}")
            if response.headers.get("content-encoding", "").strip().lower() not in {"", "identity"}:
                await response.aclose()
                raise ToolFailure(
                    "network_http_error", "web_fetch does not support compressed responses"
                )
            body = bytearray()
            try:
                async for chunk in response.aiter_raw():
                    body.extend(chunk[: MAX_RESPONSE_BYTES - len(body)])
                    if len(body) >= MAX_RESPONSE_BYTES:
                        break
            finally:
                await response.aclose()
            return (
                bytes(body),
                current,
                response.headers.get("content-type", "").lower(),
                response.encoding or "utf-8",
            )
    raise ToolFailure("network_http_error", "web_fetch redirect loop terminated unexpectedly")


async def _validated_addresses(host: str, port: int, denylist: tuple[str, ...]) -> list[str]:
    import ipaddress
    import socket

    if _denied_host(host, port, denylist):
        raise ToolFailure("network_ssrf_blocked", f"Blocked address for {host}")
    try:
        records = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ToolFailure("network_dns_failed", f"Could not resolve {host}") from exc
    addresses: list[str] = []
    for record in records:
        address = str(record[4][0])
        parsed = ipaddress.ip_address(address)
        if _denied_ip(parsed, denylist):
            raise ToolFailure("network_ssrf_blocked", f"Blocked address for {host}")
        if address not in addresses:
            addresses.append(address)
    if not addresses:
        raise ToolFailure("network_dns_failed", f"Could not resolve {host}")
    return addresses


def _denied_host(host: str, port: int, denylist: tuple[str, ...]) -> bool:
    endpoint = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    return any(entry.lower() in {host, endpoint} for entry in denylist)


def _denied_ip(address: object, denylist: tuple[str, ...]) -> bool:
    import ipaddress

    assert isinstance(address, ipaddress.IPv4Address | ipaddress.IPv6Address)
    candidates = [address]
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        candidates.append(address.ipv4_mapped)
    for entry in denylist:
        try:
            network = ipaddress.ip_network(entry, strict=False)
            if any(
                candidate.version == network.version and candidate in network
                for candidate in candidates
            ):
                return True
        except ValueError:
            continue
    return False


def _decode(data: bytes, charset: str) -> str:
    try:
        return data.decode(charset, errors="replace")
    except LookupError:
        return data.decode("utf-8", errors="replace")
