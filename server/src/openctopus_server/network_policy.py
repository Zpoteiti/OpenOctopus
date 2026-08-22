from __future__ import annotations

import ipaddress
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError

type IpNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


def _subnet_of(network: IpNetwork, parent: IpNetwork) -> bool:
    if isinstance(network, ipaddress.IPv4Network):
        return isinstance(parent, ipaddress.IPv4Network) and network.subnet_of(parent)
    return isinstance(parent, ipaddress.IPv6Network) and network.subnet_of(parent)


def _network_without(parent: str, exclusions: tuple[str, ...]) -> tuple[str, ...]:
    base = ipaddress.ip_network(parent)
    remaining: list[IpNetwork] = [base]
    for value in exclusions:
        exclusion = ipaddress.ip_network(value)
        if not _subnet_of(exclusion, base):
            raise ValueError("default SSRF exception must be inside its parent network")
        next_remaining: list[IpNetwork] = []
        for network in remaining:
            if not _subnet_of(exclusion, network):
                next_remaining.append(network)
            elif isinstance(network, ipaddress.IPv4Network):
                assert isinstance(exclusion, ipaddress.IPv4Network)
                next_remaining.extend(network.address_exclude(exclusion))
            else:
                assert isinstance(exclusion, ipaddress.IPv6Network)
                next_remaining.extend(network.address_exclude(exclusion))
        remaining = next_remaining
    remaining.sort(key=lambda network: (int(network.network_address), network.prefixlen))
    return tuple(network.with_prefixlen for network in remaining)


_IPV4_IETF_PROTOCOL_ASSIGNMENTS = _network_without(
    "192.0.0.0/24",
    ("192.0.0.9/32", "192.0.0.10/32"),
)
_IPV6_IETF_PROTOCOL_ASSIGNMENTS = _network_without(
    "2001::/23",
    (
        "2001:1::1/128",
        "2001:1::2/128",
        "2001:1::3/128",
        "2001:3::/32",
        "2001:4:112::/48",
        "2001:20::/28",
        "2001:30::/28",
    ),
)

DEFAULT_SSRF_DENYLIST = (
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    *_IPV4_IETF_PROTOCOL_ASSIGNMENTS,
    "192.0.2.0/24",
    "192.88.99.2/32",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "::/128",
    "::1/128",
    "::ffff:0.0.0.0/96",
    "64:ff9b:1::/48",
    "100::/64",
    "100:0:0:1::/64",
    *_IPV6_IETF_PROTOCOL_ASSIGNMENTS,
    "2001:db8::/32",
    "2002::/16",
    "3fff::/20",
    "5f00::/16",
    "fc00::/7",
    "fe80::/10",
    "ff00::/8",
)
DEFAULT_SSRF_DENYLIST_JSON = json.dumps(DEFAULT_SSRF_DENYLIST, separators=(",", ":"))

_HOST_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_MAX_ENTRIES = 256
_MAX_ENTRY_BYTES = 512


@dataclass(frozen=True, slots=True)
class SsrfPolicy:
    canonical_entries: tuple[str, ...]
    networks: tuple[IpNetwork, ...]
    hosts: frozenset[str]
    host_ports: frozenset[tuple[str, int]]

    def denies_host(self, hostname: str, port: int) -> bool:
        return hostname in self.hosts or (hostname, port) in self.host_ports

    def denies_address(self, address: ipaddress.IPv4Address | ipaddress.IPv6Address, port: int) -> bool:
        candidates = [address]
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
            candidates.append(address.ipv4_mapped)
        return any(
            (str(candidate), port) in self.host_ports
            or any(
                candidate.version == network.version and candidate in network
                for network in self.networks
            )
            for candidate in candidates
        )


def canonicalize_ssrf_denylist(entries: Iterable[str]) -> tuple[str, ...]:
    values = tuple(entries)
    if len(values) > _MAX_ENTRIES:
        raise _invalid("web_fetch_denylist has too many entries")

    canonical: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _canonical_entry(value)
        if normalized in seen:
            raise _invalid("web_fetch_denylist contains duplicate entries")
        seen.add(normalized)
        canonical.append(normalized)
    return tuple(canonical)


def compile_ssrf_policy(entries: Iterable[str] | None) -> SsrfPolicy:
    canonical = canonicalize_ssrf_denylist(
        DEFAULT_SSRF_DENYLIST if entries is None else entries
    )
    networks: list[IpNetwork] = []
    hosts: set[str] = set()
    host_ports: set[tuple[str, int]] = set()
    for entry in canonical:
        try:
            network = ipaddress.ip_network(entry, strict=True)
        except ValueError:
            host, port = _split_host_port(entry)
            if port is None:
                hosts.add(host)
            else:
                host_ports.add((host, port))
        else:
            networks.append(network)
    return SsrfPolicy(canonical, tuple(networks), frozenset(hosts), frozenset(host_ports))


def _canonical_entry(value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise _invalid("web_fetch_denylist entries must be non-blank without outer whitespace")
    if "\x00" in value or len(value.encode("utf-8")) > _MAX_ENTRY_BYTES:
        raise _invalid("web_fetch_denylist entries must be bounded and contain no NUL")

    try:
        network = ipaddress.ip_network(value, strict=False)
    except ValueError:
        host, port = _split_host_port(value)
        canonical_host = _canonical_host(host)
        if port is None:
            return canonical_host
        rendered_host = f"[{canonical_host}]" if ":" in canonical_host else canonical_host
        return f"{rendered_host}:{port}"
    return network.with_prefixlen


def _split_host_port(value: str) -> tuple[str, int | None]:
    if value.startswith("["):
        closing = value.find("]")
        if closing <= 1 or closing + 1 >= len(value) or value[closing + 1] != ":":
            raise _invalid("web_fetch_denylist contains an invalid host and port")
        host = value[1:closing]
        port_text = value[closing + 2 :]
    elif value.count(":") == 1:
        host, separator, port_text = value.partition(":")
        if not separator:
            return value, None
    elif ":" in value:
        raise _invalid("IPv6 host and port entries must use brackets")
    else:
        return value, None

    if not port_text.isascii() or not port_text.isdecimal():
        raise _invalid("web_fetch_denylist port must be an integer")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise _invalid("web_fetch_denylist port must be between 1 and 65535")
    return host, port


def _canonical_host(value: str) -> str:
    if not value or any(character in value for character in "/\\@?#*"):
        raise _invalid("web_fetch_denylist contains an invalid hostname")
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        try:
            hostname = value.encode("idna").decode("ascii").rstrip(".").lower()
        except UnicodeError as exc:
            raise _invalid("web_fetch_denylist contains an invalid hostname") from exc
        if (
            not hostname
            or len(hostname) > 253
            or any(_HOST_LABEL.fullmatch(label) is None for label in hostname.split("."))
        ):
            raise _invalid("web_fetch_denylist contains an invalid hostname")
        return hostname
    return str(address)


def _invalid(message: str) -> ConfigError:
    return ConfigError(ErrorCode.CONFIG_VALIDATION_FAILED, message)
